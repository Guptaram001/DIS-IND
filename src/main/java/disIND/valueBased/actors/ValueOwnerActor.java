package disIND.valueBased.actors;

import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;

import java.util.BitSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntConsumer;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.roaringbitmap.RoaringBitmap;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.CMCommand;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.model.SharedModel.ColumnUpdates;
import disIND.valueBased.model.SharedModel.DataOrientation;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import disIND.valueBased.model.SharedModel.MembershipUpdates;
import disIND.valueBased.model.SharedModel.ValueUpdates;
import disIND.valueBased.structures.ColumnMajorProcessor;
import disIND.valueBased.structures.CountCandidateTracker;
import disIND.valueBased.structures.PruneCandidateTracker;
import disIND.valueBased.structures.PruneMetricsCollector;
import disIND.valueBased.structures.ValueMajorProcessor;
import disIND.valueBased.structures.ValueOwnerClusterIndex;
import disIND.valueBased.structures.ValueOwnerCqf;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.WitnessCandidateTracker;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.WorkerValueIdStore;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {
    private static final int LONG_COLUMN_SET_LIMIT = Long.SIZE;
    private static final int BIT_SET_COLUMN_LIMIT = 5_000;

    public sealed interface Command extends AkkaSerializable permits StoreBatch, GetBucket, FinalizeMembership {}
    public record ColumnRows(int columnId,long[] rowIds) {}
    public record ValueData(String value,List<ColumnRows> columns) {}
    public record ValueMajorBatch(List<ValueData> values) implements BatchBody {}
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "batchType")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ValueMajorBatch.class, name = "value-major"),
            @JsonSubTypes.Type(value = ColumnMajorBatch.class, name = "column-major")
    })
    public sealed interface BatchBody extends AkkaSerializable permits ValueMajorBatch, ColumnMajorBatch {}
    public record ColumnMajorBatch(List<ColumnValues> columns) implements BatchBody {
         public ColumnMajorBatch {
            columns = List.copyOf(columns);
        }
    }

    public static final EntityTypeKey<Command> TYPE_KEY =EntityTypeKey.create(Command.class,"ValueOwnerActor");

    public record ValueRows(String value, long[] rowIds) implements AkkaSerializable {
        public ValueRows {
            rowIds = rowIds.clone();
        }
    }

    public record ColumnValues(int colId, List<ValueRows> values) implements AkkaSerializable {
        public ColumnValues {
            values = List.copyOf(values);
        }
    }

    public record StoreBatch(long epoch, int tableId, int batchId, int round, int bucketId,
        DataOrientation orientation,BatchBody body, ActorRef<DirectBatchAggregatorActor.Command> ackTo) implements Command {
        public StoreBatch {
            Objects.requireNonNull(orientation, "orientation");
            Objects.requireNonNull(body, "body");
            boolean matchingBody = switch (orientation) {
                case VALUE_MAJOR -> body instanceof ValueMajorBatch;
                case COLUMN_MAJOR -> body instanceof ColumnMajorBatch;
            };
            if (!matchingBody) {
                throw new IllegalArgumentException(
                        "Batch orientation " + orientation + " does not match body "
                                + body.getClass().getSimpleName());
            }
        }
    }
    public interface BatchProcessor {
        MembershipUpdates process(int bucketId,BatchBody batch,WorkerValueIdStore valueIds);
    }

    public record TrackingResult(Map<CandidateKey, CandidateState> changedStates,Map<Integer, List<CandidateLocalStatus>>
                transitionsByLhs) {}

    public interface CandidateChanges {
        void violationCreated(int lhsCol,int rhsCol,int valueId);
        void violationRepaired(int lhsCol,int rhsCol,int valueId);
    }

    public interface CandidateTracker {
        CandidateChanges newChanges(int bucketId);
        TrackingResult apply(CandidateChanges changes,Map<Integer, Int2IntMap> updatedMembership,
                ValueOwnerMembershipStore store);
    }

    public record GetBucket(ActorRef<BucketSnapshot> replyTo) implements Command {}
    public record FinalizeMembership(int finalRound,int expectedBuckets,int totalColumns) implements Command {}
    public record ColumnCount(int colId, long count) implements AkkaSerializable {}
    public record BucketSnapshot(int bucketId, Map<Integer, List<ColumnCount>> values)implements AkkaSerializable {}

    private final String entityId;
    private final int bucketId;
    private final ClusterSharding sharding;
    private final DatasetMetadata metadata;
    private final ValueOwnerMembershipStore membershipStore;
    private final WorkerValueIdStore valueIdStore;
    private final int recentBatchLimit;
    private final Map<String, Boolean> resolvedBatches;
    private final Set<Long> locallyRejectedCandidates = new HashSet<>();
    private final int[] localDistinctCounts;
    private final int[][] localDistinctCountsByPartition;
    private final long[] exactComparisonsByLhs;
    private final long[] candidateEvaluationsByLhs;
    private final DataOrientation orientation;
    private final BatchProcessor batchProcessor;
    private final CandidateTracker candidateTracker;
    private final CandidateTrackingMode candidateTracking;
    private final ValueOwnerClusterIndex pruneClusters;
    private final ValueOwnerCqf pruneCqf;
    private final PruneMetricsCollector pruneMetrics;
    private long statusSequence;

    public static String entityId(int bucketId) {
        return "value-bucket-" + bucketId;
    }

    public static Behavior<Command> create(String entityId,ClusterSharding sharding,DatasetMetadata metadata,
        ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore,DataOrientation orientation,
    CandidateTrackingMode candidateTracking) {
        return Behaviors.setup(ctx ->new ValueOwnerActor(ctx, entityId, sharding, metadata, membershipStore,
            valueIdStore,orientation,candidateTracking));
    }

    private ValueOwnerActor(ActorContext<Command> ctx,String entityId,ClusterSharding sharding,
            DatasetMetadata metadata,ValueOwnerMembershipStore membershipStore,
            WorkerValueIdStore valueIdStore,DataOrientation orientation,CandidateTrackingMode candidateTracking) {
        super(ctx);
        this.entityId = entityId;
        this.bucketId = Integer.parseInt(entityId.substring("value-bucket-".length()));
        this.sharding = sharding;
        this.metadata = metadata;
        this.membershipStore = membershipStore;
        this.valueIdStore = valueIdStore;
        this.localDistinctCounts =new int[metadata.totalCols()];
        this.pruneMetrics = new PruneMetricsCollector(metadata.totalCols());
        this.exactComparisonsByLhs = new long[metadata.totalCols()];
        this.candidateEvaluationsByLhs = new long[metadata.totalCols()];
        this.recentBatchLimit = UserConfig.DEFAULT_VO_BATCH_EVICTION_LIMIT;
        this.orientation=orientation;
        this.batchProcessor=newProcessor(orientation);
        this.candidateTracking=Objects.requireNonNull(candidateTracking,"candidateTracking");
        if (candidateTracking == CandidateTrackingMode.PRUNE) {
            this.localDistinctCountsByPartition =new int[metadata.totalCols()][UserConfig.PRUNE_COUNT_PARTITIONS];
            this.pruneClusters = new ValueOwnerClusterIndex(bucketId, metadata.totalCols());
            this.pruneCqf = UserConfig.PRUNE_CQF_ENABLED? new ValueOwnerCqf(bucketId, metadata.totalCols()): null;
        } else {
            this.localDistinctCountsByPartition = null;
            this.pruneClusters = null;
            this.pruneCqf = null;
        }
        this.candidateTracker = switch (candidateTracking) {
            case COUNT -> new CountCandidateTracker();
            case WITNESS -> new WitnessCandidateTracker(UserConfig.MAX_TRACKED_VIOLATIONS);
            case PRUNE -> new PruneCandidateTracker(pruneCqf, pruneClusters, localDistinctCounts,
                    localDistinctCountsByPartition, pruneMetrics);
        };
        this.resolvedBatches = new LinkedHashMap<>(recentBatchLimit, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > ValueOwnerActor.this.recentBatchLimit;
            }
        };
        if(Debug.INTERNAL)
            getContext().getLog().info("[PLACEMENT] type=VO bucket={} entity={} node={}",bucketId, entityId,
                Cluster.get(ctx.getSystem()).selfMember().address());
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StoreBatch.class, this::onStoreBatch)
                .onMessage(GetBucket.class, this::onGetBucket)
                .onMessage(FinalizeMembership.class, this::onFinalizeMembership)
                .build();
    }

    private Behavior<Command> onFinalizeMembership(FinalizeMembership msg) {
        RoaringBitmap emptyFinalStatus = new RoaringBitmap();
        for (int lhsCol = 0; lhsCol < msg.totalColumns(); lhsCol++) {
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsCol))
                    .tell(new CMCommand.ValueOwnerDrained(msg.finalRound(), bucketId,
                            msg.expectedBuckets(), emptyFinalStatus,
                            candidateEvaluationsByLhs[lhsCol],
                            exactComparisonsByLhs[lhsCol],
                            pruneMetrics.snapshot(lhsCol)));
        }
        if(Debug.INTERNAL)
            getContext().getLog().info(
                    "[VO] bucket={} finalRound={} notifiedCms={} localRejectedCandidates={} finalStatus=ack-only",
                    bucketId, msg.finalRound(), msg.totalColumns(), locallyRejectedCandidates.size());
        return this;
    }

    private Behavior<Command> onStoreBatch(StoreBatch msg) {
        if(Debug.INTERNAL)
            getContext().getLog().info("[VO] round={} bucket={} epoch={} tableId={} batchId={}",
                msg.round(),msg.bucketId(), msg.epoch(), msg.tableId(), msg.batchId());
        if (msg.bucketId() != bucketId)
            throw new IllegalArgumentException("Message for bucket " + msg.bucketId()+ " sent to value owner " + entityId);

        if (msg.orientation() != orientation)
            throw new IllegalStateException(
                    "VO configured for " + orientation + " but received " + msg.orientation());
    
        String batchKey = msg.tableId() + ":" + msg.batchId();
        if (resolvedBatches.containsKey(batchKey)) {
            acknowledge(msg);
            return this;
        }

        MembershipUpdates updatesByValue =batchProcessor.process(bucketId,msg.body(),valueIdStore);
        statusSequence = Math.incrementExact(statusSequence);
        applyUpdates(msg, updatesByValue, statusSequence);
        resolvedBatches.put(batchKey, Boolean.TRUE);
        acknowledge(msg);
        return this;
    }

    private void applyUpdates(StoreBatch message, MembershipUpdates updates, long voSequence) {

        if (updates instanceof ValueUpdates valueUpdates) {
            applyValueUpdates(message, valueUpdates.byValue(), voSequence);
            return;
        }

        if (updates instanceof ColumnUpdates columnUpdates) {
            //Internally uses the same Value based storage
            applyValueUpdates(message, columnUpdates.byValue(), voSequence);
            return;
        }

        throw new IllegalArgumentException("Unsupported membership update type: "+ updates.getClass().getName());
    }

    private void applyValueUpdates(StoreBatch message, Map<Integer, Map<Integer, Integer>> updatesByValue,
            long voSequence) {
        Map<Integer, Int2IntMap> records = membershipStore.loadBatch(bucketId, updatesByValue.keySet());
        Map<Integer, ColumnSet> addedColumnsByValue = new HashMap<>();

        updatesByValue.forEach((valueId, columnUpdates) -> {
            Int2IntMap record = records.get(valueId);
            if (record == null)
                throw new IllegalStateException("No membership record loaded for value " + valueId);
            ColumnSet addedColumns = newColumnSet();
            columnUpdates.forEach((columnId, count) -> {
                int primitiveColumnId = columnId.intValue();
                int primitiveCount = count.intValue();
                int previousCount = record.getOrDefault(primitiveColumnId, 0);
                boolean wasPresent = record.containsKey(primitiveColumnId);
                record.put(primitiveColumnId, Math.addExact(previousCount, primitiveCount));
                if (!wasPresent) {
                    localDistinctCounts[primitiveColumnId] =Math.addExact(localDistinctCounts[primitiveColumnId], 1);
                    if (localDistinctCountsByPartition != null) {
                        int partition = countPartition(valueId, localDistinctCountsByPartition[columnId].length);
                        localDistinctCountsByPartition[primitiveColumnId][partition] = Math.addExact(
                                localDistinctCountsByPartition[primitiveColumnId][partition], 1);
                    }
                    addedColumns.add(primitiveColumnId);
                    if (pruneCqf != null)
                        pruneCqf.addMembership(primitiveColumnId, valueId);
                }
            });
            if (!addedColumns.isEmpty()) {
                updateClusterMembership(record, addedColumns);
                addedColumnsByValue.put(valueId, addedColumns);
            }
        });

        CandidateChanges candidateChanges = candidateTracker.newChanges(bucketId);
        calculateMembershipUpdates(addedColumnsByValue, records, candidateChanges);
        TrackingResult trackingResult = candidateTracker.apply(candidateChanges, records, membershipStore);

        // Membership and candidate state become visible atomically.
        membershipStore.writeBatch(bucketId, records, trackingResult.changedStates());
        updateLocallyRejectedCandidates(trackingResult.changedStates());
        sendCandidateStatusTransitions(message, voSequence, trackingResult.transitionsByLhs(),
                trackingResult.changedStates().size());
    }

    private void updateClusterMembership(Int2IntMap membershipAfter, ColumnSet addedColumns) {
        if (pruneClusters == null)
            return;

        BitSet afterSignature = new BitSet(metadata.totalCols());
        membershipAfter.keySet().forEach(afterSignature::set);
        BitSet beforeSignature = (BitSet) afterSignature.clone();
        addedColumns.forEach(beforeSignature::clear);
        pruneClusters.moveMembership(beforeSignature, afterSignature);
    }

    private void sendCandidateStatusTransitions(StoreBatch batch, long voSequence,
            Map<Integer, List<CandidateLocalStatus>> transitionsByLhs, int affectedCandidateCount) {

        transitionsByLhs.forEach((lhsCol, transitions) -> {
            if (transitions.isEmpty())
                return;
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY,CMCommand.entityId(lhsCol))
                    .tell(new CMCommand.ValueOwnerCandidateStatusUpdate(
                            batch.epoch(), voSequence, batch.round(), bucketId, transitions));
        });

        if (Debug.INTERNAL) {
            int transitionCount = transitionsByLhs.values().stream().mapToInt(List::size).sum();
            getContext().getLog().info(
                    "[VO] round={} bucket={} epoch={} voSequence={} affectedCandidates={} transitions={} affectedCms={}",
                    batch.round(), bucketId, batch.epoch(), voSequence, affectedCandidateCount,
                    transitionCount, transitionsByLhs.size());
        }
    }

    private void acknowledge(StoreBatch message) {
        if (message.ackTo() != null) 
            message.ackTo().tell(new DirectBatchAggregatorActor.ValueOwnerPersisted(bucketId));
    }

    private BatchProcessor newProcessor(DataOrientation orientation)
    {
        return switch(orientation){
            case COLUMN_MAJOR -> new ColumnMajorProcessor();
            case VALUE_MAJOR -> new ValueMajorProcessor();
        };
    }

    private Behavior<Command> onGetBucket(GetBucket msg) {
        Map<Integer, List<ColumnCount>> snapshot = new HashMap<>();
        membershipStore.snapshotBucket(bucketId).forEach((valueId, columns) -> {
            List<ColumnCount> columnCounts = columns.entrySet().stream().map(entry -> new ColumnCount(entry.getKey(), entry.getValue()))
                    .toList();snapshot.put(valueId, columnCounts);
        });
        msg.replyTo().tell(new BucketSnapshot(bucketId, Map.copyOf(snapshot)));
        return this;
    }

    private void calculateMembershipUpdates(Map<Integer, ColumnSet> addedColumnsByValue,Map<Integer, Int2IntMap> records,
        CandidateChanges changes) {

        Set<Long> evaluatedCandidates = new HashSet<>();
        Set<Long> newlyRejectedThisBatch =new HashSet<>();
        boolean pruneMode =candidateTracking == CandidateTrackingMode.PRUNE;
        addedColumnsByValue.forEach((valueId, addedColumns) -> {
            ColumnSet after = newColumnSet();
            records.get(valueId).keySet().forEach(after::add);
            ColumnSet before = after.copy();
            before.andNot(addedColumns);

            addedColumns.forEach(lhsCol -> {
                for (int rhsCol = 0;rhsCol < metadata.totalCols();rhsCol++) {
                    if (!compatible(lhsCol, rhsCol))
                        continue;
                    long compactKey =candidateKey(lhsCol, rhsCol);
                    if (pruneMode) {
                        // Invali, lhs: Skip
                        if (locallyRejectedCandidates.contains(compactKey)) {
                            pruneMetrics.invalidLhsSkipped(lhsCol);
                            continue;
                        }
                
                        if (newlyRejectedThisBatch.contains(compactKey)) {
                            pruneMetrics.sameBatchSkipped(lhsCol);
                            continue;
                        }
                    }
                    countComparison(evaluatedCandidates, lhsCol, rhsCol);
                    if (!after.contains(rhsCol)) {
                        changes.violationCreated(lhsCol,rhsCol,valueId);
                        if (pruneMode) {
                            newlyRejectedThisBatch.add(compactKey);
                        }
                    }
                }
            });

            addedColumns.forEach(rhsCol ->
                    before.forEach(lhsCol -> {
                        if (compatible(lhsCol, rhsCol)) {
                            long compactKey =candidateKey(lhsCol, rhsCol);
                            if (pruneMode) {
                                if (newlyRejectedThisBatch.contains(compactKey)) {
                                    pruneMetrics.sameBatchSkipped(lhsCol);
                                    return;
                                }
                                //Valid + RHS: Skip
                                if (!locallyRejectedCandidates.contains(compactKey)) {
                                    pruneMetrics.validRhsSkipped(lhsCol);
                                    return;
                                }
                            }
                            countComparison(evaluatedCandidates, lhsCol, rhsCol);
                            changes.violationRepaired(lhsCol,rhsCol,valueId);
                        }
                    }));
        });
    }

    private boolean compatible(int lhsCol, int rhsCol) {
        return lhsCol != rhsCol && testCompatibility(metadata.typeOf(lhsCol), metadata.typeOf(rhsCol));
    }

    private void countComparison(Set<Long> evaluatedCandidates, int lhsCol, int rhsCol) {
        exactComparisonsByLhs[lhsCol] =Math.addExact(exactComparisonsByLhs[lhsCol], 1L);
        countCandidateEvaluation(evaluatedCandidates, lhsCol, rhsCol);
    }

    private void countCandidateEvaluation(Set<Long> evaluatedCandidates, int lhsCol, int rhsCol) {
        long key = ((long) lhsCol << 32) | (rhsCol & 0xffffffffL);
        if (evaluatedCandidates.add(key)) {
            candidateEvaluationsByLhs[lhsCol] =
                    Math.addExact(candidateEvaluationsByLhs[lhsCol], 1);
        }
    }

    private void updateLocallyRejectedCandidates(Map<CandidateKey, CandidateState> changedStates) {
        changedStates.forEach((key, state) -> {
            long compactKey =candidateKey(key.lhsCol(),key.rhsCol());
            if (state.rejected())
                locallyRejectedCandidates.add(compactKey);
            else
                locallyRejectedCandidates.remove(compactKey);
        });
    }

    private static long candidateKey(int lhsCol, int rhsCol) {
        return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
    }

    //Look into something else
    private static int countPartition(int valueId, int partitionCount) {
        int hash = valueId;
        hash ^= hash >>> 16;
        hash *= 0x7feb352d;
        hash ^= hash >>> 15;
        hash *= 0x846ca68b;
        hash ^= hash >>> 16;
        return hash & (partitionCount - 1);
    }

    private ColumnSet newColumnSet() {
        int totalColumns = metadata.totalCols();
        if (totalColumns <= LONG_COLUMN_SET_LIMIT)
            return new LongColumnSet();
        if (totalColumns <= BIT_SET_COLUMN_LIMIT)
            return new BitSetColumnSet(totalColumns);
        return new RoaringColumnSet();
    }

    private interface ColumnSet {
        void add(int column);
        boolean contains(int column);
        boolean isEmpty();
        int cardinality();
        void forEach(IntConsumer action);
        ColumnSet copy();
        void andNot(ColumnSet other);
    }

    private static final class LongColumnSet implements ColumnSet {
        private long bits;

        private LongColumnSet() {}

        private LongColumnSet(long bits) {
            this.bits = bits;
        }

        @Override
        public void add(int column) {
            bits |= 1L << column;
        }

        @Override
        public boolean contains(int column) {
            return (bits & (1L << column)) != 0;
        }

        @Override
        public boolean isEmpty() {
            return bits == 0;
        }

        @Override
        public int cardinality() {
            return Long.bitCount(bits);
        }

        @Override
        public void forEach(IntConsumer action) {
            long remaining = bits;
            while (remaining != 0) {
                int column = Long.numberOfTrailingZeros(remaining);
                action.accept(column);
                remaining &= remaining - 1;
            }
        }

        @Override
        public ColumnSet copy() {
            return new LongColumnSet(bits);
        }

        @Override
        public void andNot(ColumnSet other) {
            bits &= ~((LongColumnSet) other).bits;
        }
    }

    private static final class BitSetColumnSet implements ColumnSet {
        private final BitSet bits;

        private BitSetColumnSet(int totalColumns) {
            this.bits = new BitSet(totalColumns);
        }

        private BitSetColumnSet(BitSet bits) {
            this.bits = bits;
        }

        @Override
        public void add(int column) {
            bits.set(column);
        }

        @Override
        public boolean contains(int column) {
            return bits.get(column);
        }

        @Override
        public boolean isEmpty() {
            return bits.isEmpty();
        }

        @Override
        public int cardinality() {
            return bits.cardinality();
        }

        @Override
        public void forEach(IntConsumer action) {
            bits.stream().forEach(action);
        }

        @Override
        public ColumnSet copy() {
            return new BitSetColumnSet((BitSet) bits.clone());
        }

        @Override
        public void andNot(ColumnSet other) {
            bits.andNot(((BitSetColumnSet) other).bits);
        }
    }

    private static final class RoaringColumnSet implements ColumnSet {
        private final RoaringBitmap bits;

        private RoaringColumnSet() {
            this.bits = new RoaringBitmap();
        }

        private RoaringColumnSet(RoaringBitmap bits) {
            this.bits = bits;
        }

        @Override
        public void add(int column) {
            bits.add(column);
        }

        @Override
        public boolean contains(int column) {
            return bits.contains(column);
        }

        @Override
        public boolean isEmpty() {
            return bits.isEmpty();
        }

        @Override
        public int cardinality() {
            return bits.getCardinality();
        }

        @Override
        public void forEach(IntConsumer action) {
            bits.forEach((int column) -> action.accept(column));
        }

        @Override
        public ColumnSet copy() {
            return new RoaringColumnSet(bits.clone());
        }

        @Override
        public void andNot(ColumnSet other) {
            bits.andNot(((RoaringColumnSet) other).bits);
        }
    }

}
