package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.typed.Cluster;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.CMCommand;
import disIND.valueBased.model.SharedModel.CandidateViolationDelta;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.WorkerValueIdStore;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.utility.UserConfig;
import org.roaringbitmap.RoaringBitmap;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.function.IntConsumer;

import static disIND.valueBased.utility.Debug.formLog;
import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;

public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {
    private static final int LONG_COLUMN_SET_LIMIT = Long.SIZE;
    private static final int BIT_SET_COLUMN_LIMIT = 5_000;

    public sealed interface Command extends AkkaSerializable permits StoreBatch, GetBucket, FinalizeMembership {}
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
        List<ColumnValues> columns, ActorRef<DirectBatchAggregatorActor.Command> ackTo) implements Command {
        public StoreBatch {
            columns = List.copyOf(columns);
        }
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
    private final long[] exactComparisonsByLhs;
    private final long[] candidateEvaluationsByLhs;

    public static String entityId(int bucketId) {
        return "value-bucket-" + bucketId;
    }

    public static Behavior<Command> create(String entityId,ClusterSharding sharding,DatasetMetadata metadata,
        ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore) {
        return Behaviors.setup(ctx ->new ValueOwnerActor(ctx, entityId, sharding, metadata, membershipStore, valueIdStore));
    }

    private ValueOwnerActor(ActorContext<Command> ctx,String entityId,ClusterSharding sharding,
            DatasetMetadata metadata,ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore) {
        super(ctx);
        this.entityId = entityId;
        this.bucketId = Integer.parseInt(entityId.substring("value-bucket-".length()));
        this.sharding = sharding;
        this.metadata = metadata;
        this.membershipStore = membershipStore;
        this.valueIdStore = valueIdStore;
        this.exactComparisonsByLhs = new long[metadata.totalCols()];
        this.candidateEvaluationsByLhs = new long[metadata.totalCols()];
        this.recentBatchLimit = UserConfig.DEFAULT_VO_BATCH_EVICTION_LIMIT;
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
        for (int lhsCol = 0; lhsCol < msg.totalColumns(); lhsCol++) {
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsCol))
                    .tell(new CMCommand.ValueOwnerDrained(msg.finalRound(), bucketId,
                            msg.expectedBuckets(), candidateEvaluationsByLhs[lhsCol],
                            exactComparisonsByLhs[lhsCol]));
        }
        if(Debug.INTERNAL)
            getContext().getLog().info("[VO] bucket={} finalRound={} notifiedCms={}",
                bucketId, msg.finalRound(), msg.totalColumns());
        return this;
    }

    private Behavior<Command> onStoreBatch(StoreBatch msg) {
        if(Debug.INTERNAL)
            getContext().getLog().info("[VO] round={} bucket={} epoch={} tableId={} batchId={} aggregatedUpdates={}",
                msg.round(),msg.bucketId(), msg.epoch(), msg.tableId(), msg.batchId(),
                msg.columns().stream().mapToInt(column -> column.values().size()).sum());
        if (msg.bucketId() != bucketId)
            throw new IllegalArgumentException("Message for bucket " + msg.bucketId()+ " sent to value owner " + entityId);

        String batchKey = msg.tableId() + ":" + msg.batchId();
        boolean applied = !resolvedBatches.containsKey(batchKey);
        int changedValues = 0;
        if (applied) {
            List<String> distinctValues = msg.columns().stream()
                    .flatMap(column -> column.values().stream())
                    .map(ValueRows::value)
                    .distinct()
                    .toList();
            Map<String, Integer> idsByValue = valueIdStore.resolveBatch(bucketId, distinctValues);
            Map<Integer, Map<Integer, Long>> updatesByValue = new HashMap<>();
            for (ColumnValues column : msg.columns()) {
                for (ValueRows item : column.values()) {
                    int valueId = idsByValue.get(item.value());
                    updatesByValue.computeIfAbsent(valueId, ignored -> new HashMap<>())
                            .merge(column.colId(), (long) item.rowIds().length, Long::sum);
                }
            }
            resolvedBatches.put(batchKey, Boolean.TRUE);

            Map<Integer, Map<Integer, Long>> records =membershipStore.loadBatch(bucketId, updatesByValue.keySet());

            Map<Integer, ColumnSet> addedColumnsByValue = new HashMap<>();
            updatesByValue.forEach((valueId, columnUpdates) -> {
                Map<Integer, Long> record = records.get(valueId);
                ColumnSet addedColumns = newColumnSet();
                columnUpdates.forEach((colId, count) -> {
                    Long previous = record.put(colId,Math.addExact(record.getOrDefault(colId, 0L), count));
                    if (previous == null)
                        addedColumns.add(colId);
                });
                if (!addedColumns.isEmpty()) {
                    addedColumnsByValue.put(valueId, addedColumns);
                }
            });

            Map<Integer, Map<Integer, Integer>> countDeltasByLhs =calculateMembershipUpdates(addedColumnsByValue, records);
            membershipStore.writeBatch(bucketId, records);
            changedValues = records.size();

            emitMembershipUpdates(msg, addedColumnsByValue, countDeltasByLhs);
        }
        if (Debug.INTERNAL)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.vo(),-1,"-",
                    String.valueOf(Debug.State.NONE),
                    "Disk membership updated bucketId={} epoch={} applied={} changedValues={}",
                    bucketId, msg.epoch(), applied, changedValues);
        if (msg.ackTo() != null)
            msg.ackTo().tell(new DirectBatchAggregatorActor.ValueOwnerPersisted(bucketId));
        return this;
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

    private Map<Integer, Map<Integer, Integer>> calculateMembershipUpdates(Map<Integer, ColumnSet> addedColumnsByValue,
            Map<Integer, Map<Integer, Long>> records) {
            Map<Integer, Map<Integer, Integer>> countDeltasByLhs = new HashMap<>();
            Set<Long> evaluatedCandidates = new HashSet<>();
            addedColumnsByValue.forEach((valueId, addedColumns) -> {
            ColumnSet after = newColumnSet();
            records.get(valueId).keySet().forEach(after::add);
            ColumnSet before = after.copy();
            before.andNot(addedColumns);

            addedColumns.forEach(lhsCol -> {
                for (int rhsCol = 0; rhsCol < metadata.totalCols(); rhsCol++) {
                    if (rhsCol != lhsCol
                            && testCompatibility(metadata.typeOf(lhsCol), metadata.typeOf(rhsCol))) {
                        exactComparisonsByLhs[lhsCol] =
                                Math.addExact(exactComparisonsByLhs[lhsCol], 1);
                        countCandidateEvaluation(evaluatedCandidates, lhsCol, rhsCol);
                        if (!after.contains(rhsCol))
                            mergeCountDelta(countDeltasByLhs, lhsCol, rhsCol, 1);
                    }
                }
            });

            addedColumns.forEach(rhsCol -> before.forEach(lhsCol -> {
                        if (lhsCol != rhsCol && testCompatibility(metadata.typeOf(lhsCol), metadata.typeOf(rhsCol))) {
                            exactComparisonsByLhs[lhsCol] =
                                    Math.addExact(exactComparisonsByLhs[lhsCol], 1);
                            countCandidateEvaluation(evaluatedCandidates, lhsCol, rhsCol);
                            mergeCountDelta(countDeltasByLhs, lhsCol, rhsCol, -1);
                        }
                    }));
        });
        return countDeltasByLhs;
    }

    private void countCandidateEvaluation(Set<Long> evaluatedCandidates, int lhsCol, int rhsCol) {
        long key = ((long) lhsCol << 32) | (rhsCol & 0xffffffffL);
        if (evaluatedCandidates.add(key)) {
            candidateEvaluationsByLhs[lhsCol] =
                    Math.addExact(candidateEvaluationsByLhs[lhsCol], 1);
        }
    }

    private void emitMembershipUpdates(StoreBatch batch,Map<Integer, ColumnSet> addedColumnsByValue,Map<Integer,
        Map<Integer, Integer>> countDeltasByLhs) {
        if (addedColumnsByValue.isEmpty())
            return;

        countDeltasByLhs.forEach((lhsCol, rhsDeltas) -> {List<CandidateViolationDelta> deltas = rhsDeltas.entrySet().stream()
                    .filter(entry -> entry.getValue() != 0)
                    .map(entry -> new CandidateViolationDelta(entry.getKey(), entry.getValue()))
                    .toList();
            if (!deltas.isEmpty()) {
                sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsCol))
                .tell(new CMCommand.ValueOwnerMembershipUpdate(
                                batch.epoch(), batch.round(), bucketId, deltas));
            }
        });
        if(Debug.INTERNAL)
            getContext().getLog().info("[VO] round={} bucket={} epoch={} newMemberships={} affectedCms={}",
                batch.round(),bucketId, batch.epoch(),addedColumnsByValue.values().stream().mapToInt(ColumnSet::cardinality).sum(),
                countDeltasByLhs.size());
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

    private static void mergeCountDelta(Map<Integer, Map<Integer, Integer>> byLhs,int lhsCol,int rhsCol,int delta) {
        byLhs.computeIfAbsent(lhsCol, ignored -> new HashMap<>())
                .merge(rhsCol, delta, Integer::sum);
    }
}
