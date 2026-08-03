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
import disIND.valueBased.model.SharedModel.BDCommand;
import disIND.valueBased.model.SharedModel.CMCommand;
import disIND.valueBased.model.SharedModel.CandidateViolationDelta;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.utility.Debug;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static disIND.valueBased.utility.Debug.formLog;
import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;

public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public sealed interface Command extends AkkaSerializable permits StoreBatch, GetBucket, FinalizeMembership {}
    public static final EntityTypeKey<Command> TYPE_KEY =EntityTypeKey.create(Command.class,"ValueOwnerActor");

    public record ValueCount(int valueId, int colId, int count) implements AkkaSerializable {
        public ValueCount {
            if (count <= 0)
                throw new IllegalArgumentException("count must be positive");
        }
    }

    public record StoreBatch(long epoch, int tableId, int batchId, int round, int bucketId,
        List<ValueCount> values, ActorRef<BDCommand> ackTo) implements Command {
        public StoreBatch {
            values = List.copyOf(values);
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
    private final Set<String> appliedBatches = new HashSet<>();

    public static String entityId(int bucketId) {
        return "value-bucket-" + bucketId;
    }

    public static Behavior<Command> create(String entityId,ClusterSharding sharding,DatasetMetadata metadata,
        ValueOwnerMembershipStore membershipStore) {
        return Behaviors.setup(ctx ->new ValueOwnerActor(ctx, entityId, sharding, metadata, membershipStore));
    }

    private ValueOwnerActor(ActorContext<Command> ctx,String entityId,ClusterSharding sharding,
            DatasetMetadata metadata,ValueOwnerMembershipStore membershipStore) {
        super(ctx);
        this.entityId = entityId;
        this.bucketId = Integer.parseInt(entityId.substring("value-bucket-".length()));
        this.sharding = sharding;
        this.metadata = metadata;
        this.membershipStore = membershipStore;
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
                    .tell(new CMCommand.ValueOwnerDrained(msg.finalRound(), bucketId, msg.expectedBuckets()));
        }
        if(Debug.INTERNAL)
            getContext().getLog().info("[VO] bucket={} finalRound={} notifiedCms={}",
                bucketId, msg.finalRound(), msg.totalColumns());
        return this;
    }

    private Behavior<Command> onStoreBatch(StoreBatch msg) {
        if(Debug.INTERNAL)
            getContext().getLog().info("[VO] round={} bucket={} epoch={} tableId={} batchId={} aggregatedUpdates={}",
                msg.round(),msg.bucketId(), msg.epoch(), msg.tableId(), msg.batchId(), msg.values().size());
        if (msg.bucketId() != bucketId)
            throw new IllegalArgumentException("Message for bucket " + msg.bucketId()+ " sent to value owner " + entityId);

        String batchKey = msg.tableId() + ":" + msg.batchId();
        boolean applied = appliedBatches.add(batchKey);
        int changedValues = 0;
        if (applied) {
            Map<Integer, Map<Integer, Long>> updatesByValue = new HashMap<>();
            for (ValueCount item : msg.values()) {
                updatesByValue.computeIfAbsent(item.valueId(), ignored -> new HashMap<>()).merge(item.colId(), (long) item.count(),
                 Long::sum);
            }

            Map<Integer, Map<Integer, Long>> records =membershipStore.loadBatch(bucketId, updatesByValue.keySet());

            Map<Integer, RoaringBitmap> addedColumnsByValue = new HashMap<>();
            updatesByValue.forEach((valueId, columnUpdates) -> {
                Map<Integer, Long> record = records.get(valueId);
                RoaringBitmap addedColumns = new RoaringBitmap();
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
            msg.ackTo().tell(new BDCommand.ValueBucketFlushed(msg.epoch(), bucketId));
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

    private Map<Integer, Map<Integer, Integer>> calculateMembershipUpdates(Map<Integer, RoaringBitmap> addedColumnsByValue,
            Map<Integer, Map<Integer, Long>> records) {
            Map<Integer, Map<Integer, Integer>> countDeltasByLhs = new HashMap<>();
            addedColumnsByValue.forEach((valueId, addedColumns) -> {
            RoaringBitmap after = new RoaringBitmap();
            records.get(valueId).keySet().forEach(after::add);
            RoaringBitmap before = after.clone();
            before.andNot(addedColumns);

            addedColumns.forEach((int lhsCol) -> {
                for (int rhsCol = 0; rhsCol < metadata.totalCols(); rhsCol++) {
                    if (rhsCol != lhsCol&& testCompatibility(metadata.typeOf(lhsCol), metadata.typeOf(rhsCol))
                            && !after.contains(rhsCol)) {
                        mergeCountDelta(countDeltasByLhs, lhsCol, rhsCol, 1);
                    }
                }
            });

            addedColumns.forEach((int rhsCol) ->before.forEach((int lhsCol) -> {
                        if (lhsCol != rhsCol && testCompatibility(metadata.typeOf(lhsCol), metadata.typeOf(rhsCol))) {
                            mergeCountDelta(countDeltasByLhs, lhsCol, rhsCol, -1);
                        }
                    }));
        });
        return countDeltasByLhs;
    }

    private void emitMembershipUpdates(StoreBatch batch,Map<Integer, RoaringBitmap> addedColumnsByValue,Map<Integer, 
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
                batch.round(),bucketId, batch.epoch(),addedColumnsByValue.values().stream().mapToInt(RoaringBitmap::getCardinality).sum(),
                countDeltasByLhs.size());
    }

    private static void mergeCountDelta(Map<Integer, Map<Integer, Integer>> byLhs,int lhsCol,int rhsCol,int delta) {
        byLhs.computeIfAbsent(lhsCol, ignored -> new HashMap<>())
                .merge(rhsCol, delta, Integer::sum);
    }
}
