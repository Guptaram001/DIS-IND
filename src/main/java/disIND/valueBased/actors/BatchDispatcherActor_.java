package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import disIND.valueBased.model.SharedModel.*;
import disIND.valueBased.monitor.StatsCommand;
import disIND.valueBased.structures.*;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.utility.UserConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import static disIND.valueBased.utility.Debug.formLog;

public class BatchDispatcherActor_  extends AbstractBehavior<BDCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final ClusterSharding sharding;
    private final ActorRef<AppraiserCommand> appraiserRef;
    private final ActorRef<BDCommand> selfRef;

    private final ActorRef<RCCommand> rcRef;
    private ActorRef<BDReply> finishReplyTo;
    private int finalRound = -1;
    private boolean finishRequested = false;

    private long epoch     = 0L;
    private final Map<Long, PendingEpoch> pendingPerEpoch = new HashMap<>();
    private final Map<Integer, CheckpointCleanup> checkpointCleanups = new HashMap<>();

    private final Map<String, CachedTableBatch> batchCache = new HashMap<>();
    private static String key(int tableId, int batchId) {
        return tableId + ":" + batchId;
    }

    private record CheckpointCleanup(Map<Integer, Integer> maxBatchIdByTable, int remaining, boolean clean) {}



    public static Behavior<BDCommand> create(ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef,ActorRef<RCCommand> rcRef) {
        return Behaviors.setup(ctx ->
                new BatchDispatcherActor_(ctx, sharding, appraiserRef,metadata,statsRef,rcRef));
    }

    private BatchDispatcherActor_(ActorContext<BDCommand> ctx, ClusterSharding sharding,
                                  ActorRef<AppraiserCommand> appraiserRef,DatasetMetadata metadata,
                                  ActorRef<StatsCommand> statsRef,ActorRef<RCCommand> rcRef) {
        super(ctx);
        getContext().getLog().info("BD STARTED");
        this.sharding     = sharding;
        this.appraiserRef = appraiserRef;
        this.selfRef      = ctx.getSelf();
        this.metadata     = metadata;
        this.statsRef     = statsRef;
        this.rcRef        = rcRef;
    }

    @Override
    public Receive<BDCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(BDCommand.SendTableBatch.class, this::onSendTableBatch)
                .onMessage(BDCommand.BatchFlushed.class, this::onBatchFlushed)
                .onMessage(BDCommand.ValueBucketFlushed.class, this::onValueBucketFlushed)
                .onMessage(BDCommand.FinishDiscovery.class, this::onFinishDiscovery)
                .onMessage(BDCommand.CheckPoint.class,this::onCheckPoint)
                .onMessage(BDCommand.MissingBatchRequest.class, this::onMissingBatchRequest)
                .onMessage(BDCommand.AaCheckpointStatus.class, this::onAaCheckpointStatus)
                .onAnyMessage(msg -> {getContext().getLog().info("BD GOT {}", msg.getClass());return this;})
                .build();
    }

    private Behavior<BDCommand> onFinishDiscovery(BDCommand.FinishDiscovery msg) {
        this.finishRequested = true;
        this.finishReplyTo = msg.replyTo();
        this.finalRound = msg.finalRound();
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),-1,"-",
                    String.valueOf(Debug.State.NONE), "FinishDiscovery received finalRound={} finalBatchByTable={}",
                    msg.finalRound(), msg.finalBatchByTable());
        selfRef.tell(new BDCommand.CheckPoint(msg.finalRound(), msg.finalBatchByTable()));
        rcRef.tell(new RCCommand.AwaitDiscoveryFinished(msg.finalRound(), msg.replyTo()));
        rcRef.tell(new RCCommand.PipelineDone());

        return this;
    }

    private Behavior<BDCommand> onAaCheckpointStatus(BDCommand.AaCheckpointStatus msg) {
        if (!msg.clean()) {
            for (InputBatchDetails m : msg.missing()) {
                selfRef.tell(new BDCommand.MissingBatchRequest(m.tableId(), m.batchId(), msg.colId()));
            }
        }
        CheckpointCleanup cleanup = checkpointCleanups.get(msg.round());
        if (cleanup != null) {
            int remaining = cleanup.remaining() - 1;
            boolean clean = cleanup.clean() && msg.clean();
            if (remaining <= 0) {
                checkpointCleanups.remove(msg.round());
                if (clean)
                    evictCachedBatchesThrough(cleanup.maxBatchIdByTable());
            } else {
                checkpointCleanups.put(msg.round(),
                        new CheckpointCleanup(cleanup.maxBatchIdByTable(), remaining, clean));
            }
        }
        return this;
    }

    private Behavior<BDCommand> onCheckPoint(BDCommand.CheckPoint msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),-1,"-",
                    String.valueOf(Debug.State.NONE), "Received CheckPoint Message for round: {}", msg.round());
        evictCachedBatchesThrough(msg.maxBatchIdByTable());

        return this;
    }


    private Behavior<BDCommand> onSendTableBatch(BDCommand.SendTableBatch msg) {
        epoch++;

        InputBatchDetails incomingDetails = msg.inputBatchDetails();
        InputBatchDetails ibd = new InputBatchDetails(incomingDetails.tableId(),incomingDetails.startRowId(),
                incomingDetails.batchId(),epoch,incomingDetails.round(),-1);
        batchCache.put(key(ibd.tableId(), ibd.batchId()),
                new CachedTableBatch(ibd, msg.columns(), msg.numRows()));

        if (msg.columns().isEmpty()) {
            if (msg.replyTo() != null)
                msg.replyTo().tell(new BDReply.BatchAccepted(epoch));
            statsRef.tell(new StatsCommand.RowBatchProcessed(msg.numRows()));
            return this;
        }

        Map<Integer, List<ValueOwnerActor.ValueCount>> valueBuckets =bucketByValue(msg.columns());
        int inputValues = msg.columns().stream().mapToInt(column -> column.valueIds().length).sum();
        int aggregatedValueColumns = valueBuckets.values().stream().mapToInt(List::size).sum();
        getContext().getLog().info("[VALUE-DISPATCH] epoch={} tableId={} batchId={} inputValues={} "
                        + "aggregatedValueColumns={} targetBuckets={}",epoch, ibd.tableId(), ibd.batchId(), inputValues,
                aggregatedValueColumns, valueBuckets.size());
        int expected = valueBuckets.size();
        if (expected == 0) {
            if (msg.replyTo() != null)
                msg.replyTo().tell(new BDReply.BatchAccepted(epoch));
            statsRef.tell(new StatsCommand.RowBatchProcessed(msg.numRows()));
            return this;
        }
        pendingPerEpoch.put(epoch, new PendingEpoch(expected, msg.replyTo()));
        valueBuckets.forEach((bucketId, values) ->sharding.entityRefFor(ValueOwnerActor.TYPE_KEY,
                                ValueOwnerActor.entityId(bucketId))
                                .tell(new ValueOwnerActor.StoreBatch(epoch, ibd.tableId(),
                                ibd.batchId(), bucketId, values, selfRef)));
        statsRef.tell(new StatsCommand.RowBatchProcessed(msg.numRows()));
        return this;
    }


    private Behavior<BDCommand> onBatchFlushed(BDCommand.BatchFlushed msg) {
        //Currently not properly acking from the ATTRA. it is acking but not tested fully .
        long ackEpoch = msg.inputBatchDetails().epoch();
        completeEpochPart(ackEpoch);
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),msg.colId(),"-",
                    String.valueOf(Debug.State.NONE), " BatchFlushed completed epoch={} col={}",
                    ackEpoch, msg.colId());
        return this;
    }

    private Behavior<BDCommand> onValueBucketFlushed(BDCommand.ValueBucketFlushed msg) {
        completeEpochPart(msg.epoch());
        return this;
    }

    private void completeEpochPart(long ackEpoch) {
        PendingEpoch p = pendingPerEpoch.get(ackEpoch);
        if (p == null) return;
        int remaining = p.remaining() - 1;
        if (remaining > 0) {
            pendingPerEpoch.put(ackEpoch, new PendingEpoch(remaining, p.replyTo()));
            return;
        }
        pendingPerEpoch.remove(ackEpoch);
        if (p.replyTo() != null)
            p.replyTo().tell(new BDReply.BatchAccepted(ackEpoch));
    }

    static Map<Integer, List<ValueOwnerActor.ValueCount>> bucketByValue(List<ColumnBatch> columns) {
        Map<Integer, Map<Long, Integer>> countsByBucket = new HashMap<>();
        for (ColumnBatch column : columns) {
            for (int valueId : column.valueIds()) {
                int bucketId = Math.floorMod(valueId, UserConfig.VALUE_OWNER_BUCKETS);
                long valueAndColumn = ((long) valueId << 32)| (column.colId() & 0xffffffffL);
                countsByBucket.computeIfAbsent(bucketId, ignored -> new HashMap<>())
                        .merge(valueAndColumn, 1, Integer::sum);
            }
        }
        Map<Integer, List<ValueOwnerActor.ValueCount>> result = new HashMap<>();
        countsByBucket.forEach((bucketId, counts) -> 
        {List<ValueOwnerActor.ValueCount> values = new ArrayList<>(counts.size());
            counts.forEach((key, count) -> values.add(new ValueOwnerActor.ValueCount(
                    (int) (key >> 32), (int) (long) key, count)));
            result.put(bucketId, List.copyOf(values));
        });
        return result;
    }
    private Behavior<BDCommand> onMissingBatchRequest(BDCommand.MissingBatchRequest msg) {
        CachedTableBatch cached = batchCache.get(key(msg.tableId(), msg.batchId()));
        if (cached == null) {
            if(Debug.INTERNAL)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.bd(),msg.colId(),"-",
                        String.valueOf(Debug.State.NONE), "Missing Batch Request for tableID: {} colID: {} batchID: {}",
                        msg.tableId(), msg.colId(),msg.batchId());
            return this;
        }
        getContext().getLog().debug("Ignoring attribute missing-batch request in value-owner mode table={} batch={} col={}",
                msg.tableId(), msg.batchId(), msg.colId());
        return this;
    }

    private void evictCachedBatchesThrough(Map<Integer, Integer> maxBatchIdByTable) {
        batchCache.entrySet().removeIf(entry -> {
            CachedTableBatch cached = entry.getValue();
            Integer maxBatchId = maxBatchIdByTable.get(cached.inputBatchDetails().tableId());
            return maxBatchId != null && cached.inputBatchDetails().batchId() <= maxBatchId;
        });
    }


}
