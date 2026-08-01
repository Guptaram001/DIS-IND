package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.structures.*;
import disIND.streamBasedShardedDispatcher.utility.Debug;
import disIND.streamBasedShardedDispatcher.utility.UserConfig;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;

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
    private final Map<Integer, Integer> inFlightByCol = new HashMap<>();
    private final Map<Integer, Deque<ColumnDispatch>> queuedByCol = new HashMap<>();
    private final Map<Integer, CheckpointCleanup> checkpointCleanups = new HashMap<>();

    private final Map<String, CachedTableBatch> batchCache = new HashMap<>();
    private static String key(int tableId, int batchId) {
        return tableId + ":" + batchId;
    }

    private record ColumnDispatch(int globalCol, InputBatchDetails details, long[] rows, int[] valueIds) {}
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
        appraiserRef.tell(new AppraiserCommand.FinishDiscovery(msg.finalRound(), rcRef));

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

        // Notify all the ATTRA regarding epoch complete to snapshot
        checkpointCleanups.put(msg.round(),
                new CheckpointCleanup(new HashMap<>(msg.maxBatchIdByTable()), metadata.totalCols(), true));
        for (int col = 0; col < metadata.totalCols(); col++) {
            sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(col)).tell(new AACommand.CheckPoint(epoch,col,
                    msg.round(),msg.maxBatchIdByTable(),selfRef,appraiserRef));
        }
        //Notify the APPA to start asking sketches--- > may need to change since too long waiting time.
        appraiserRef.tell(new AppraiserCommand.CheckPoint(msg.round(),epoch,msg.maxBatchIdByTable()));

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

        int expected = msg.columns().size();
        Deque<ColumnDispatch> dispatches = new ArrayDeque<>();
        for (ColumnBatch column : msg.columns()) {
            int globalCol = column.colId();
            if(Debug.MESSAGE)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),globalCol,"-",
                        String.valueOf(Debug.State.NONE), "Sending table batch: {} rows, {} cols",
                        msg.numRows(), metadata.totalCols());

            InputBatchDetails detailsForCol = new InputBatchDetails(ibd.tableId(), ibd.startRowId(), 
            ibd.batchId(), epoch, ibd.round(), globalCol);

            dispatches.addLast(new ColumnDispatch(globalCol, detailsForCol, column.rowIds(),
             column.valueIds()));
        }
        pendingPerEpoch.put(epoch, new PendingEpoch(expected, msg.replyTo()));
        while (!dispatches.isEmpty()) {
            enqueueOrSend(dispatches.removeFirst());
        }

        statsRef.tell(new StatsCommand.RowBatchProcessed(msg.numRows()));
        return this;
    }


    private Behavior<BDCommand> onBatchFlushed(BDCommand.BatchFlushed msg) {
        //Currently not properly acking from the ATTRA. it is acking but not tested fully .
        releaseAaCredit(msg.colId());
        long ackEpoch = msg.inputBatchDetails().epoch();
        PendingEpoch p = pendingPerEpoch.get(ackEpoch);
        if (p == null) return this;
        int remaining = p.remaining() - 1;
        if (remaining > 0) {
            pendingPerEpoch.put(ackEpoch, new PendingEpoch(remaining, p.replyTo()));
            return this;
        }
        pendingPerEpoch.remove(ackEpoch);
        if (p.replyTo() != null)
            p.replyTo().tell(new BDReply.BatchAccepted(ackEpoch));
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),msg.colId(),"-",
                    String.valueOf(Debug.State.NONE), " BatchFlushed completed epoch={} col={}",
                    ackEpoch, msg.colId());
        return this;
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
        resendBatchToColumn(cached, msg.colId());
        return this;
    }

    private void resendBatchToColumn(CachedTableBatch cached, int globalCol) {
        InputBatchDetails ibd = cached.inputBatchDetails();

        ColumnBatch column = cached.columns().stream().filter(candidate -> candidate.colId() == globalCol)
                .findFirst().orElse(null);
        if (column == null) return;

        InputBatchDetails resendDetails = new InputBatchDetails(ibd.tableId(), ibd.startRowId(), 
        ibd.batchId(), ibd.epoch(),ibd.round(), globalCol);

        enqueueOrSend(new ColumnDispatch(globalCol, resendDetails, column.rowIds(), column.valueIds()));
    }

    private void enqueueOrSend(ColumnDispatch dispatch) {
        int inFlight = inFlightByCol.getOrDefault(dispatch.globalCol(), 0);
        if (inFlight < UserConfig.BD_AA_CREDIT_WINDOW) {
            sendToAa(dispatch);
            return;
        }
        queuedByCol.computeIfAbsent(dispatch.globalCol(), ignored -> new ArrayDeque<>()).addLast(dispatch);
    }

    private void sendToAa(ColumnDispatch dispatch) {
        inFlightByCol.merge(dispatch.globalCol(), 1, Integer::sum);
        sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(dispatch.globalCol()))
                .tell(new AACommand.InsertBatch(dispatch.details(), dispatch.rows(), dispatch.valueIds(), selfRef));
    }

    private void releaseAaCredit(int colId) {
        int remaining = inFlightByCol.getOrDefault(colId, 0) - 1;
        if (remaining <= 0)
            inFlightByCol.remove(colId);
        else
            inFlightByCol.put(colId, remaining);

        Deque<ColumnDispatch> queue = queuedByCol.get(colId);
        if (queue == null || queue.isEmpty())
            return;

        while (inFlightByCol.getOrDefault(colId, 0) < UserConfig.BD_AA_CREDIT_WINDOW && !queue.isEmpty()) {
            sendToAa(queue.removeFirst());
        }
        if (queue.isEmpty())
            queuedByCol.remove(colId);
    }

    private void evictCachedBatchesThrough(Map<Integer, Integer> maxBatchIdByTable) {
        batchCache.entrySet().removeIf(entry -> {
            CachedTableBatch cached = entry.getValue();
            Integer maxBatchId = maxBatchIdByTable.get(cached.inputBatchDetails().tableId());
            return maxBatchId != null && cached.inputBatchDetails().batchId() <= maxBatchId;
        });
    }


}
