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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;

public class BatchDispatcherActor_  extends AbstractBehavior<BDCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final ValueIdMap valueIdMap;
    private final ClusterSharding sharding;
    private final ActorRef<AppraiserCommand> appraiserRef;
    private final ActorRef<BDCommand> selfRef;
    private int[]  cursors;

    private final ActorRef<RCCommand> rcRef;
    private ActorRef<BDReply> finishReplyTo;
    private int finalRound = -1;
    private boolean finishRequested = false;

    private long epoch     = 0L;
    private long[] rowBuffer;
    private int[] vidBuffer;
    private int bufCapacity = 0;
    private int localBufCols = 0;

    private final Map<Long, PendingEpoch> pendingPerEpoch = new HashMap<>();

    private final Map<String, CachedTableBatch> batchCache = new HashMap<>();
    private static String key(int tableId, int batchId) {
        return tableId + ":" + batchId;
    }



    public static Behavior<BDCommand> create(ValueIdMap vidMap, ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef,ActorRef<RCCommand> rcRef) {
        return Behaviors.setup(ctx ->
                new BatchDispatcherActor_(ctx,  vidMap, sharding, appraiserRef,metadata,statsRef,rcRef));
    }

    private BatchDispatcherActor_(ActorContext<BDCommand> ctx, ValueIdMap valueIdMap,
                                  ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                  DatasetMetadata metadata,ActorRef<StatsCommand> statsRef,ActorRef<RCCommand> rcRef) {
        super(ctx);
        getContext().getLog().info("BD STARTED");
        this.valueIdMap   = valueIdMap;
        this.sharding     = sharding;
        this.appraiserRef = appraiserRef;
        this.selfRef      = ctx.getSelf();
        this.cursors      = new int[metadata.totalCols()];
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
        return this;
    }

    private Behavior<BDCommand> onCheckPoint(BDCommand.CheckPoint msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),-1,"-",
                    String.valueOf(Debug.State.NONE), "Received CheckPoint Message for round: {}", msg.round());

        // Notify all the ATTRA regarding epoch complete to snapshot
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

        InputBatchDetails ibd = msg.inputBatchDetails();
        batchCache.put(key(ibd.tableId(), ibd.batchId()), new CachedTableBatch(ibd, msg.rows()));

        int tableId = msg.inputBatchDetails().tableId();
        List<String[]> rows = msg.rows();

        if (rows.isEmpty()) {
            if (msg.replyTo() != null)
                msg.replyTo().tell(new BDReply.BatchAccepted(epoch));
            return this;
        }

        int offset = metadata.offsets().get(tableId);
        int localCols = metadata.nCols().get(tableId);
        int numRows = rows.size();

        if (numRows > bufCapacity || localCols > localBufCols) {
            bufCapacity = Math.max(bufCapacity, numRows);
            localBufCols = Math.max(localBufCols, localCols);

            rowBuffer = new long[localBufCols * bufCapacity];
            vidBuffer = new int[localBufCols * bufCapacity];
            cursors = new int[localBufCols];
        }
        Arrays.fill(cursors, 0);

        for (int r = 0; r < numRows; r++) {
            String[] row = rows.get(r);
            long rowId = msg.inputBatchDetails().startRowId() + r;
            int limit = Math.min(localCols, row.length);
            for (int localCol = 0; localCol < limit; localCol++) {
                String value = row[localCol];
                if (value == null || value.isEmpty())
                    continue;
                int cur = cursors[localCol];
                int base = localCol * bufCapacity;
                rowBuffer[base + cur] = rowId;
                //ValueId map checking
                //int id = valueIdMap.getOrInsert(value);
                //System.out.printf("raw='%s' -> id=%d, round=%d %n", value, id,msg.inputBatchDetails().round());
                vidBuffer[base + cur] = valueIdMap.getOrInsert(value);
                cursors[localCol] = cur + 1;
            }
        }

        if (msg.replyTo() != null)
            msg.replyTo().tell(new BDReply.BatchAccepted(epoch));

        int expected = 0;
        for (int localCol = 0; localCol < localCols; localCol++) {
            int count = cursors[localCol];
            if (count == 0)
                continue;
            expected++;
            int globalCol = offset + localCol;
            int base = localCol * bufCapacity;
            long[] rArr = Arrays.copyOfRange(rowBuffer, base, base + count);
            int[] vArr = Arrays.copyOfRange(vidBuffer, base, base + count);
            if(Debug.MESSAGE)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),globalCol,"-",
                        String.valueOf(Debug.State.NONE), "Sending table batch: {} rows, {} cols",
                        msg.rows().size(), metadata.totalCols());

            InputBatchDetails detailsForCol = new InputBatchDetails(msg.inputBatchDetails().tableId(),
                    msg.inputBatchDetails().startRowId(), msg.inputBatchDetails().batchId(), epoch,
                    msg.inputBatchDetails().round(), globalCol);

            sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(globalCol))
                    .tell(new AACommand.InsertBatch(detailsForCol, rArr, vArr, selfRef));
        }
        if (expected == 0) {
            //maybeForwardIngestionDone(epoch);
        } else
            pendingPerEpoch.put(epoch, new PendingEpoch(expected, msg.replyTo()));

        statsRef.tell(new StatsCommand.RowBatchProcessed(rows.size()));
        return this;
    }


    private Behavior<BDCommand> onBatchFlushed(BDCommand.BatchFlushed msg) {
        //Currently not properly acking from the ATTRA. it is acking but not tested fully .
        long ackEpoch = msg.inputBatchDetails().epoch();
        PendingEpoch p = pendingPerEpoch.get(ackEpoch);
        if (p == null) return this;
        int remaining = p.remaining() - 1;
        if (remaining > 0) {
            pendingPerEpoch.put(ackEpoch, new PendingEpoch(remaining, p.replyTo()));
            return this;
        }
        pendingPerEpoch.remove(ackEpoch);

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

        int offset = metadata.offsets().get(ibd.tableId());
        int localCol = globalCol - offset;

        if (localCol < 0 || localCol >= metadata.nCols().get(ibd.tableId())) {
            return;
        }

        List<String[]> rows = cached.rows();
        long[] rArr = new long[rows.size()];
        int[] vArr = new int[rows.size()];
        int count = 0;

        for (int r = 0; r < rows.size(); r++) {
            String[] row = rows.get(r);
            if (localCol >= row.length) continue;

            String value = row[localCol];
            if (value == null || value.isEmpty()) continue;

            rArr[count] = ibd.startRowId() + r;
            vArr[count] = valueIdMap.getOrInsert(value);
            count++;
        }

        rArr = Arrays.copyOf(rArr, count);
        vArr = Arrays.copyOf(vArr, count);

        InputBatchDetails resendDetails = new InputBatchDetails(ibd.tableId(), ibd.startRowId(), ibd.batchId(), ibd.epoch(),
                ibd.round(), globalCol);

        sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(globalCol))
                .tell(new AACommand.InsertBatch(resendDetails, rArr, vArr, selfRef));
    }


}
