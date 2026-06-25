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

    private long epoch     = 0L;

    private long[] rowBuffer;
    private int[] vidBuffer;
    private int bufCapacity = 0;
    private int localBufCols = 0;

    private final Map<Long, PendingEpoch> pendingPerEpoch = new HashMap<>();
    private long    lastSentEpoch     = -1L;
    private boolean ingestionDoneReceived = false;



    public static Behavior<BDCommand> create(ValueIdMap vidMap, ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new BatchDispatcherActor_(ctx,  vidMap, sharding, appraiserRef,metadata,statsRef));
    }

    private BatchDispatcherActor_(ActorContext<BDCommand> ctx, ValueIdMap valueIdMap,
                                  ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                  DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        getContext().getLog().info("BD STARTED");
        this.valueIdMap   = valueIdMap;
        this.sharding     = sharding;
        this.appraiserRef = appraiserRef;
        this.selfRef      = ctx.getSelf();
        this.cursors      = new int[metadata.totalCols()];
        this.metadata     = metadata;
        this.statsRef     = statsRef;
    }

    @Override
    public Receive<BDCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(BDCommand.SendTableBatch.class, this::onSendTableBatch)
                .onMessage(BDCommand.BatchFlushed.class, this::onBatchFlushed)
                .onMessage(BDCommand.IngestionDone.class, this::onIngestionDone)
                .onAnyMessage(msg -> {getContext().getLog().info("BD GOT {}", msg.getClass());return this;})
                .build();
    }



    private Behavior<BDCommand> onSendTableBatch(BDCommand.SendTableBatch msg) {
        epoch++;

        int tableId = msg.tableId();
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
            long rowId = msg.startRowId() + r;
            int limit = Math.min(localCols, row.length);
            for (int localCol = 0; localCol < limit; localCol++) {
                String value = row[localCol];
                if (value == null || value.isEmpty())
                    continue;
                int cur = cursors[localCol];
                int base = localCol * bufCapacity;
                rowBuffer[base + cur] = rowId;
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
            sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(globalCol))
                    .tell(new AACommand.InsertBatch(epoch, rArr, vArr, selfRef));
        }

        if (epoch % metadata.totalCols() == 0) {
            if(Debug.MESSAGE)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),-1,"-",
                        String.valueOf(Debug.State.NONE), "Sending epoch complete message to all ATTRA and APPA");
            //Notify the APPA to start asking sketches
            appraiserRef.tell(new AppraiserCommand.EpochComplete(epoch));
            // Notify all the ATTRA regarding epoch complete to snapshot
            for (int col = 0; col < metadata.totalCols(); col++) {
                sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(col)).tell(new AACommand.EpochComplete(epoch,col));
            }
        }

        if (expected == 0) {
            if (msg.replyTo() != null)
                msg.replyTo().tell(new BDReply.BatchAccepted(epoch));
            maybeForwardIngestionDone(epoch);
        } else
            pendingPerEpoch.put(epoch, new PendingEpoch(expected, msg.replyTo()));

        statsRef.tell(new StatsCommand.RowBatchProcessed(rows.size()));
        return this;
    }

    private Behavior<BDCommand> onIngestionDone(BDCommand.IngestionDone msg) {
        ingestionDoneReceived = true;
        lastSentEpoch = epoch;
        ActorRef<BDReply> finishReplyTo = msg.replyTo();
        if (pendingPerEpoch.isEmpty()) {
            appraiserRef.tell(new AppraiserCommand.IngestionDone(epoch, getContext().getSelf()));
        }
        return this;
    }

    private Behavior<BDCommand> onBatchFlushed(BDCommand.BatchFlushed msg) {
        //Currently not properly acking from the ATTRA. it is acking but not tested fully .
        PendingEpoch p = pendingPerEpoch.get(msg.epoch());
        if (p == null) return this;
        int remaining = p.remaining() - 1;
        if (remaining > 0) {
            pendingPerEpoch.put(msg.epoch(), new PendingEpoch(remaining, p.replyTo()));
            return this;
        }
        pendingPerEpoch.remove(msg.epoch());
        if (p.replyTo() != null)
            p.replyTo().tell(new BDReply.BatchAccepted(msg.epoch()));

        maybeForwardIngestionDone(msg.epoch());
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.bd(),msg.colId(),"-",
                    String.valueOf(Debug.State.NONE), " BatchFlushed unknown/completed epoch={} col={}",
                    msg.epoch(), msg.colId());
        return this;
    }

    private void maybeForwardIngestionDone(long completedEpoch) {
        if (ingestionDoneReceived && completedEpoch == lastSentEpoch) {
            ingestionDoneReceived = false;
            appraiserRef.tell(new AppraiserCommand.IngestionDone(completedEpoch, getContext().getSelf()));
        }
    }


}
