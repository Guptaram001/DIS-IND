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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchDispatcherActor_  extends AbstractBehavior<BDCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final int numCols;
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

    private final Map<Long, Integer> pendingPerEpoch = new HashMap<>();
    private long    lastSentEpoch     = -1L;
    private boolean ingestionDoneReceived = false;



    public static Behavior<BDCommand> create(int numCols, ValueIdMap vidMap, ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new BatchDispatcherActor_(ctx, numCols, vidMap, sharding, appraiserRef,metadata,statsRef));
    }

    private BatchDispatcherActor_(ActorContext<BDCommand> ctx, int numCols, ValueIdMap valueIdMap,
                                  ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                  DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        getContext().getLog().info("BD STARTED");
        this.numCols      = numCols;
        this.valueIdMap   = valueIdMap;
        this.sharding     = sharding;
        this.appraiserRef = appraiserRef;
        this.selfRef      = ctx.getSelf();
        this.cursors      = new int[numCols];
        this.metadata     = metadata;
        this.statsRef     = statsRef;
    }

    @Override
    public Receive<BDCommand> createReceive() {
        getContext().getLog().info("BD receive created");
        return newReceiveBuilder()
                .onMessage(BDCommand.SendTableBatch.class, this::onSendTableBatch)
                .onMessage(BDCommand.BatchFlushed.class, this::onBatchFlushed)
                .onMessage(BDCommand.IngestionDone.class, this::onIngestionDone)
                .onAnyMessage(msg -> {getContext().getLog().info("BD GOT {}", msg.getClass());return this;})
                .build();
    }



    private Behavior<BDCommand> onSendTableBatch(BDCommand.SendTableBatch msg) {
        getContext().getLog().info("[BD] Initial Sending table batch: {} rows, {} cols", msg.rows().size(), numCols);
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
        long e = epoch;
        for (int localCol = 0; localCol < localCols; localCol++) {
            int count = cursors[localCol];
            if (count == 0)
                continue;
            expected++;
            int globalCol = offset + localCol;
            int base = localCol * bufCapacity;
            long[] rArr = Arrays.copyOfRange(rowBuffer, base, base + count);
            int[] vArr = Arrays.copyOfRange(vidBuffer, base, base + count);
            getContext().getLog().info("[BD] Finally Sending table batch: {} rows, {} cols", msg.rows().size(), numCols);
            sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(globalCol))
                    .tell(new AACommand.InsertBatch(e, rArr, vArr, selfRef));
        }

        if (expected == 0) {
            appraiserRef.tell(new AppraiserCommand.EpochComplete(e));
            maybeForwardIngestionDone(e);
        } else
            pendingPerEpoch.put(e, expected);

        statsRef.tell(new StatsCommand.RowBatchProcessed(rows.size()));
        return this;
    }

    private Behavior<BDCommand> onIngestionDone(BDCommand.IngestionDone msg) {
        ingestionDoneReceived = true;
        lastSentEpoch = epoch;
        if (!pendingPerEpoch.containsKey(lastSentEpoch)) {
            appraiserRef.tell(new AppraiserCommand.IngestionDone());
            ingestionDoneReceived = false;
        }
        return this;
    }

    private Behavior<BDCommand> onBatchFlushed(BDCommand.BatchFlushed msg) {
        long e = msg.epoch();
        Integer remaining = pendingPerEpoch.get(e);
        if (remaining == null) {
            getContext().getLog().info("[BD] BatchFlushed unknown/completed epoch={} col={}", e, msg.colId());
            return this;
        }
        int nr = remaining - 1;
        if (nr > 0) {
            pendingPerEpoch.put(e, nr);
            return this;
        }
        pendingPerEpoch.remove(e);
        appraiserRef.tell(new AppraiserCommand.EpochComplete(e));
        maybeForwardIngestionDone(e);
        return this;
    }

    private void maybeForwardIngestionDone(long completedEpoch) {
        if (ingestionDoneReceived && completedEpoch == lastSentEpoch) {
            ingestionDoneReceived = false;
            appraiserRef.tell(new AppraiserCommand.IngestionDone());
        }
    }

}
