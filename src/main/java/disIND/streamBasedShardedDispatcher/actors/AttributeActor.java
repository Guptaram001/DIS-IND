package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import disIND.streamBasedShardedDispatcher.model.SharedModel;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.structures.*;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class AttributeActor extends AbstractBehavior<AACommand> {
    public static final EntityTypeKey<AACommand> TYPE_KEY = EntityTypeKey.create(AACommand.class, "AttributeActor");
    private final ActorRef<StatsCommand> statsRef;

    private final int colId;
    private final ValueIdMap valueIdMap;
    private final AtomicReference<ActorRef<CMCommand>> cmRef;
    private final Map<UnaryPair, ActorRef<CMCommand>> subscribers = new HashMap<>();

    private final BitmapStore bitmapStore=new BitmapStore();
    private final ValueToRowsStore valueToRowsStore = new ValueToRowsStore();
    private final SketchStore sketchStore = new SketchStore();
    private long epochsProcessed = 0L;

    private AttributeCheckPoint checkPoint ;
    private final Deque<AttributeDelta> deltaLogDequeue = new ArrayDeque<>();

    public static Behavior<AACommand> create(int colId, ValueIdMap vidMap,
                                             AtomicReference<ActorRef<CMCommand>> cmRef, ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new AttributeActor(ctx, colId, vidMap, cmRef
        ,statsRef));
    }

    private AttributeActor(ActorContext<AACommand> ctx, int colId, ValueIdMap valueIdMap,
                           AtomicReference<ActorRef<CMCommand>> cmRef,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.colId      = colId;

        this.valueIdMap = valueIdMap;
        this.cmRef   = cmRef;
        this.statsRef   = statsRef;
    }


    @Override
    public Receive<AACommand> createReceive() {

        return newReceiveBuilder()
                .onMessage(AACommand.InsertBatch.class,this::onInsertBatch)
                .onMessage(AACommand.EmitSketch.class,this::onEmitSketch)
                .onMessage(AACommand.EpochComplete.class,this::onEpochComplete)
                .onMessage(AACommand.SendColumnData.class,this::onSendColumnData)
                .onMessage(AACommand.CompareBitmap.class,this::onCompareBitmap)
                .build();
    }

    private Behavior<AACommand> onSendColumnData(AACommand.SendColumnData sendColumnData) {
        getContext().getLog().info("[ATTRA] Invoked for sending column col:{} to col: {}", sendColumnData.candidate().pair().lhsCol(),
                sendColumnData.candidate().pair().rhsCol());
        RoaringBitmap lhsBitmap = checkPoint.bitmapStore().getBitmap();
        ActorRef<CMCommand> cmRef = sendColumnData.cmRef();
        subscribers.put(sendColumnData.candidate().pair(), sendColumnData.cmRef());
        sendColumnData.aaRef().tell(new AACommand.CompareBitmap(sendColumnData.candidate(),lhsBitmap,cmRef));
        return this;
    }

    private Behavior<AACommand> onCompareBitmap(AACommand.CompareBitmap compareBitmap) {
        getContext().getLog().info("[ATTRA] Comparing {} ⊆ {}", compareBitmap.candidate().pair().lhsCol(),
                compareBitmap.candidate().pair().rhsCol());
        subscribers.put(compareBitmap.candidate().pair(), compareBitmap.cmRef());
        ScanResult result = checkPoint.bitmapStore().compareAgainst(
                                compareBitmap.lhsBitmap(),
                                compareBitmap.candidate().pair(),
                                compareBitmap.candidate().evalEpoch());

        compareBitmap.cmRef().tell(new CMCommand.UnaryViolationReport(result));

        return this;
    }

    private Behavior<AACommand> onEpochComplete(AACommand.EpochComplete epochComplete) {
        getContext().getLog().info("[ATTRA] Snapshotting Epoch {} complete for col {}", epochComplete.epoch(), colId);
        if (epochComplete.epoch() >= epochsProcessed) {
            checkPoint=new AttributeCheckPoint(epochComplete.epoch(),
                    bitmapStore.deepCopy(),
                    valueToRowsStore.deepCopy(),
                    sketchStore.getSummary(colId, epochComplete.epoch()));

        }
        cleanupOldDeltas(epochComplete.epoch());
        return this;
    }

    private Behavior<AACommand> onEmitSketch(AACommand.EmitSketch msg) {
        getContext().getLog().info("[ATTRA] Emitting sketch for col {} epoch {}", colId, msg.epoch());
        msg.replyTo().tell(new AppraiserCommand.SketchArrived(sketchStore.getSummary(colId, msg.epoch())));
        return this;
    }

    private Behavior<AACommand> onInsertBatch(AACommand.InsertBatch insertBatch) {
        getContext().getLog().info("[ATTRA] Received insert batch for col {} with {} rows", colId, insertBatch.rows().length);

        int[] valueIds    = insertBatch.valueIds();
        long[] rows   = insertBatch.rows();
        AttributeDeltaBuilder deltaBuilder = new AttributeDeltaBuilder(insertBatch.epoch());

        for (int i = 0; i < rows.length; i++) {
            int vid  = valueIds[i];
            int rowI = (int) rows[i];
            sketchStore.insert(vid);
            getContext().getLog().info("[ATTRA] Adding row {} to bitmap for value {}", rowI, vid);

            boolean newValue = !valueToRowsStore.containsValue(vid);
            valueToRowsStore.add(vid, rowI);
            if (newValue)
                bitmapStore.insertIds(new int[]{vid});
            deltaBuilder.addInsert(vid,rowI);
        }

        if (insertBatch.ackTo() != null)
            insertBatch.ackTo().tell(new BDCommand.BatchFlushed(insertBatch.epoch(), colId));

        deltaLogDequeue.addLast(deltaBuilder.build());
        epochsProcessed=insertBatch.epoch();
        return this;
    }

    private void cleanupOldDeltas(long checkpointEpoch){
        while(!deltaLogDequeue.isEmpty() && deltaLogDequeue.peekFirst().epoch() <= checkpointEpoch){
            deltaLogDequeue.removeFirst();
        }
    }



    // After processing values add
//    valuesSinceLastStats += ids.length;
//
//if (valuesSinceLastStats >= 100_000) {
//        statsRef.tell(
//                new StatsCommand.AttributeStats(
//                        colId,
//                        bitmapStore.cardinality(),
//                        bitmapStore.cardinality(),
//                        minHash.cardinality()
//                )
//        );
//
//        valuesSinceLastStats = 0;
//    }

}
