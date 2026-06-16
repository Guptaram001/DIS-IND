package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import disIND.streamBasedShardedDispatcher.model.SharedModel;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.structures.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AttributeActor extends AbstractBehavior<AACommand> {
    public static final EntityTypeKey<AACommand> TYPE_KEY = EntityTypeKey.create(AACommand.class, "AttributeActor");
    private final ActorRef<StatsCommand> statsRef;

    private final int colId;
    private final ValueIdMap valueIdMap;
    private final AtomicReference<ActorRef<CMCommand>> cmRef;

    private final BitmapStore bitmapStore=new BitmapStore();
    private final ValueToRowsStore valueToRowsStore = new ValueToRowsStore();
    private final SketchStore sketchStore = new SketchStore();
    private long epochsProcessed = 0L;

    private final Map<Long, AttributeSnapshot> snapshots = new HashMap<>();

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
                .build();
    }

    private Behavior<AACommand> onEpochComplete(AACommand.EpochComplete epochComplete) {
        getContext().getLog().info("[ATTRA] Snapshotting Epoch {} complete for col {}", epochComplete.epoch(), colId);
        if (epochComplete.epoch() >= epochsProcessed) {
            if (!snapshots.containsKey(epochComplete.epoch())) {
                snapshots.put(epochComplete.epoch(), new AttributeSnapshot(
                                epochComplete.epoch(),
                                bitmapStore.deepCopy(),
                                valueToRowsStore.deepCopy(),
                                sketchStore.getSummary(colId, epochComplete.epoch())
                        )
                );
            }
        }
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

        for (int i = 0; i < rows.length; i++) {
            int vid  = valueIds[i];
            int rowI = (int) rows[i];
            sketchStore.insert(vid);
            getContext().getLog().info("[ATTRA] Adding row {} to bitmap for value {}", rowI, vid);

            boolean newValue = !valueToRowsStore.containsValue(vid);
            valueToRowsStore.add(vid, rowI);
            if (newValue)
                bitmapStore.insertIds(new int[]{vid});
        }

        if (insertBatch.ackTo() != null)
            insertBatch.ackTo().tell(new BDCommand.BatchFlushed(insertBatch.epoch(), colId));

        epochsProcessed=insertBatch.epoch();
        return this;
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
