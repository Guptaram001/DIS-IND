package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.structures.*;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AttributeActor extends AbstractBehavior<AACommand> {
    public static final EntityTypeKey<AACommand> TYPE_KEY = EntityTypeKey.create(AACommand.class, "AttributeActor");
    private final ActorRef<StatsCommand> statsRef;

    private final int colId;
    private final ValueIdMap valueIdMap;
    private final WatermarkRegister wmReg;
    private final AtomicReference<ActorRef<CMCommand>> cmRef;

    private final Map<Integer, RoaringBitmap> valueToRows = new HashMap<>();
    private final BitmapStore bitmapStore=new BitmapStore();
    private long epochsProcessed = 0L;


    public static Behavior<AACommand> create(int colId, ValueIdMap vidMap, WatermarkRegister wmReg,
                                             AtomicReference<ActorRef<CMCommand>> cmRef, ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new AttributeActor(ctx, colId, vidMap, wmReg, cmRef
        ,statsRef));
    }

    private AttributeActor(ActorContext<AACommand> ctx, int colId, ValueIdMap valueIdMap, WatermarkRegister wmReg,
                           AtomicReference<ActorRef<CMCommand>> cmRef,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.colId      = colId;
        this.valueIdMap = valueIdMap;
        this.wmReg      = wmReg;
        this.cmRef   = cmRef;
        this.statsRef   = statsRef;
    }


    @Override
    public Receive<AACommand> createReceive() {

        return newReceiveBuilder()
                .onMessage(AACommand.InsertBatch.class,this::onInsertBatch)
                .build();
    }

    private Behavior<AACommand> onInsertBatch(AACommand.InsertBatch insertBatch) {
        getContext().getLog().debug("[ATTRA] Received insert batch for col {} with {} rows", colId, insertBatch.rows().length);
        RoaringBitmap newDistinct = new RoaringBitmap();

        int[] valueIds    = insertBatch.valueIds();
        long[] rows   = insertBatch.rows();

        for (int i = 0; i < rows.length; i++) {
            int vid  = valueIds[i];
            int rowI = (int) rows[i];

            RoaringBitmap rowSet = valueToRows.computeIfAbsent(vid, k -> new RoaringBitmap());
            boolean wasEmpty = rowSet.isEmpty();
            rowSet.add(rowI);
            getContext().getLog().debug("[ATTRA] Adding row {} to bitmap for value {}", rowI, vid);

            if (wasEmpty) {
                bitmapStore.insertIds(new int[]{vid});
                newDistinct.add(vid);
            } else if (!bitmapStore.containsId(vid)) {
                bitmapStore.insertIds(new int[]{vid});
                newDistinct.add(vid);
            }

        }


        if (insertBatch.ackTo() != null)
            insertBatch.ackTo().tell(new BDCommand.BatchFlushed(insertBatch.epoch(), colId));

        ActorRef<CMCommand> cm = cmRef.get();
        if (cm != null && !newDistinct.isEmpty()) {
            newDistinct.runOptimize();
            cm.tell(new CMCommand.DistinctValueDelta(colId, newDistinct, insertBatch.epoch()));
        }

        epochsProcessed++;
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
