package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
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
    private long valuesSinceLastStats = 0;

    private final int colId;
    private final ValueIdMap valueIdMap;
    private final WatermarkRegister wmReg;
    private final AtomicReference<ActorRef<CMCommand>> cmRefRef;


    public static Behavior<AACommand> create(int colId, ValueIdMap vidMap, WatermarkRegister wmReg,
                                             AtomicReference<ActorRef<CMCommand>> cmRefRef, ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new AttributeActor(ctx, colId, vidMap, wmReg, cmRefRef
        ,statsRef));
    }

    private AttributeActor(ActorContext<AACommand> ctx, int colId, ValueIdMap valueIdMap, WatermarkRegister wmReg,
                           AtomicReference<ActorRef<CMCommand>> cmRefRef,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.colId      = colId;
        this.valueIdMap = valueIdMap;
        this.wmReg      = wmReg;
        this.cmRefRef   = cmRefRef;
        this.statsRef   = statsRef;
    }


    @Override
    public Receive<AACommand> createReceive() {
        return null;
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
