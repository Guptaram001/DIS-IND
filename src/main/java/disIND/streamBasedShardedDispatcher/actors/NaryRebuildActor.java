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

public class NaryRebuildActor extends AbstractBehavior<NRACommand>  {
    private final ActorRef<StatsCommand> statsRef;
    private final DatasetMetadata metadata;
    private final ClusterSharding sharding;
    private final ActorRef<CMCommand> cmRef;
    private final ActorRef<LMCommand> lmRef;
    private final int witnessK;

    public static Behavior<NRACommand> create(ClusterSharding sharding, ActorRef<CMCommand> cmRef,
                                              ActorRef<LMCommand> lmRef, int witnessK, DatasetMetadata metadata
    ,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new NaryRebuildActor(ctx, sharding, cmRef, lmRef, witnessK
        ,metadata,statsRef));
    }

    private NaryRebuildActor(ActorContext<NRACommand> ctx, ClusterSharding sharding, ActorRef<CMCommand> cmRef,
                             ActorRef<LMCommand> lmRef, int witnessK, DatasetMetadata metadata,
                             ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.sharding  = sharding;
        this.cmRef     = cmRef;
        this.lmRef     = lmRef;
        this.witnessK  = witnessK;
        this.metadata  = metadata;
        this.statsRef  = statsRef;
    }

    @Override
    public Receive<NRACommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
