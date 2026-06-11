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


public class AppraisalActor_ extends AbstractBehavior<AppraiserCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final int                     numCols;
    private final ClusterSharding         sharding;
    private final ActorRef<CMCommand>     cmRef;
    private final ActorRef<SketchSummary> sketchAdapter;


    public static Behavior<AppraiserCommand> create(int numCols, ClusterSharding sharding, ActorRef<CMCommand> cmRef,
                                                    DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new AppraisalActor_(ctx, numCols, sharding, cmRef,
                metadata,statsRef));
    }

    private AppraisalActor_(ActorContext<AppraiserCommand> ctx, int numCols, ClusterSharding sharding, ActorRef<CMCommand> cmRef
            , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.numCols  = numCols;
        this.sharding = sharding;
        this.cmRef    = cmRef;
        this.sketchAdapter = ctx.messageAdapter(SketchSummary.class, AppraiserCommand.SketchArrived::new);
        this.metadata = metadata;
        this.statsRef = statsRef;
    }

    @Override
    public Receive<AppraiserCommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
