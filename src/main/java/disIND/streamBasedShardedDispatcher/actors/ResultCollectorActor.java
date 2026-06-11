package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.AbstractActor;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;

import java.util.Map;

public class ResultCollectorActor extends AbstractBehavior<RCCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final Map<Integer, String> colNames;
    private final DatasetMetadata metadata;
    public static Behavior<RCCommand> create(Map<Integer, String> colNames, DatasetMetadata metadata,
                                             ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new ResultCollectorActor(ctx, colNames,metadata,statsRef));
    }

    private ResultCollectorActor(ActorContext<RCCommand> ctx, Map<Integer, String> colNames, DatasetMetadata metadata,
                                 ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.colNames = colNames;
        this.metadata = metadata;
        this.statsRef = statsRef;
    }

    @Override
    public Receive createReceive() {
        return newReceiveBuilder().build();
    }
}
