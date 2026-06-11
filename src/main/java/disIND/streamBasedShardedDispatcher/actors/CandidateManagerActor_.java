package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.structures.*;

import java.util.HashMap;
import java.util.Map;

public class CandidateManagerActor_ extends AbstractBehavior<CMCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final DatasetMetadata metadata;
    private final ActorRef<RACommand>  raRef;
    private final ActorRef<LMCommand>  lmRef;
    private final ActorRef<RCCommand>  rcRef;
    private final WatermarkRegister    wmReg;
    private final int                  cleanThreshold;

    public static Behavior<CMCommand> create(ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef,
                                             ActorRef<RCCommand> rcRef, WatermarkRegister wmReg, int cleanThreshold
            , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new CandidateManagerActor_(ctx, raRef, lmRef, rcRef, wmReg, cleanThreshold, metadata,statsRef));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> ctx, ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef,
                                   ActorRef<RCCommand> rcRef, WatermarkRegister wmReg, int cleanThreshold
                        , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.raRef          = raRef;
        this.lmRef          = lmRef;
        this.rcRef          = rcRef;
        this.wmReg          = wmReg;
        this.cleanThreshold = cleanThreshold;
        this.metadata       = metadata;
        this.statsRef       = statsRef;
    }


    @Override
    public Receive<CMCommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
