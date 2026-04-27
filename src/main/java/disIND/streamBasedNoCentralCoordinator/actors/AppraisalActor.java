package disIND.streamBasedNoCentralCoordinator.actors;

import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedNoCentralCoordinator.model.AkkaSerializable;

public class AppraisalActor extends AbstractBehavior<AppraisalActor.Command> {
    public AppraisalActor(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return null;
    }

    public interface Command extends AkkaSerializable {}
}
