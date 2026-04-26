package disIND.actors;

import akka.actor.AbstractActor;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import disIND.model.AkkaSerializable;

public class CandidateManagerActor extends AbstractBehavior<CandidateManagerActor.Command> {
    public CandidateManagerActor(ActorContext context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return null;
    }

    public interface Command extends AkkaSerializable {}
}
