package disIND.actors;

import akka.actor.AbstractActor;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import disIND.model.AkkaSerializable;

public class ResultCollectorActor extends AbstractBehavior<ResultCollectorActor.Command> {
    public ResultCollectorActor(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return null;
    }

    public interface Command extends AkkaSerializable {}
}
