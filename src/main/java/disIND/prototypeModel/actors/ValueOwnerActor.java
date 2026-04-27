package disIND.prototypeModel.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.prototypeModel.model.AkkaSerializable;
import java.util.HashMap;
import java.util.Map;
public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command extends AkkaSerializable {}

    //public record InsertCmd(short attrId, long batchId) implements Command {}
    public record InsertCmd(short attrId, ActorRef<InputReaderActor.Command> replyTo) implements Command {}
    public record DeleteCmd(
            short attrId,
            ActorRef<InputReaderActor.Command> replyTo
    ) implements Command {}

    private final String entityId;
    private final Map<Short, Integer> counts = new HashMap<>();

    public static Behavior<Command> create(String entityId) {
        return Behaviors.setup(ctx -> new ValueOwnerActor(ctx, entityId));
    }

    private ValueOwnerActor(ActorContext<Command> ctx, String entityId) {
        super(ctx);
        this.entityId = entityId;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(InsertCmd.class, this::onInsert)
                .onMessage(DeleteCmd.class, this::onDelete)
                .build();
    }

    private Behavior<Command> onInsert(InsertCmd cmd) {
        counts.merge(cmd.attrId(), 1, Integer::sum);

        getContext().getLog().info(
                "ValueOwner {} INSERT attr={} counts={}",
                entityId, cmd.attrId(), counts
        );
        cmd.replyTo().tell(new InputReaderActor.EventProcessed());

        return this;
    }

    private Behavior<Command> onDelete(DeleteCmd cmd) {
        counts.computeIfPresent(cmd.attrId(), (k, v) -> {
            int next = v - 1;
            return next <= 0 ? null : next;
        });

        getContext().getLog().info(
                "ValueOwner {} DELETE attr={} counts={}",
                entityId, cmd.attrId(), counts
        );

        cmd.replyTo().tell(new InputReaderActor.EventProcessed());

        return this;
    }
}