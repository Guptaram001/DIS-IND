package disIND.streamBasedNoCentralCoordinator.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.Done;
import disIND.streamBasedNoCentralCoordinator.model.AkkaSerializable;

import java.util.HashMap;
import java.util.Map;
public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command {}

    public record BatchUpdate(
            Map<Short, Integer> attrCounts,
            ActorRef<Done> replyTo
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
                .onMessage(BatchUpdate.class, this::onBatchUpdate)
                .build();
    }

    private Behavior<Command> onBatchUpdate(BatchUpdate cmd) {
        for (Map.Entry<Short, Integer> e : cmd.attrCounts().entrySet()) {
            counts.merge(e.getKey(), e.getValue(), Integer::sum);
        }

        getContext().getLog().info(
                "ValueOwner {} update={} state={}",
                entityId,
                cmd.attrCounts(),
                counts
        );

        cmd.replyTo().tell(Done.getInstance());
        return this;
    }
}