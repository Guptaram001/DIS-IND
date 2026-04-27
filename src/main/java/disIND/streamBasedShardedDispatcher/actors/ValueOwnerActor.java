package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;

import java.util.HashMap;
import java.util.Map;
public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record ApplyUpdate(
            long batchId,
            String entityId,
            Map<Short, Integer> attrCounts,
            ActorRef<BatchDispatcherActor.Command> ackTo
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
                .onMessage(ApplyUpdate.class, this::onApplyUpdate)
                .build();
    }

    private Behavior<Command> onApplyUpdate(ApplyUpdate cmd) {

        for (Map.Entry<Short, Integer> e : cmd.attrCounts().entrySet()) {
            counts.merge(e.getKey(), e.getValue(), Integer::sum);
        }

        getContext().getLog().info(
                "ValueOwner {} batch={} update={} state={}",
                entityId,
                cmd.batchId(),
                cmd.attrCounts(),
                counts
        );

        cmd.ackTo().tell(
                new BatchDispatcherActor.ValueAck(
                        cmd.batchId(),
                        cmd.entityId()
                )
        );

        return this;
    }
}