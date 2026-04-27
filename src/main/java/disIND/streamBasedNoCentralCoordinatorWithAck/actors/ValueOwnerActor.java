package disIND.streamBasedNoCentralCoordinatorWithAck.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedNoCentralCoordinatorWithAck.model.AkkaSerializable;


import java.util.HashMap;
import java.util.Map;
public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record BatchUpdate(
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
                .onMessage(BatchUpdate.class, this::onBatchUpdate)
                .build();
    }

    private Behavior<Command> onBatchUpdate(BatchUpdate cmd) {
        for (var e : cmd.attrCounts().entrySet()) {
            counts.merge(e.getKey(), e.getValue(), Integer::sum);
        }

        getContext().getLog().info(
                "ValueOwner {} batch={} update={} state={}",
                entityId, cmd.batchId(), cmd.attrCounts(), counts
        );

        cmd.ackTo().tell(
                new BatchDispatcherActor.EntityAck(cmd.batchId(), cmd.entityId())
        );

        return this;
    }
}