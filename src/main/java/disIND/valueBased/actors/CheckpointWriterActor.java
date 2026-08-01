package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.valueBased.model.SharedModel.AACommand;
import disIND.valueBased.structures.NodeValueToRowsStore;
import disIND.valueBased.structures.ValueToRowsStore;

/**
 * Node-local blocking-I/O worker. Columns are consistently routed to one writer so their checkpoint requests execute in order.
 */
public final class CheckpointWriterActor extends AbstractBehavior<CheckpointWriterActor.Command> {

    public sealed interface Command permits PersistCheckpoint {}

    public record PersistCheckpoint(int colId, int round,ValueToRowsStore frozenDelta,ActorRef<AACommand> replyTo) 
    implements Command {}

    private final NodeValueToRowsStore store;

    public static Behavior<Command> create(NodeValueToRowsStore store) {
        return Behaviors.setup(context -> new CheckpointWriterActor(context, store));
    }

    private CheckpointWriterActor(ActorContext<Command> context, NodeValueToRowsStore store) {
        super(context);
        this.store = store;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PersistCheckpoint.class, this::onPersistCheckpoint)
                .build();
    }

    private Behavior<Command> onPersistCheckpoint(PersistCheckpoint command) {
        long startedAt = System.nanoTime();
        try {
            store.mergeCheckpoint(command.colId(), command.round(), command.frozenDelta());
            getContext().getLog().info("Checkpoint persisted col={} round={} ",
                    command.colId(), command.round());
            command.replyTo().tell(new AACommand.CheckpointPersisted(command.round()));
        } catch (RuntimeException exception) {
            getContext().getLog().error("Checkpoint persistence failed for col={} round={}",
                    command.colId(), command.round(), exception);
            command.replyTo().tell(new AACommand.CheckpointPersistenceFailed(
                    command.round(), exception.toString()));
        }
        return this;
    }
}
