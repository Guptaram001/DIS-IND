package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedShardedDispatcher.model.Ack;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;
import disIND.streamBasedShardedDispatcher.model.WorkType;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record BatchUpdate(
            int batchId,
            long entityHash,
            Map<Short, Integer> attrCounts,
            ActorRef<Ack> replyTo
    ) implements Command {}

    private Behavior<Command> onBatchUpdate(BatchUpdate cmd) {
        for (Map.Entry<Short, Integer> e : cmd.attrCounts().entrySet()) {
            counts.merge(e.getKey(), e.getValue(), Integer::sum);
            attrsContainingValue.set(e.getKey());
        }

        getContext().getLog().info(
                "ValueOwner {} batch={} update={} state={}",
                entityId,
                cmd.batchId(),
                cmd.attrCounts(),
                counts
        );

        cmd.replyTo().tell(new Ack(cmd.batchId(), cmd.entityHash(), WorkType.VALUE));
        candidateManager.tell(new CandidateManagerActor.ChangePropagate(cmd.entityHash(), (BitSet) attrsContainingValue.clone()));

        return this;
    }

    private final String entityId;
    private final BitSet attrsContainingValue = new BitSet();
    private final Map<Short, Integer> counts = new HashMap<>();
    private final ActorRef<CandidateManagerActor.Command> candidateManager;

    public static Behavior<Command> create(String entityId,ActorRef<CandidateManagerActor.Command> candidateManager) {
        return Behaviors.setup(ctx -> new ValueOwnerActor(ctx, entityId, candidateManager));
    }

    private ValueOwnerActor(ActorContext<Command> ctx, String entityId, ActorRef<CandidateManagerActor.Command> candidateManager) {
        super(ctx);
        this.entityId = entityId;
        this.candidateManager = candidateManager;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(BatchUpdate.class, this::onBatchUpdate)
                .build();
    }

}