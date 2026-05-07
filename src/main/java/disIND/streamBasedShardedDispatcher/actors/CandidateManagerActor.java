package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.util.HashSet;
import java.util.Set;

public class CandidateManagerActor extends AbstractBehavior<CandidateManagerActor.Command> {

    public interface Command {}

    public record ActivatePair(long lhsAttr, long rhsAttr) implements Command {}

    private final Set<String> activePairs = new HashSet<>();

    public static Behavior<Command> create() {
        return Behaviors.setup(CandidateManagerActor::new);
    }

    private CandidateManagerActor(ActorContext<Command> ctx) {
        super(ctx);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ActivatePair.class, this::onActivatePair)
                .build();
    }

    private Behavior<Command> onActivatePair(ActivatePair cmd) {
        String key = cmd.lhsAttr() + "->" + cmd.rhsAttr();
        if (activePairs.add(key)) {
            getContext().getLog().info(
                    "Activated candidate IND {}",
                    key
            );
        }
        return this;
    }
}