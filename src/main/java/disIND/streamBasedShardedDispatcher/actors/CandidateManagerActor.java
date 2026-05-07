package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;

import java.util.*;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

public class CandidateManagerActor extends AbstractBehavior<CandidateManagerActor.Command> {

    public interface Command extends AkkaSerializable {}
    public record ChangePropagate(long valueHash, BitSet attrs) implements Command {}

    public record ActivatePair(long lhsAttr, long rhsAttr) implements Command {}

    private final Set<String> activePairs = new HashSet<>();

    private final Set<String> activeCandidates = new HashSet<>();
    private final Map<String, Integer> violationCounts = new HashMap<>();
    private final Map<String, Long> witnesses = new HashMap<>();
    private static final int MAX_VIOLATIONS = 10;

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
                .onMessage(ChangePropagate.class, this::onChange)
                .build();
    }

    private Behavior<Command> onActivatePair(ActivatePair cmd) {
        String key = cmd.lhsAttr() + "->" + cmd.rhsAttr();
        if (activeCandidates.add(key)) {
            getContext().getLog().info("Activated candidate IND {}", key);
            violationCounts.put(key, 0);
        }
        return this;
    }

    private Behavior<Command> onChange(ChangePropagate cmd) {

        BitSet attrs = cmd.attrs();
        Set<String> toRemove = new HashSet<>();
        for (String candidate : activeCandidates) {
            String[] parts = candidate.split("->");
            short lhs = Short.parseShort(parts[0]);
            short rhs = Short.parseShort(parts[1]);
            boolean lhsPresent = attrs.get(lhs);
            boolean rhsPresent = attrs.get(rhs);

            if (lhsPresent && !rhsPresent) {
                int violations = violationCounts.merge(candidate, 1, Integer::sum);
                witnesses.put(candidate, cmd.valueHash());
                getContext().getLog().info("IND violation {} witness={} count={}", candidate, Long.toUnsignedString(
                                cmd.valueHash(),
                                16),
                        violations
                );

                if (violations >= MAX_VIOLATIONS) {
                    toRemove.add(candidate);
                    getContext().getLog().info("IND {} deactivated after violations", candidate);
                }
            }
        }
        activeCandidates.removeAll(toRemove);
        return this;
    }
}