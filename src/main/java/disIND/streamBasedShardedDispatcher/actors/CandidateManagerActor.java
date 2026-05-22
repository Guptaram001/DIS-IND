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
    public record Change(
            long valueHash,
            short changedAttr,
            boolean added
    ) implements Command {}
    public record SemanticTransition(
            long valueHash,
            BitSet before,
            BitSet after
    ) implements Command {}

    //activation received from AA
    public record ActivatePair(short lhsAttr, short rhsAttr) implements Command {}

    private final Set<String> activePairs = new HashSet<>();
    private final Set<Short> rhsCandidates = new HashSet<>();

    private final Set<String> activeCandidates = new HashSet<>();
    private final Map<Short, Long> violationCounts = new HashMap<>();
    private final short lhsAttr;
    private final Map<String, Long> witnesses = new HashMap<>();
    private static final int MAX_VIOLATIONS = 10;

    public static Behavior<Command> create(String lhsAttr) {
        return Behaviors.setup(ctx ->
        new CandidateManagerActor(ctx, Short.parseShort(lhsAttr)));
    }

    private final Map<Short, Long> witness = new HashMap<>();

    private CandidateManagerActor(ActorContext<Command> ctx,short lhsAttr) {
        super(ctx);
        this.lhsAttr = lhsAttr;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ActivatePair.class, this::onActivatePair)
                .onMessage(ChangePropagate.class, this::onChangePropagate)
                .onMessage(SemanticTransition.class, this::onSemanticTransition)
                .onMessage(Change.class, this::onChange)
                .build();
    }

    private Behavior<Command> onActivatePair(ActivatePair cmd) {
        if (cmd.lhsAttr() != lhsAttr) {
            getContext().getLog().warn(
                    "Wrong shard. shard={} pair={}⊆{}",
                    lhsAttr,
                    cmd.lhsAttr(),
                    cmd.rhsAttr()
            );
            return this;
        }

        if (rhsCandidates.add(cmd.rhsAttr())) {

            violationCounts.put(cmd.rhsAttr(), 0L);

            getContext().getLog().info(
                    "Activated IND {}⊆{}",
                    cmd.lhsAttr(),
                    cmd.rhsAttr()
            );
        }

        return this;
    }

    private Behavior<Command> onSemanticTransition(SemanticTransition cmd) {
        BitSet before = cmd.before();
        BitSet after = cmd.after();

        for (short rhs : rhsCandidates) {
            boolean beforeViolation =
                    before.get(lhsAttr) && !before.get(rhs);

            boolean afterViolation =
                    after.get(lhsAttr) && !after.get(rhs);

            if (!beforeViolation && afterViolation) {
                long count = violationCounts.merge(rhs, 1L,Long::sum);
                witness.put(rhs, cmd.valueHash());

                getContext().getLog().info(
                        "IND violation CREATED {}⊆{} value={} count={}",
                        lhsAttr,
                        rhs,
                        Long.toUnsignedString(cmd.valueHash(), 16),
                        count
                );
            }

            if (beforeViolation && !afterViolation) {
                long count = violationCounts.merge(rhs, -1L, Long::sum);
                if (count < 0) {
                    violationCounts.put(rhs, 0L);
                    count = 0;
                }

                getContext().getLog().info(
                        "IND violation RESOLVED {}⊆{} value={} count={}",
                        lhsAttr,
                        rhs,
                        Long.toUnsignedString(cmd.valueHash(), 16),
                        count
                );
            }
        }

        return this;
    }

    private Behavior<Command> onChange(Change cmd) {

        return this;
    }

    private Behavior<Command> onChangePropagate(ChangePropagate cmd) {

//        BitSet attrs = cmd.attrs();
//        Set<String> toRemove = new HashSet<>();
//        for (String candidate : activeCandidates) {
//            String[] parts = candidate.split("->");
//            short lhs = Short.parseShort(parts[0]);
//            short rhs = Short.parseShort(parts[1]);
//            boolean lhsPresent = attrs.get(lhs);
//            boolean rhsPresent = attrs.get(rhs);
//
//            if (lhsPresent && !rhsPresent) {
//                int violations = violationCounts.merge(candidate, 1, Integer::sum);
//                witnesses.put(candidate, cmd.valueHash());
//                getContext().getLog().info("IND violation {} witness={} count={}", candidate, Long.toUnsignedString(
//                                cmd.valueHash(),
//                                16),
//                        violations
//                );
//
//                if (violations >= MAX_VIOLATIONS) {
//                    toRemove.add(candidate);
//                    getContext().getLog().info("IND {} deactivated after violations", candidate);
//                }
//            }
//        }
//        activeCandidates.removeAll(toRemove);
        return this;
    }
}