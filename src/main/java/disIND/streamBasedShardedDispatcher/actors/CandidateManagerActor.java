package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;
import org.roaringbitmap.RoaringBitmap;

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
    public record WrappedRebuildResult(RebuildActor.CandidateCheckResult result) implements Command {}

    //activation received from AA
    public record ActivatePair(short lhsAttr, short rhsAttr) implements Command {}

    private final Set<Short> activeCandidates = new HashSet<>();
    private final Map<Short, Long> violationCounts = new HashMap<>();
    private final short lhsAttr;

    private static final int MAX_VIOLATIONS = 10;
    private final Map<Short, RoaringBitmap> activeViolations = new HashMap<>();
    private final Set<Short> currentlyValid = new HashSet<>();
    private final Map<Short, RoaringBitmap> rebuildViolations = new HashMap<>();
    private final Map<Short, RoaringBitmap> liveViolations = new HashMap<>();

    public static Behavior<Command> create(String lhsAttr, ActorRef<ShardingEnvelope<RebuildActor.Command>> rebuildRegion) {
        return Behaviors.setup(ctx ->
        new CandidateManagerActor(ctx, Short.parseShort(lhsAttr),rebuildRegion));
    }

    private final Map<Short, Long> witness = new HashMap<>();
    private final ActorRef<ShardingEnvelope<RebuildActor.Command>> rebuildRegion;
    private final ClusterSharding sharding;

    private CandidateManagerActor(ActorContext<Command> ctx,short lhsAttr, ActorRef<ShardingEnvelope<RebuildActor.Command>> rebuildRegion) {
        super(ctx);
        this.lhsAttr = lhsAttr;
        this.rebuildRegion = rebuildRegion;
        this.sharding = ClusterSharding.get( ctx.getSystem() );
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ActivatePair.class, this::onActivatePair)
                .onMessage(ChangePropagate.class, this::onChangePropagate)
                .onMessage(SemanticTransition.class, this::onSemanticTransition)
                .onMessage(Change.class, this::onChange)
                .onMessage(WrappedRebuildResult.class, this::onWrappedRebuildResult)
                .build();
    }

    private Behavior<Command> onActivatePair(ActivatePair cmd) {
        short lhs = cmd.lhsAttr();
        short rhs = cmd.rhsAttr();

        if (cmd.lhsAttr() != lhsAttr) {
            getContext().getLog().warn("Wrong shard. shard={} pair={}⊆{}", lhsAttr, cmd.lhsAttr(), cmd.rhsAttr());
            return this;
        }

        if (activeCandidates.add (rhs)) {
            getContext().getLog().info("Activated IND {}⊆{}", lhs, rhs);
        }

        ActorRef<RebuildActor.CandidateCheckResult> rebuildAdapter = getContext().messageAdapter(
                        RebuildActor.CandidateCheckResult.class,
                        WrappedRebuildResult::new
                );

        rebuildRegion.tell(
                new ShardingEnvelope<>(
                        String.valueOf(lhs),
                        new RebuildActor.CheckCandidate(
                                lhs,
                                rhs,
                                rebuildAdapter
                        )
                )
        );
        return this;
    }

    private Behavior<Command> onSemanticTransition(SemanticTransition cmd) {

        BitSet before = cmd.before();
        BitSet after = cmd.after();
        int value = (int) cmd.valueHash();
        for (short rhs : activeCandidates) {
            boolean beforeViolation = before.get(lhsAttr) && !before.get(rhs);
            boolean afterViolation = after.get(lhsAttr) && !after.get(rhs);
            RoaringBitmap live = liveViolations.computeIfAbsent(rhs, k -> new RoaringBitmap());
            if (!beforeViolation && afterViolation) {
                int beforeSize = live.getCardinality();
                live.add(value);
                int afterSize = live.getCardinality();
                if (afterSize > beforeSize) {
                    getContext().getLog().info("IND violation CREATED {}⊆{} value={} liveCount={}", lhsAttr, rhs,
                            Long.toUnsignedString(cmd.valueHash(), 16), afterSize);
                }
            }

            if (beforeViolation && !afterViolation) {
                int beforeSize = live.getCardinality();
                live.remove(value);
                int afterSize = live.getCardinality();
                if (afterSize < beforeSize) {
                    getContext().getLog().info(
                            "IND violation RESOLVED {}⊆{} value={} liveCount={}", lhsAttr, rhs,
                            Long.toUnsignedString(cmd.valueHash(), 16), afterSize);
                }
            }
            updateValidity(rhs);
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

    private Behavior<Command> onWrappedRebuildResult(WrappedRebuildResult msg) {
        RebuildActor.CandidateCheckResult result = msg.result();
        rebuildViolations.put(result.rhs(), result.violations());
        int count = result.violations().getCardinality();
        int witness = result.violations().isEmpty() ? -1 : result.violations().first();

        getContext().getLog().info("Rebuild result {}⊆{} violations={} witness={}", result.lhs(), result.rhs(), count, witness);
        updateValidity(result.rhs());
        return this;
    }

    private boolean isValid(short rhs) {
        RoaringBitmap rebuild = rebuildViolations.getOrDefault(rhs, new RoaringBitmap());
        RoaringBitmap live = liveViolations.getOrDefault(rhs, new RoaringBitmap());
        return rebuild.isEmpty() && live.isEmpty();
    }

    private void updateValidity(short rhs) {
        boolean nowValid = effectiveViolations(rhs).isEmpty();
        boolean wasValid = currentlyValid.contains(rhs);
        if (nowValid && !wasValid) {
            currentlyValid.add(rhs);
            getContext().getLog().info("VALID IND {}⊆{}", lhsAttr, rhs);
        }

        if (!nowValid && wasValid) {
            currentlyValid.remove(rhs);
            getContext().getLog().info("INVALIDATED IND {}⊆{}", lhsAttr, rhs
            );
        }
    }

    private RoaringBitmap effectiveViolations(short rhs) {
        RoaringBitmap rebuild = rebuildViolations.getOrDefault(rhs, new RoaringBitmap());
        RoaringBitmap live = liveViolations.getOrDefault(rhs, new RoaringBitmap());
        RoaringBitmap merged = (RoaringBitmap) rebuild.clone();
        merged.or(live);
        return merged;
    }
}