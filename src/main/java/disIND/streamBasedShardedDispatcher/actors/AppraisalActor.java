package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.ActorRef;
import akka.cluster.sharding.typed.ShardingEnvelope;

import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AppraisalActor extends AbstractBehavior<AppraisalActor.Command> {

    public interface Command {}

    public record SketchSummary(
            short attrId,
            int distinctCount,
            Set<Long> sampleHashes
    ) implements Command {}


    private final Map<Short, SketchSummary> summaries = new HashMap<>();
    private final Set<String> activated = new HashSet<>();

    private final ActorRef<ShardingEnvelope<CandidateManagerActor.Command>> candidateRegion;

    public static Behavior<Command> create(ActorRef<ShardingEnvelope<CandidateManagerActor.Command>> candidateRegion) {
        return Behaviors.setup(ctx -> new AppraisalActor(ctx, candidateRegion));
    }

    private AppraisalActor(ActorContext<Command> ctx,ActorRef<ShardingEnvelope<CandidateManagerActor.Command>> candidateRegion) {
        super(ctx);
        this.candidateRegion = candidateRegion;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(SketchSummary.class, this::onSketchSummary)
                .build();
    }

    private Behavior<Command> onSketchSummary(SketchSummary incoming) {

        getContext().getLog().info(
                "Received sketch summary attr={} distinct={}",
                incoming.attrId(),
                incoming.distinctCount()
        );

//        for (SketchSummary existing : summaries.values()) {
//            if (existing.attrId() == incoming.attrId()) {
//                continue;
//            }
//            if (looksPromising(existing, incoming)) {
//                candidateRegion.tell(
//                        new ShardingEnvelope<>(
//                                String.valueOf(existing.attrId()),
//                                new CandidateManagerActor.ActivatePair(
//                                        existing.attrId(),
//                                        incoming.attrId()
//                                )
//                        )
//                );
//            }
//        }
//        summaries.put(incoming.attrId(), incoming);
        for (SketchSummary existing : summaries.values()) {
            if (existing.attrId() == incoming.attrId()) continue;

            if (looksPromising(existing, incoming)) {
                activate(existing.attrId(), incoming.attrId());
            }

            if (looksPromising(incoming, existing)) {
                activate(incoming.attrId(), existing.attrId());
            }
        }
        summaries.put(incoming.attrId(), incoming);

        return this;
    }

    private void activate(short lhs, short rhs) {
        String key = lhs + "->" + rhs;

        if (!activated.add(key)) {
            return;
        }
        candidateRegion.tell(
                new ShardingEnvelope<>(
                        String.valueOf(lhs),
                        new CandidateManagerActor.ActivatePair(lhs, rhs)
                )
        );

        getContext().getLog().info(
                "Appraisal activated candidate {} ⊆ {}",
                lhs,
                rhs
        );
    }

    private boolean looksPromising(SketchSummary a, SketchSummary b) {
        //dummy ratio 0.8 threshold
        int minDistinct = Math.min(a.distinctCount(), b.distinctCount());
        int maxDistinct = Math.max(a.distinctCount(), b.distinctCount());
        double ratio = (double) minDistinct / maxDistinct;
        if (ratio < 0.2) {
            return false;
        }
        Set<Long> overlap = new HashSet<>(a.sampleHashes());
        overlap.retainAll(b.sampleHashes());
        return !overlap.isEmpty();
    }
}
