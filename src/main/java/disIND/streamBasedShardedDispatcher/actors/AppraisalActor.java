package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.ActorRef;

import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AppraisalActor extends AbstractBehavior<AppraisalActor.Command> {

    public interface Command {}

    public record SketchSummary(
            long attrId,
            int distinctCount,
            Set<Long> sampleHashes
    ) implements Command {}


    private final Map<Long, SketchSummary> summaries = new HashMap<>();

    private final ActorRef<CandidateManagerActor.Command> candidateManager;

    public static Behavior<Command> create(ActorRef<CandidateManagerActor.Command> candidateManager) {
        return Behaviors.setup(ctx -> new AppraisalActor(ctx, candidateManager));
    }

    private AppraisalActor(ActorContext<Command> ctx,ActorRef<CandidateManagerActor.Command> candidateManager) {
        super(ctx);
        this.candidateManager = candidateManager;
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

        for (SketchSummary existing : summaries.values()) {
            if (existing.attrId() == incoming.attrId()) {
                continue;
            }
            if (looksPromising(existing, incoming)) {
                candidateManager.tell(new CandidateManagerActor.ActivatePair(existing.attrId(), incoming.attrId()));
            }
        }
        summaries.put(incoming.attrId(), incoming);

        return this;
    }

    private boolean looksPromising(SketchSummary a, SketchSummary b) {
        //dummy ratio 0.8 threshold
        int minDistinct = Math.min(a.distinctCount(), b.distinctCount());
        int maxDistinct = Math.max(a.distinctCount(), b.distinctCount());
        double ratio = (double) minDistinct / maxDistinct;
        if (ratio < 0.8) {
            return false;
        }
        Set<Long> overlap = new HashSet<>(a.sampleHashes());
        overlap.retainAll(b.sampleHashes());
        return !overlap.isEmpty();
    }
}
