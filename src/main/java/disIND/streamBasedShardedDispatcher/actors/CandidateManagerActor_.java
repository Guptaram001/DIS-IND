package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CandidateManagerActor_ extends AbstractBehavior<CMCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final DatasetMetadata metadata;
    private final ActorRef<RACommand>  raRef;
    private final ActorRef<LMCommand>  lmRef;
    private final ActorRef<RCCommand>  rcRef;
    private final int cleanThreshold;

    public enum CandidateStatus {  REBUILDING,TRACKED_CLEAN, TRACKED_VIOLATING, UNTRACKED }
    private static final int MAX_WITNESSES = 2;
    private int raInProgress = 0;

    private static class UnaryState {
        CandidateStatus status = CandidateStatus.UNTRACKED;
        long lastEvaluatedEpoch = -1L;
        List<String> witnesses = List.of();
        int violatingCount = 0;
        boolean reportedConfirmed = false;
    }

    private static class NaryState {
        CandidateStatus status = CandidateStatus.UNTRACKED;
        long lastEvaluatedEpoch = -1L;
        List<String> witnesses = List.of();
        int violatingCount = 0;
        boolean reportedConfirmed = false;
    }

    private final Map<UnaryPair, UnaryState> unaryPairs = new HashMap<>();
    private final Map<NaryPair,  NaryState>  naryPairs  = new HashMap<>();


    public static Behavior<CMCommand> create(ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef,
                                             ActorRef<RCCommand> rcRef,  int cleanThreshold,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new CandidateManagerActor_(ctx, raRef, lmRef, rcRef, cleanThreshold, metadata,statsRef));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> ctx, ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef,
                                   ActorRef<RCCommand> rcRef,  int cleanThreshold
                        , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.raRef          = raRef;
        this.lmRef          = lmRef;
        this.rcRef          = rcRef;
        this.cleanThreshold = cleanThreshold;
        this.metadata       = metadata;
        this.statsRef       = statsRef;
    }


    @Override
    public Receive<CMCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CMCommand.UnaryCandidateProposed.class,this::onUnaryCandidateProposed)
                .onMessage(CMCommand.UnaryViolationReport.class,this::onUnaryViolationReport)
                .build();
    }

    private Behavior<CMCommand> onUnaryViolationReport(CMCommand.UnaryViolationReport unaryViolationReport) {
        raInProgress = Math.max(0, raInProgress - 1);

      return this;

    }

    private Behavior<CMCommand> onUnaryCandidateProposed(CMCommand.UnaryCandidateProposed unaryCandidateProposed) {
        getContext().getLog().info("Unary candidate proposed: {}", unaryCandidateProposed.candidate().pair().toString());

        UnaryPair pair = unaryCandidateProposed.candidate().pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null || s.status == CandidateStatus.UNTRACKED) {
            s = new UnaryState();
            s.status = CandidateStatus.REBUILDING;
            unaryPairs.putIfAbsent(pair, s);
            raRef.tell(new RACommand.EvaluateCandidate(unaryCandidateProposed.candidate()));
            raInProgress++;
        }
        return this;
    }
}
