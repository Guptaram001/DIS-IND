package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
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
    private final ClusterSharding sharding;

    public enum CandidateStatus {  REBUILDING,TRACKED_CLEAN, TRACKED_VIOLATING, UNTRACKED }
    private static final int MAX_WITNESSES = 2;
    private int raInProgress = 0;

    private static class UnaryState {
        CandidateStatus status = CandidateStatus.UNTRACKED;
        long lastEvaluatedEpoch = -1L;
        List<Integer> witnesses = List.of();
        int violatingCount = 0;
        boolean reportedConfirmed = false;
    }

    private static class NaryState {
        CandidateStatus status = CandidateStatus.UNTRACKED;
        long lastEvaluatedEpoch = -1L;
        List<Integer> witnesses = List.of();
        int violatingCount = 0;
        boolean reportedConfirmed = false;
    }

    private final Map<UnaryPair, UnaryState> unaryPairs = new HashMap<>();
    private final Map<NaryPair,  NaryState>  naryPairs  = new HashMap<>();


    public static Behavior<CMCommand> create(ClusterSharding sharding,ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef,
                                             ActorRef<RCCommand> rcRef,  int cleanThreshold,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new CandidateManagerActor_(ctx, sharding,raRef, lmRef, rcRef, cleanThreshold, metadata,statsRef));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> ctx, ClusterSharding sharding,ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef,
                                   ActorRef<RCCommand> rcRef,  int cleanThreshold
                        , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.sharding       = sharding;
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
        getContext().getLog().info("[CM] Unary Report Received: pair: {} , epoch: {} , witnesses: {} , violatingCount: {}",
                unaryViolationReport.result().pair(),unaryViolationReport.result().epoch(),unaryViolationReport.result().witnesses(),
                unaryViolationReport.result().violationCount());
        UnaryPair pair = unaryViolationReport.result().pair();
        UnaryState s = unaryPairs.get(pair);
        if (s.status == CandidateStatus.REBUILDING){
            s.status=CandidateStatus.TRACKED_VIOLATING;
            s.violatingCount=unaryViolationReport.result().violationCount();
            s.witnesses=unaryViolationReport.result().witnesses();

        }
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
            //raRef.tell(new RACommand.EvaluateCandidate(unaryCandidateProposed.candidate()));
            EntityRef<AACommand> lhs = sharding.entityRefFor(AttributeActor.TYPE_KEY,
                    AACommand.entityId(unaryCandidateProposed.candidate().pair().lhsCol()));
            EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY,
                    AACommand.entityId(unaryCandidateProposed.candidate().pair().rhsCol()));
            lhs.tell(new AACommand.SendColumnData(unaryCandidateProposed.candidate(),rhs,getContext().getSelf()));
            raInProgress++;
        }
        return this;
    }
}
