package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.utility.Debug;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;

public class CandidateManagerActor_ extends AbstractBehavior<CMCommand> {
    public static final EntityTypeKey<CMCommand> TYPE_KEY = EntityTypeKey.create(CMCommand.class, "CandidateManagerActor");
    private final int lhsOwnerCol;

    private final ActorRef<StatsCommand> statsRef;
    private final DatasetMetadata metadata;
    private final ActorRef<RACommand>  raRef;
    private final ActorRef<LMCommand>  lmRef;
    private final ActorRef<RCCommand>  rcRef;
    private final int cleanThreshold;
    private final ClusterSharding sharding;

    public enum CandidateStatus {  REBUILDING,REPLAYING,TRACKED_CLEAN, TRACKED_VIOLATING, UNTRACKED }
    private static final int MAX_TRACKED_VIOLATIONS = 2;
    private int raInProgress = 0;

    private static class UnaryState {
        CandidateStatus status = CandidateStatus.UNTRACKED;
        long lastEvaluatedEpoch = -1L;
        List<Integer> witnesses = List.of();
        int violatingCount = 0;
        RoaringBitmap violatingValues = new RoaringBitmap();
        boolean reportedConfirmed = false;
        RoaringBitmap bufferedLhsNewValues = new RoaringBitmap();
        RoaringBitmap bufferedRhsNewValues = new RoaringBitmap();
        long bufferedMaxEpoch = -1L;
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


    public static Behavior<CMCommand> create(int lhsOwnerCol,ClusterSharding sharding,ActorRef<RACommand> raRef,
                                             ActorRef<LMCommand> lmRef, ActorRef<RCCommand> rcRef,  int cleanThreshold,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new CandidateManagerActor_(ctx, lhsOwnerCol,sharding,raRef, lmRef, rcRef, cleanThreshold, metadata,statsRef));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> ctx, int lhsOwnerCol, ClusterSharding sharding,ActorRef<RACommand> raRef,
                                   ActorRef<LMCommand> lmRef, ActorRef<RCCommand> rcRef,  int cleanThreshold
                        , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.lhsOwnerCol    = lhsOwnerCol;
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
                .onMessage(CMCommand.RhsColumnDelta.class, this::onRhsColumnDelta)
                .onMessage(CMCommand.LhsColumnDelta.class, this::onLhsColumnDelta)
                .onMessage(CMCommand.MembershipResult.class, this::onMembershipResult)
                .build();
    }

    private Behavior<CMCommand> onMembershipResult(CMCommand.MembershipResult msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,Debug.pairTag(msg.pair()),
                    String.valueOf(Debug.State.NONE), "Membership Result received: pair: {}, epoch: {}, missingValues: {}",
                    msg.pair(), msg.epoch(),msg.missingValues());
        UnaryState s = unaryPairs.get(msg.pair());
        if (s == null)
            return this;
        if (!isTracked(s))
            return this;

        s.violatingValues.or(msg.missingValues());
        refreshUnaryState(s, msg.epoch());

        if (s.violatingValues.getCardinality() > MAX_TRACKED_VIOLATIONS) {
            s.violatingValues.clear();
            s.witnesses = List.of();
            s.violatingCount = 0;
            s.status = CandidateStatus.UNTRACKED;
            //notify AA for desubscription
        }
        return this;
    }

    private Behavior<CMCommand> onLhsColumnDelta(CMCommand.LhsColumnDelta msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                    String.valueOf(Debug.State.NONE), "LhsColumnDelta received: colId: {}, epoch: {}, newValues: {}",
                    msg.colId(), msg.epoch(), msg.newValues());
        for (Map.Entry<UnaryPair, UnaryState> e : unaryPairs.entrySet()) {
            UnaryPair pair = e.getKey();
            UnaryState s = e.getValue();
            if (pair.lhsCol() != msg.colId())
                continue;
            if (s.status == CandidateStatus.REBUILDING) {
                s.bufferedLhsNewValues.or(msg.newValues());
                s.bufferedMaxEpoch = Math.max(s.bufferedMaxEpoch, msg.epoch());
                continue;
            }
            if (!isTracked(s))
                continue;
            EntityRef<CMCommand> cmSelf = selfEntityRef();
            EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
            rhs.tell(new AACommand.CheckMembership(pair, msg.epoch(), msg.newValues().clone(), cmSelf));
        }
        return this;
    }

    private Behavior<CMCommand> onRhsColumnDelta(CMCommand.RhsColumnDelta msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                    String.valueOf(Debug.State.NONE), "RhsColumnDelta received: colId: {}, epoch: {}, newValues: {}",
                    msg.colId(), msg.epoch(), msg.newValues());
        for (Map.Entry<UnaryPair, UnaryState> e : unaryPairs.entrySet()) {
            UnaryPair pair = e.getKey();
            UnaryState s = e.getValue();
            if (pair.rhsCol() != msg.colId())
                continue;
            if (s.status == CandidateStatus.REBUILDING) {
                s.bufferedRhsNewValues.or(msg.newValues());
                s.bufferedMaxEpoch = Math.max(s.bufferedMaxEpoch, msg.epoch());
                continue;
            }
            if (!isTracked(s))
                continue;
            s.violatingValues.andNot(msg.newValues());
            refreshUnaryState(s, msg.epoch());
        }

        return this;
    }


    private Behavior<CMCommand> onUnaryViolationReport(CMCommand.UnaryViolationReport msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                String.valueOf(Debug.State.REPLAYING), "Unary Report Received: pair: {} , epoch: {} , witnesses: {} , violatingCount: {}",
                    msg.result().pair(), msg.result().epoch(), msg.result().witnesses(), msg.result().violationCount());
        UnaryPair pair = msg.result().pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null || s.status != CandidateStatus.REBUILDING) {
            return this;
        }
        s.status=CandidateStatus.REPLAYING;
        s.violatingValues = msg.result().violationBitmap().clone();
        drainEstablishmentBuffers(pair, s, msg.result().epoch());
        return this;
    }

    private void drainEstablishmentBuffers(UnaryPair pair, UnaryState s, long baselineEpoch) {
        long mergedEpoch = Math.max(baselineEpoch, s.bufferedMaxEpoch);

        if (!s.bufferedRhsNewValues.isEmpty()) {
            s.violatingValues.andNot(s.bufferedRhsNewValues);
        }
        if (!s.bufferedLhsNewValues.isEmpty()) {
            EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
            EntityRef<CMCommand> cmSelf = selfEntityRef();
            rhs.tell(new AACommand.CheckMembership(pair, mergedEpoch, s.bufferedLhsNewValues.clone(), cmSelf));
        }
        s.bufferedLhsNewValues.clear();
        s.bufferedRhsNewValues.clear();
        s.bufferedMaxEpoch = -1L;

        refreshUnaryState(s, mergedEpoch);
    }

    private Behavior<CMCommand> onUnaryCandidateProposed(CMCommand.UnaryCandidateProposed msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,Debug.pairTag(msg.candidate().pair()),
                    String.valueOf(Debug.State.REBUILDING), " Unary candidate proposed: {}",  msg.candidate().pair().toString());
        UnaryPair pair = msg.candidate().pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null || s.status == CandidateStatus.UNTRACKED) {
            s = new UnaryState();
            s.status = CandidateStatus.REBUILDING;
            unaryPairs.putIfAbsent(pair, s);
            //raRef.tell(new RACommand.EvaluateCandidate(unaryCandidateProposed.candidate()));
            EntityRef<AACommand> lhs = sharding.entityRefFor(AttributeActor.TYPE_KEY,
                    AACommand.entityId(msg.candidate().pair().lhsCol()));
            EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY,
                    AACommand.entityId(msg.candidate().pair().rhsCol()));
            EntityRef<CMCommand> cmSelf = selfEntityRef();
            lhs.tell(new AACommand.SendColumnData(msg.candidate(),rhs,cmSelf));
            raInProgress++;
        }
        return this;
    }

    private EntityRef<CMCommand> selfEntityRef() {
        return sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsOwnerCol));
    }

    private void refreshUnaryState(UnaryState s, long epoch) {
        s.violatingCount = s.violatingValues.getCardinality();
        s.witnesses = witnessesFrom(s.violatingValues, MAX_TRACKED_VIOLATIONS);
        s.lastEvaluatedEpoch = Math.max(s.lastEvaluatedEpoch, epoch);
        if (s.status != CandidateStatus.REBUILDING && s.status != CandidateStatus.UNTRACKED) {
            s.status = s.violatingCount == 0 ? CandidateStatus.TRACKED_CLEAN : CandidateStatus.TRACKED_VIOLATING;
        }
    }

    private static List<Integer> witnessesFrom(RoaringBitmap bitmap, int max) {
        List<Integer> out = new java.util.ArrayList<>(max);
        org.roaringbitmap.IntIterator it = bitmap.getIntIterator();

        while (it.hasNext() && out.size() < max)
            out.add(it.next());
        return out;
    }

    private boolean isTracked(UnaryState s) {
        return s.status == CandidateStatus.TRACKED_CLEAN || s.status == CandidateStatus.TRACKED_VIOLATING ||
                s.status == CandidateStatus.REPLAYING;
    }
}
