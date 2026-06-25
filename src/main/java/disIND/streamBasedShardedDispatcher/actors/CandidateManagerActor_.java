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
import disIND.streamBasedShardedDispatcher.model.SharedModel;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.utility.Debug;
import disIND.streamBasedShardedDispatcher.utility.UserConfig;
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
    private final ActorRef<AppraiserCommand> apRef;
    private final int cleanThreshold;
    private final ClusterSharding sharding;

    public enum CandidateStatus {  REBUILDING,REPLAYING,TRACKED_CLEAN, TRACKED_VIOLATING, UNTRACKED }
    private int raInProgress = 0;

    private static class UnaryState {
        CandidateStatus status = CandidateStatus.UNTRACKED;
        long lastEvaluatedEpoch = -1L;
        int pendingMembershipChecks = 0;

        List<Integer> witnesses = List.of();
        int violatingCount = 0;
        RoaringBitmap violatingValues = new RoaringBitmap();

        RoaringBitmap bufferedLhsNewValues = new RoaringBitmap();
        RoaringBitmap bufferedRhsNewValues = new RoaringBitmap();
        long bufferedMaxEpoch = -1L;

        RoaringBitmap pendingLhsReplayValues = new RoaringBitmap();
        RoaringBitmap pendingRhsReplayValues = new RoaringBitmap();
        long pendingReplayMaxEpoch = -1L;

        boolean baselineSeen = false;
        boolean lhsReplaySeen = false;
        boolean rhsReplaySeen = false;
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


    public static Behavior<CMCommand> create(int lhsOwnerCol,ClusterSharding sharding,ActorRef<AppraiserCommand> apRef,
                                             ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef, ActorRef<RCCommand> rcRef,
                                             int cleanThreshold, DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new CandidateManagerActor_(ctx, lhsOwnerCol,sharding,apRef,raRef, lmRef, rcRef, cleanThreshold, metadata,statsRef));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> ctx, int lhsOwnerCol, ClusterSharding sharding,ActorRef<AppraiserCommand> apRef,
                                   ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef, ActorRef<RCCommand> rcRef,
                                   int cleanThreshold, DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.lhsOwnerCol = lhsOwnerCol;
        this.sharding = sharding;
        this.apRef=apRef;
        this.raRef = raRef;
        this.lmRef = lmRef;
        this.rcRef = rcRef;
        this.cleanThreshold = cleanThreshold;
        this.metadata = metadata;
        this.statsRef = statsRef;
    }


    @Override
    public Receive<CMCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CMCommand.UnaryCandidateProposed.class,this::onUnaryCandidateProposed)
                .onMessage(CMCommand.UnaryViolationReport.class,this::onUnaryViolationReport)
                .onMessage(CMCommand.RhsReplayDelta.class, this::onRhsReplayDelta)
                .onMessage(CMCommand.LhsReplayDelta.class, this::onLhsReplayDelta)
                .onMessage(CMCommand.MembershipResult.class, this::onMembershipResult)
                .onMessage(CMCommand.RhsLiveDelta.class,this::onRhsLiveDelta)
                .onMessage(CMCommand.LhsLiveDelta.class,this::onLhsLiveDelta)
                .build();
    }

    private Behavior<CMCommand> onLhsLiveDelta(CMCommand.LhsLiveDelta msg) {
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
            EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
            s.pendingMembershipChecks++;
            rhs.tell(new AACommand.CheckMembership(pair, msg.epoch(), msg.newValues().clone(), selfEntityRef()));
        }
        return this;
    }

    private Behavior<CMCommand> onRhsLiveDelta(CMCommand.RhsLiveDelta msg) {
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
            int before = s.violatingValues.getCardinality();
            s.violatingValues.andNot(msg.newValues());
            refreshUnaryState(s, msg.epoch());
            if (Debug.MESSAGE) {
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),
                        lhsOwnerCol, Debug.pairTag(pair), String.valueOf(s.status),
                        "Applied RHS live delta col={} epoch={} values={} violationsBefore={} violationsAfter={}",
                        msg.colId(), msg.epoch(), msg.newValues().getCardinality(), before, s.violatingValues.getCardinality());
            }
        }
        return this;
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

        if(msg.epoch()<s.pendingReplayMaxEpoch)
            return this;

        if (s.pendingMembershipChecks > 0)
            s.pendingMembershipChecks--;
        s.violatingValues.or(msg.missingValues());
        if (s.pendingMembershipChecks == 0)
            maybeFinishReplay(msg.pair(), s, msg.epoch());

        if (s.violatingValues.getCardinality() > UserConfig.MAX_TRACKED_VIOLATIONS)
            deactivatePair(msg.pair(), s);

        return this;
    }

    private Behavior<CMCommand> onLhsReplayDelta(CMCommand.LhsReplayDelta msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                    String.valueOf(Debug.State.NONE),
                    "LHS replay delta received col={} fromEpoch={} toEpoch={} values={}",
                    msg.colId(), msg.fromEpochExclusive(),msg.toEpochInclusive(), msg.newValues().getCardinality());
        UnaryPair pair = msg.pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null)
            return this;
        s.lhsReplaySeen = true;
        s.pendingReplayMaxEpoch = Math.max(s.pendingReplayMaxEpoch, msg.toEpochInclusive());
        if (!s.baselineSeen) {
            s.pendingLhsReplayValues.or(msg.newValues());
            return this;
        }
        applyLhsReplay(pair, s, msg.newValues(), msg.toEpochInclusive());
        maybeFinishReplay(pair, s, msg.toEpochInclusive());
        //EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
        //s.pendingMembershipChecks++;
        //rhs.tell(new AACommand.CheckMembership(pair, msg.toEpochInclusive(), msg.newValues().clone(), selfEntityRef()));
        return this;
    }

    private Behavior<CMCommand> onRhsReplayDelta(CMCommand.RhsReplayDelta msg) {
        UnaryPair pair = msg.pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null)
            return this;
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                    String.valueOf(Debug.State.NONE),
                    "RHS replay delta received col={} fromEpoch={} toEpoch={} values={}",
                    msg.colId(), msg.fromEpochExclusive(), msg.toEpochInclusive(),msg.newValues().getCardinality());
        s.rhsReplaySeen = true;
        s.pendingReplayMaxEpoch = Math.max(s.pendingReplayMaxEpoch, msg.toEpochInclusive());
        if (!s.baselineSeen) {
            s.pendingRhsReplayValues.or(msg.newValues());
            return this;
        }
        applyRhsReplay(s, msg.newValues());
        maybeFinishReplay(pair, s, msg.toEpochInclusive());

//        int before = s.violatingValues.getCardinality();
//        s.violatingValues.andNot(msg.newValues());
//        refreshUnaryState(s, msg.toEpochInclusive());
        return this;
    }


    private Behavior<CMCommand> onUnaryViolationReport(CMCommand.UnaryViolationReport msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                String.valueOf(Debug.State.REPLAYING), "Unary Report Received: pair: {} , epoch: {} , witnesses: {} , violatingCount: {}",
                    msg.result().pair(), msg.result().epoch(), msg.result().witnesses(), msg.result().violationCount());
        UnaryPair pair = msg.result().pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null || s.status != CandidateStatus.REBUILDING)
            return this;

        s.status=CandidateStatus.REPLAYING;
        s.baselineSeen = true;
        s.violatingValues = msg.result().violationBitmap().clone();
        long epoch = msg.result().epoch();

        if (!s.pendingRhsReplayValues.isEmpty()) {
            applyRhsReplay(s, s.pendingRhsReplayValues);
            epoch = Math.max(epoch, s.pendingReplayMaxEpoch);
            s.pendingRhsReplayValues.clear();
        }

        if (!s.pendingLhsReplayValues.isEmpty()) {
            applyLhsReplay(pair, s, s.pendingLhsReplayValues, s.pendingReplayMaxEpoch);
            epoch = Math.max(epoch, s.pendingReplayMaxEpoch);
            s.pendingLhsReplayValues.clear();
        }

        drainLiveBuffers(pair, s, epoch);
        maybeFinishReplay(pair, s, epoch);
        return this;
    }

    private void drainLiveBuffers(UnaryPair pair, UnaryState s, long baselineEpoch) {
        long mergedEpoch = Math.max(baselineEpoch, s.bufferedMaxEpoch);
        if (!s.bufferedRhsNewValues.isEmpty())
            s.violatingValues.andNot(s.bufferedRhsNewValues);
        if (!s.bufferedLhsNewValues.isEmpty())
            sendMembershipCheck(pair, s, mergedEpoch, s.bufferedLhsNewValues);

        s.bufferedLhsNewValues.clear();
        s.bufferedRhsNewValues.clear();
        s.bufferedMaxEpoch = -1L;
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
        s.witnesses = witnessesFrom(s.violatingValues, UserConfig.MAX_TRACKED_VIOLATIONS);
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

    private void applyLhsReplay(UnaryPair pair, UnaryState s, RoaringBitmap values, long epoch) {
        if (values == null || values.isEmpty())
            return;
        sendMembershipCheck(pair, s, epoch, values);
    }

    private void applyRhsReplay(UnaryState s, RoaringBitmap values) {
        if (values == null || values.isEmpty())
            return;
        s.violatingValues.andNot(values);
    }

    private void sendMembershipCheck(UnaryPair pair, UnaryState s, long epoch, RoaringBitmap values) {
        if (values == null || values.isEmpty())
            return;

        EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
        s.pendingMembershipChecks++;
        rhs.tell(new AACommand.CheckMembership(pair, epoch, values.clone(), selfEntityRef()));
    }

    private void maybeFinishReplay(UnaryPair pair, UnaryState s, long epoch) {
        if (!s.baselineSeen)
            return;
        if (!s.lhsReplaySeen || !s.rhsReplaySeen)
            return;
        if (s.pendingMembershipChecks > 0)
            return;
        refreshUnaryState(s, epoch);
    }

    private void deactivatePair(UnaryPair pair, UnaryState s) {
        s.violatingValues.clear();
        s.witnesses = List.of();
        s.violatingCount = 0;
        s.status = CandidateStatus.UNTRACKED;
        unaryPairs.remove(pair);
        apRef.tell(new AppraiserCommand.PairStateChanged(pair, PairState.INACTIVE));
        EntityRef<AACommand> lhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.lhsCol()));
        EntityRef<AACommand> rhs =sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
        lhs.tell(new AACommand.DeactiveUnaryPair(pair, true));
        rhs.tell(new AACommand.DeactiveUnaryPair(pair, false));
    }
}
