package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.typed.Cluster;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.utility.Debug;
import disIND.streamBasedShardedDispatcher.utility.UserConfig;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.util.ArrayList;
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
    private final ActorRef<AppraiserCommand> apRef;
    private final int cleanThreshold;
    private final ClusterSharding sharding;
    private final TimerScheduler<CMCommand> timers;

    private final ActorRef<RCCommand> rcRef;
    private boolean noMoreCandidates = false;
    private boolean finishedReported = false;
    private int finalRound = -1;
    private int activeReplays = 0;
    private int activeMembershipChecks = 0;

    public enum CandidateStatus {  REBUILDING,REPLAYING,TRACKED_CLEAN, TRACKED_VIOLATING, UNTRACKED }
    private int raInProgress = 0;

    private static class UnaryState {
        CandidateStatus status = CandidateStatus.UNTRACKED;
        int lastValidatedRound = -1;
        int pendingMembershipChecks = 0;
        int replayRound = -1;

        List<Integer> witnesses = List.of();
        int violatingCount = 0;
        RoaringBitmap violatingValues = new RoaringBitmap();

        RoaringBitmap bufferedLhsNewValues = new RoaringBitmap();
        RoaringBitmap bufferedRhsNewValues = new RoaringBitmap();

        RoaringBitmap pendingLhsReplayValues = new RoaringBitmap();
        RoaringBitmap pendingRhsReplayValues = new RoaringBitmap();

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
                Behaviors.withTimers(timers ->
                        new CandidateManagerActor_(ctx, timers, lhsOwnerCol, sharding, apRef, raRef, lmRef, rcRef,
                                cleanThreshold, metadata, statsRef)));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> ctx, TimerScheduler<CMCommand> timers,
                                   int lhsOwnerCol, ClusterSharding sharding,ActorRef<AppraiserCommand> apRef,
                                   ActorRef<RACommand> raRef, ActorRef<LMCommand> lmRef, ActorRef<RCCommand> rcRef,
                                   int cleanThreshold, DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.timers = timers;
        this.lhsOwnerCol = lhsOwnerCol;
        this.sharding = sharding;
        this.apRef=apRef;
        this.raRef = raRef;
        this.lmRef = lmRef;
        this.rcRef = rcRef;
        this.cleanThreshold = cleanThreshold;
        this.metadata = metadata;
        this.statsRef = statsRef;
        getContext().getLog().info("[PLACEMENT] type=CM col={} node={}",
                lhsOwnerCol, Cluster.get(ctx.getSystem()).selfMember().address());
        if(Debug.STATE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.STATE), Debug.attr(),lhsOwnerCol,"-",
         String.valueOf(Debug.State.NONE),"PLACEMENT type=AA col={} node={}",
                    lhsOwnerCol, Cluster.get(ctx.getSystem()).selfMember().address());
    }


    @Override
    public Receive<CMCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CMCommand.UnaryCandidateProposed.class,this::onUnaryCandidateProposed)
                .onMessage(CMCommand.UnaryViolationReport.class,this::onUnaryViolationReport)
                .onMessage(CMCommand.RhsReplayDelta.class, this::onRhsReplayDelta)
                .onMessage(CMCommand.LhsReplayDelta.class, this::onLhsReplayDelta)
                .onMessage(CMCommand.MembershipResult.class, this::onMembershipResult)
                .onMessage(CMCommand.NoMoreCandidates.class, this::onNoMoreCandidates)
                .onMessage(CMCommand.ForceFinish.class, this::onForceFinish)
                .onMessage(CMCommand.RhsLiveDelta.class,this::onRhsLiveDelta)
                .onMessage(CMCommand.LhsLiveDelta.class,this::onLhsLiveDelta)
                .build();
    }

    private Behavior<CMCommand> onNoMoreCandidates(CMCommand.NoMoreCandidates msg) {
        noMoreCandidates = true;
        finalRound = msg.finalRound();
        maybeReportFinished();
        if (!finishedReported) {
            timers.startSingleTimer("force-finish-" + finalRound,
                    new CMCommand.ForceFinish(finalRound),
                    Duration.ofSeconds(UserConfig.FINAL_CM_DRAIN_TIMEOUT_SECONDS));
        }
        return this;
    }

    private Behavior<CMCommand> onForceFinish(CMCommand.ForceFinish msg) {
        if (finishedReported || !noMoreCandidates || msg.finalRound() != finalRound)
            return this;

        List<UnaryPair> unfinished = unfinishedPairs();
        if (!unfinished.isEmpty() && Debug.STATE) {
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.STATE), Debug.cm(),
                    lhsOwnerCol, "", String.valueOf(Debug.State.NONE),
                    "Force finishing lhsOwnerCol={} finalRound={} unfinishedPairs={} rebuilding={} replaying={} pendingMembership={}",
                    lhsOwnerCol, finalRound, unfinished, rebuildingCount(), replayingCount(), pendingMembershipCheckCount());
        }

        maybeReportFinished();
        if (!finishedReported) {
            timers.startSingleTimer("force-finish-" + finalRound,
                    new CMCommand.ForceFinish(finalRound),
                    Duration.ofSeconds(UserConfig.FINAL_CM_DRAIN_TIMEOUT_SECONDS));
        }
        return this;
    }

    private void maybeReportFinished() {
        if (!noMoreCandidates)
            return;
        if (finishedReported)
            return;
        if (rebuildingCount() > 0)
            return;
        if (replayingCount() > 0)
            return;
        if (pendingMembershipCheckCount() > 0)
            return;
        finishedReported = true;

        List<UnaryPair> unary = collectUnaryResults();
        List<NaryPair> nary = List.of();

        rcRef.tell(new RCCommand.CmDiscoveryComplete(lhsOwnerCol, finalRound, unary, nary));

        if (Debug.STATE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.STATE), Debug.cm(),
                    lhsOwnerCol, "", String.valueOf(Debug.State.NONE),
                    "Finished lhsOwnerCol={} finalRound={} ", lhsOwnerCol, finalRound);

    }

    private int rebuildingCount() {
        int count = 0;
        for (UnaryState s : unaryPairs.values()) {
            if (s.status == CandidateStatus.REBUILDING)
                count++;
        }
        return count;
    }

    private int replayingCount() {
        int count = 0;
        for (UnaryState s : unaryPairs.values()) {
            if (s.status == CandidateStatus.REPLAYING)
                count++;
        }
        return count;
    }

    private int pendingMembershipCheckCount() {
        int count = 0;
        for (UnaryState s : unaryPairs.values())
            count += Math.max(0, s.pendingMembershipChecks);
        return count;
    }

    private List<UnaryPair> unfinishedPairs() {
        List<UnaryPair> out = new ArrayList<>();
        for (Map.Entry<UnaryPair, UnaryState> e : unaryPairs.entrySet()) {
            UnaryState s = e.getValue();
            if (s.status == CandidateStatus.REBUILDING || s.status == CandidateStatus.REPLAYING ||
                    s.pendingMembershipChecks > 0) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    private List<UnaryPair> collectUnaryResults() {
        List<UnaryPair> result = new ArrayList<>();
        for (Map.Entry<UnaryPair, UnaryState> e : unaryPairs.entrySet()) {
            UnaryState s = e.getValue();
            if (s.status != CandidateStatus.TRACKED_CLEAN)
                continue;
            if (!s.violatingValues.isEmpty())
                continue;
            result.add(e.getKey());
        }
        return result;
    }

    private Behavior<CMCommand> onLhsLiveDelta(CMCommand.LhsLiveDelta msg) {
        if (Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),
                    lhsOwnerCol, "", String.valueOf(Debug.State.NONE),
                    "Applied LHS live delta col={} round={} values={} ",
                    msg.colId(), msg.round(), msg.newValues().getCardinality());
        for (Map.Entry<UnaryPair, UnaryState> e : unaryPairs.entrySet()) {
            UnaryPair pair = e.getKey();
            UnaryState s = e.getValue();
            if (pair.lhsCol() != msg.colId())
                continue;
            if (s.status == CandidateStatus.REBUILDING || s.status == CandidateStatus.REPLAYING) {
                s.bufferedLhsNewValues.or(msg.newValues());
                continue;
            }
            if (!isTracked(s))
                continue;
            EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
            s.pendingMembershipChecks++;
            rhs.tell(new AACommand.CheckMembership(pair, msg.round(), msg.newValues(), lhsOwnerCol));
            activeMembershipChecks++;
        }
        return this;
    }

    private Behavior<CMCommand> onRhsLiveDelta(CMCommand.RhsLiveDelta msg) {
        for (Map.Entry<UnaryPair, UnaryState> e : unaryPairs.entrySet()) {
            UnaryPair pair = e.getKey();
            UnaryState s = e.getValue();
            if (pair.rhsCol() != msg.colId())
                continue;
            if (s.status == CandidateStatus.REBUILDING || s.status == CandidateStatus.REPLAYING) {
                s.bufferedRhsNewValues.or(msg.newValues());
                continue;
            }
            if (!isTracked(s))
                continue;
            int before = s.violatingValues.getCardinality();
            s.violatingValues.andNot(msg.newValues());
            refreshUnaryState(s, msg.round());
            if (Debug.MESSAGE) {
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),
                        lhsOwnerCol, Debug.pairTag(pair), String.valueOf(s.status),
                        "Applied RHS live delta col={} round={} values={} violationsBefore={} violationsAfter={}",
                        msg.colId(), msg.round(), msg.newValues().getCardinality(), before, s.violatingValues.getCardinality());
            }
        }
        return this;
    }

    private Behavior<CMCommand> onMembershipResult(CMCommand.MembershipResult msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,Debug.pairTag(msg.pair()),
                    String.valueOf(Debug.State.NONE), "Membership Result received: pair: {}, round: {}, missingValues: {}",
                    msg.pair(), msg.round(),msg.missingValues().getCardinality());

        UnaryState s = unaryPairs.get(msg.pair());
        activeMembershipChecks--;
        if (s == null)
            return this;
        if (!isTracked(s))
            return this;

        boolean replayResult = s.status == CandidateStatus.REPLAYING;
        if(replayResult && msg.round()!=s.replayRound)
            return this;

        if (s.pendingMembershipChecks > 0)
            s.pendingMembershipChecks--;

        s.violatingValues.or(msg.missingValues());
        if (replayResult && s.pendingMembershipChecks == 0)
            maybeFinishReplay(msg.pair(), s, msg.round());
        else if (!replayResult)
            refreshUnaryState(s, msg.round());

        maybeReportFinished();
        return this;
    }

    private Behavior<CMCommand> onLhsReplayDelta(CMCommand.LhsReplayDelta msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                    String.valueOf(Debug.State.NONE),
                    "LHS replay delta received col={} fromRound={} toRound={} values={}",
                    msg.colId(), msg.fromRound(),msg.toRound(), msg.newValues().getCardinality());
        UnaryPair pair = msg.pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null)
            return this;
        s.lhsReplaySeen = true;
        if (!s.baselineSeen) {
            s.pendingLhsReplayValues.or(msg.newValues());
            return this;
        }
        applyLhsReplay(pair, s, msg.newValues(), msg.toRound());
        maybeFinishReplay(pair, s, msg.toRound());
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
                    "RHS replay delta received col={} fromRound={} toRound={} values={}",
                    msg.colId(), msg.fromRound(), msg.toRound(),msg.newValues().getCardinality());
        s.rhsReplaySeen = true;
        if (!s.baselineSeen) {
            s.pendingRhsReplayValues.or(msg.newValues());
            return this;
        }
        applyRhsReplay(s, msg.newValues());
        maybeFinishReplay(pair, s, msg.toRound());
        return this;
    }


    private Behavior<CMCommand> onUnaryViolationReport(CMCommand.UnaryViolationReport msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,"-",
                String.valueOf(Debug.State.REPLAYING), "Unary Report Received: pair: {} , round: {} , witnesses: {} , violatingCount: {}",
                    msg.result().pair(), msg.result().round(), msg.result().witnesses().size(), msg.result().violationCount());
        UnaryPair pair = msg.result().pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null || s.status != CandidateStatus.REBUILDING)
            return this;

        raInProgress--;
        s.status=CandidateStatus.REPLAYING;
        activeReplays++;
        s.baselineSeen = true;
        s.violatingValues = msg.result().violationBitmap().clone();
        int round = msg.result().round();

        if (!s.pendingRhsReplayValues.isEmpty()) {
            applyRhsReplay(s, s.pendingRhsReplayValues);
            s.pendingRhsReplayValues.clear();
        }

        if (!s.pendingLhsReplayValues.isEmpty()) {
            applyLhsReplay(pair, s, s.pendingLhsReplayValues, s.replayRound);
            round = Math.max(round, s.replayRound);
            s.pendingLhsReplayValues.clear();
        }

        drainLiveBuffers(pair, s, msg.result().round());
        maybeFinishReplay(pair, s, msg.result().round());
        maybeReportFinished();
        return this;
    }

    private void drainLiveBuffers(UnaryPair pair, UnaryState s, int round) {
        if (!s.bufferedRhsNewValues.isEmpty())
            s.violatingValues.andNot(s.bufferedRhsNewValues);
        if (!s.bufferedLhsNewValues.isEmpty())
            sendMembershipCheck(pair, s, s.replayRound, s.bufferedLhsNewValues);

        s.bufferedLhsNewValues.clear();
        s.bufferedRhsNewValues.clear();
    }

    private Behavior<CMCommand> onUnaryCandidateProposed(CMCommand.UnaryCandidateProposed msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,Debug.pairTag(msg.candidate().pair()),
                    String.valueOf(Debug.State.REBUILDING), " Unary candidate proposed: {}, round: {}",
                    msg.candidate().pair().toString(), msg.candidate().checkpointRound());
        UnaryPair pair = msg.candidate().pair();
        UnaryState s = unaryPairs.get(pair);
        if (s == null || s.status == CandidateStatus.UNTRACKED) {
            s = new UnaryState();
            s.status = CandidateStatus.REBUILDING;
            unaryPairs.putIfAbsent(pair, s);
            s.replayRound = msg.candidate().checkpointRound();
            s.lastValidatedRound = msg.candidate().checkpointRound();
            s.baselineSeen = false;
            s.lhsReplaySeen = false;
            s.rhsReplaySeen = false;
            //raRef.tell(new RACommand.EvaluateCandidate(unaryCandidateProposed.candidate()));
            EntityRef<AACommand> lhs = sharding.entityRefFor(AttributeActor.TYPE_KEY,
                    AACommand.entityId(msg.candidate().pair().lhsCol()));
            lhs.tell(new AACommand.SendColumnData(msg.candidate()));
            raInProgress++;
        }
        return this;
    }

    private EntityRef<CMCommand> selfEntityRef() {
        return sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsOwnerCol));
    }

    private void refreshUnaryState(UnaryState s, int round) {
        s.violatingCount = s.violatingValues.getCardinality();
        s.witnesses = witnessesFrom(s.violatingValues, UserConfig.MAX_TRACKED_VIOLATIONS);
        s.lastValidatedRound = Math.max(s.lastValidatedRound, round);
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

    private void applyLhsReplay(UnaryPair pair, UnaryState s, RoaringBitmap values, int  replayRound) {
        if (values == null || values.isEmpty())
            return;
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.cm(),lhsOwnerCol,Debug.pairTag(pair),
                    String.valueOf(s.status),"Applying LHS Replay -> Membership Check: pair: {}, round {} value {}",
                    Debug.pairTag(pair),replayRound,values.getCardinality());
        sendMembershipCheck(pair, s, replayRound, values);
    }

    private void applyRhsReplay(UnaryState s, RoaringBitmap values) {
        if (values == null || values.isEmpty())
            return;
        s.violatingValues.andNot(values);
    }

    private void sendMembershipCheck(UnaryPair pair, UnaryState s, int round, RoaringBitmap values) {
        if (values == null || values.isEmpty())
            return;

        EntityRef<AACommand> rhs = sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(pair.rhsCol()));
        s.pendingMembershipChecks++;
        rhs.tell(new AACommand.CheckMembership(pair, round, values.clone(), lhsOwnerCol));
        activeMembershipChecks++;
    }

    private void maybeFinishReplay(UnaryPair pair, UnaryState s, int  round) {
        if (!s.baselineSeen)
            return;
        if (!s.lhsReplaySeen || !s.rhsReplaySeen)
            return;
        if (s.pendingMembershipChecks > 0)
            return;
        if (!s.bufferedRhsNewValues.isEmpty()) {
            s.violatingValues.andNot(s.bufferedRhsNewValues);
            s.bufferedRhsNewValues.clear();
        }
        if (!s.bufferedLhsNewValues.isEmpty()) {
            sendMembershipCheck(pair, s, s.replayRound, s.bufferedLhsNewValues);
            s.bufferedLhsNewValues.clear();
            return;
        }
        refreshUnaryState(s, round);
        activeReplays = Math.max(0, activeReplays - 1);
        s.baselineSeen = false;
        s.lhsReplaySeen = false;
        s.rhsReplaySeen = false;

        s.pendingLhsReplayValues.clear();
        s.pendingRhsReplayValues.clear();
        s.replayRound = -1;
        maybeReportFinished();
    }

    private void deactivatePair(UnaryPair pair, UnaryState s) {
        if (s.status == CandidateStatus.REPLAYING && activeReplays > 0)
            activeReplays--;
        if (s.status == CandidateStatus.REBUILDING && raInProgress > 0)
            raInProgress--;
        if (s.pendingMembershipChecks > 0) {
            activeMembershipChecks = Math.max(0, activeMembershipChecks - s.pendingMembershipChecks);
            s.pendingMembershipChecks = 0;
        }
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
        maybeReportFinished();
    }
}
