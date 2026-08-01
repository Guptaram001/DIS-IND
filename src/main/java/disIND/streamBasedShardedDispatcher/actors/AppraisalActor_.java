package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.utility.Debug;
import disIND.streamBasedShardedDispatcher.utility.UserConfig;

import java.time.Duration;
import java.util.*;

import static disIND.streamBasedShardedDispatcher.utility.ColTypeCompatibility.testCompatibility;
import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;


public class AppraisalActor_ extends AbstractBehavior<AppraiserCommand> {
    private static final Duration MISSING_SKETCH_DELAY = Duration.ofSeconds(2);
    private static final int KEEP_LAST_EVALUATED_CHECKPOINTS = 3;
    private enum PairEvalState {PENDING, DONE, TYPE_PRUNED, DISTINCT_PRUNED, KMV_PRUNED, TRACKED}

    private static final class CheckpointState {
        final int round;
        final long epoch;
        final Map<Integer, Integer> maxBatchIdByTable;

        final SketchSummary[] sketches;
        final BitSet received;
        final BitSet outstanding;
        final BitSet requested;
        final PairEvalState[] pairState;

        int receivedCount = 0;
        boolean missingCheckScheduled = false;
        boolean evaluated = false;

        CheckpointState(int round, long epoch, Map<Integer, Integer> maxBatchIdByTable, int totalCols) {
            this.round = round;
            this.epoch = epoch;
            this.maxBatchIdByTable = new HashMap<>(maxBatchIdByTable);
            this.sketches = new SketchSummary[totalCols];
            this.received = new BitSet(totalCols);
            this.outstanding = new BitSet(totalCols);
            this.outstanding.set(0, totalCols);
            this.requested = new BitSet(totalCols);
            this.pairState = new PairEvalState[totalCols * totalCols];
            Arrays.fill(this.pairState, PairEvalState.PENDING);
        }

        int idx(int lhs, int rhs, int totalCols) {
            return lhs * totalCols + rhs;
        }
    }

    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final ClusterSharding sharding;
    private final ActorRef<SketchSummary> sketchAdapter;

    private boolean finishRequested = false;
    private int finalRound = -1;
    private ActorRef<RCCommand> rcRef;
    private boolean noMoreSent = false;

    private HashSet<UnaryPair> activeCandidates;

    private final TimerScheduler<AppraiserCommand> timers;
    private final Map<Integer, CheckpointState> checkpoints = new HashMap<>();
    public static Behavior<AppraiserCommand> create( ClusterSharding sharding, DatasetMetadata metadata,ActorRef<StatsCommand> statsRef
    ,ActorRef<RCCommand> rcRef) {
        return Behaviors.setup(ctx  ->
                Behaviors.withTimers(timers ->
                new AppraisalActor_(ctx, timers,sharding, metadata,statsRef,rcRef)));
    }

    private AppraisalActor_(ActorContext<AppraiserCommand> ctx, TimerScheduler<AppraiserCommand> timers,
                            ClusterSharding sharding, DatasetMetadata metadata,ActorRef<StatsCommand> statsRef,
                             ActorRef<RCCommand> rcRef) {
        super(ctx);
        this.timers = timers;
        if(Debug.STATE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.attr(),-1,"-",
                    String.valueOf(Debug.State.NONE), "Constructor called for AppraisalActor");
        this.sharding = sharding;
        this.sketchAdapter = ctx.messageAdapter(SketchSummary.class, AppraiserCommand.SketchArrived::new);
        this.metadata = metadata;
        this.statsRef = statsRef;
        this.rcRef=rcRef;
        this.activeCandidates = new HashSet<>();
    }

    @Override
    public Receive<AppraiserCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(AppraiserCommand.CheckPoint.class,this::onCheckPoint)
                .onMessage(AppraiserCommand.SketchArrived.class,this::onSketchArrived)
                .onMessage(AppraiserCommand.PairStateChanged.class,this::onPairStateChanged)
                .onMessage(AppraiserCommand.CheckMissingSketches.class, this::onCheckMissingSketches)
                .onMessage(AppraiserCommand.FinishDiscovery.class, this::onFinishDiscovery)
                .build();
    }

    private Behavior<AppraiserCommand> onFinishDiscovery(AppraiserCommand.FinishDiscovery msg) {
        finishRequested = true;
        finalRound = msg.finalRound();
        rcRef = msg.rcRef();

        maybeSendNoMoreCandidates();

        return this;
    }

    private void maybeSendNoMoreCandidates() {
        if (!finishRequested) return;
        if (noMoreSent) return;

        CheckpointState st = checkpoints.get(finalRound);
        if (st == null || !st.evaluated)
            return;

        noMoreSent = true;
        for (int lhs = 0; lhs < metadata.totalCols(); lhs++) {
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhs))
                    .tell(new CMCommand.NoMoreCandidates(finalRound));
        }

        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(),-1,"",
                    String.valueOf(Debug.State.NONE),
                    "Final checkpoint round={} evaluated. Sent NoMoreCandidates to all CM shards.",finalRound);

    }


    private Behavior<AppraiserCommand> onPairStateChanged(AppraiserCommand.PairStateChanged msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(),-1,Debug.pairTag(msg.pair()),
                    String.valueOf(Debug.State.INACTIVE), "Pair marked inactive");
        activeCandidates.remove(msg.pair());
        return this;
    }

    private Behavior<AppraiserCommand> onSketchArrived(AppraiserCommand.SketchArrived msg) {
        SketchSummary s = msg.summary();
        int round = s.round();
        CheckpointState st = checkpoints.computeIfAbsent(s.round(), r -> new CheckpointState(s.round(),
                        s.epoch(), Map.of(), metadata.totalCols()));
        int col = s.colId();
        if (col < 0 || col >= metadata.totalCols()) {
            getContext().getLog().warn("Ignoring sketch with invalid colId={} round={}", col, s.round());
            return this;
        }
        if (st.received.get(col)) {
            return this;        //duplicate sketch
        }

        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(), msg.summary().colId(),
                    "-", String.valueOf(Debug.State.NONE),
                    "Sketch arrived for round: {} epoch {},  colid: {} , cardinaliy: {}", msg.summary().round(),
                    msg.summary().epoch(), msg.summary().colId(), msg.summary().distinctValues());

        compareWithExisting(st,col,s);
        st.sketches[col] = s;
        st.received.set(col);
        st.outstanding.clear(col);
        st.receivedCount++;

        tryMarkEvaluated(st);
        return this;
    }

    private void compareWithExisting(CheckpointState st, int newCol,SketchSummary newSketch){
        for (int otherCol = st.received.nextSetBit(0); otherCol >= 0; otherCol =
                st.received.nextSetBit(otherCol + 1)) {
            SketchSummary other = st.sketches[otherCol];
            if (other == null)
                continue;
            evaluateOneDirection(st, newSketch, other);
            evaluateOneDirection(st, other, newSketch);
        }
    }

    private Behavior<AppraiserCommand> onCheckMissingSketches(AppraiserCommand.CheckMissingSketches msg) {
        CheckpointState st = checkpoints.get(msg.round());
        if (st == null|| st.evaluated) return this;
        requestMissingSketches(st);
        return this;
    }

    private Behavior<AppraiserCommand> onCheckPoint(AppraiserCommand.CheckPoint msg) {

        CheckpointState st = checkpoints.computeIfAbsent(msg.round(),
                r -> new CheckpointState(msg.round(), msg.epoch(), msg.maxBatchIdByTable(), metadata.totalCols()));
        if (!st.missingCheckScheduled) {
            st.missingCheckScheduled = true;
            timers.startSingleTimer("missing-sketches-" + msg.round(),
                    new AppraiserCommand.CheckMissingSketches(msg.round()), MISSING_SKETCH_DELAY);
        }
        cleanupOldCheckpoints();
        return this;
    }

    private void evaluateOneDirection(CheckpointState st, SketchSummary lhsS, SketchSummary rhsS) {
        int lhs = lhsS.colId();
        int rhs = rhsS.colId();

        if (lhs == rhs)
            return;


        int idx = st.idx(lhs, rhs, metadata.totalCols());

        if (st.pairState[idx] != PairEvalState.PENDING)
            return;


        UnaryPair pair = new UnaryPair(lhs, rhs);

        if (activeCandidates.contains(pair)) {
            st.pairState[idx] = PairEvalState.TRACKED;
            return;
        }

        if (lhsS.distinctValues() == 0) {
            st.pairState[idx] = PairEvalState.DISTINCT_PRUNED;
            return;
        }

        if (!testCompatibility(metadata.typeOf(lhs), metadata.typeOf(rhs))) {
            st.pairState[idx] = PairEvalState.TYPE_PRUNED;
            if(Debug.PRUNED_TYPE)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.PRUNED), Debug.app(),-1,
                        Debug.pair(lhs,rhs), String.valueOf(Debug.State.NONE),
                        " Type Pruned: pair: {}, {}, {}, round: {}, lhsType: {} , rhsType: {}",Debug.pair(lhs,rhs),
                        metadata.displayName(lhs),metadata.displayName(rhs),metadata.typeOf(lhs),metadata.typeOf(rhs));
            return;
        }

        if (lhsS.distinctValues() > rhsS.distinctValues()) {
            st.pairState[idx] = PairEvalState.DISTINCT_PRUNED;
            if(Debug.PRUNED_DISTINCT)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.PRUNED), Debug.app(),-1,
                        Debug.pair(lhs,rhs), String.valueOf(Debug.State.NONE),
                        "Distinct Pruned: pair: {}, {}, {}, round: {}, lhsCount: {}, rhsCount: {}",Debug.pair(lhs,rhs),
                        metadata.displayName(lhs),metadata.displayName(rhs),lhsS.distinctValues() , rhsS.distinctValues());
            return;
        }

        double containment = lhsS.kmv().containmentIn(rhsS.kmv());
        //System.out.println(containment);
        if (containment < UserConfig.KMV_PRUNE_THRESHOLD) {
            st.pairState[idx] = PairEvalState.KMV_PRUNED;
            if(Debug.PRUNED_KMV)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.PRUNED), Debug.app(),-1,
                        Debug.pair(lhs,rhs), String.valueOf(Debug.State.NONE),
                        "KMV Pruned: pair: {}, {}, {}, round: {}, containment: {}, lDV: {}, RDV: {}",
                        Debug.pair(lhs,rhs), metadata.displayName(lhs),metadata.displayName(rhs),st.round,containment
                ,lhsS.distinctValues(),rhsS.distinctValues());
            return;
        }

        st.pairState[idx] = PairEvalState.DONE;
        activeCandidates.add(pair);
        if (Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(), -1,
                    Debug.pair(lhs, rhs), String.valueOf(Debug.State.NONE),
                    "Proposing pair: {}, IND({}, {}), round={}, containment={}",
                    Debug.pair(lhs,rhs), metadata.displayName(lhs),metadata.displayName(rhs),st.round, containment);
        EntityRef<CMCommand> cmShard = sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhs));
        cmShard.tell(new CMCommand.UnaryCandidateProposed(new UnaryCandidate(pair, st.round)));
    }

    private void requestOutstandingSketches() {
        for (CheckpointState st : checkpoints.values()) {
            if (!st.evaluated) {
                requestMissingSketches(st);
            }
        }
    }

    private void requestMissingSketches(CheckpointState st) {
        BitSet toRequest = (BitSet) st.outstanding.clone();
        toRequest.andNot(st.requested);
        for (int col = toRequest.nextSetBit(0); col >= 0; col = toRequest.nextSetBit(col + 1)) {
            st.requested.set(col);
            if (Debug.MESSAGE)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(), col, "-",
                        String.valueOf(Debug.State.NONE), "Requesting missing sketch round={} epoch={} col={}",
                        st.round, st.epoch, col);
            sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(col)).tell(new AACommand.RequestSketch(
                            st.round, st.epoch, getContext().getSelf()));
        }
    }

    private void tryMarkEvaluated(CheckpointState st) {
        if (st.evaluated)
            return;
        if (st.receivedCount != metadata.totalCols())
            return;
        st.evaluated = true;
        timers.cancel("missing-sketches-" + st.round);
        if (Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(), -1, "-",
                    String.valueOf(Debug.State.NONE), "Checkpoint round={} fully evaluated with {} sketches",
                    st.round, st.receivedCount);
        maybeSendNoMoreCandidates();
        cleanupOldCheckpoints();
    }

    private void cleanupOldCheckpoints() {
        List<Integer> evaluatedRounds = new ArrayList<>();
        for (Map.Entry<Integer, CheckpointState> e : checkpoints.entrySet()) {
            if (e.getValue().evaluated)
                evaluatedRounds.add(e.getKey());
        }
        Collections.sort(evaluatedRounds);
        while (evaluatedRounds.size() > KEEP_LAST_EVALUATED_CHECKPOINTS) {
            Integer old = evaluatedRounds.remove(0);
            checkpoints.remove(old);
        }
    }
}
