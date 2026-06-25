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
import disIND.streamBasedShardedDispatcher.utility.Debug;
import disIND.streamBasedShardedDispatcher.utility.UserConfig;

import java.util.*;

import static disIND.streamBasedShardedDispatcher.utility.ColTypeCompatibility.testCompatibility;
import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;


public class AppraisalActor_ extends AbstractBehavior<AppraiserCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final ClusterSharding sharding;
    private final ActorRef<SketchSummary> sketchAdapter;

    private final Map<Long, Map<Integer, SketchSummary>> pendingSketches = new HashMap<>();
    private final Set<Long> evaluatedEpochs = new HashSet<>();

    private HashSet<UnaryPair> activeCandidates;

    public static Behavior<AppraiserCommand> create( ClusterSharding sharding,
                                                    DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new AppraisalActor_(ctx, sharding,
                metadata,statsRef));
    }

    private AppraisalActor_(ActorContext<AppraiserCommand> ctx, ClusterSharding sharding
            , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        if(Debug.STATE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.attr(),-1,"-",
                    String.valueOf(Debug.State.NONE), "Constructor called for AppraisalActor");
        this.sharding = sharding;
        this.sketchAdapter = ctx.messageAdapter(SketchSummary.class, AppraiserCommand.SketchArrived::new);
        this.metadata = metadata;
        this.statsRef = statsRef;
        this.activeCandidates = new HashSet<>();
    }

    @Override
    public Receive<AppraiserCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(AppraiserCommand.EpochComplete.class,this::onEpochComplete)
                .onMessage(AppraiserCommand.SketchArrived.class,this::onSketchArrived)
                .onMessage(AppraiserCommand.PairStateChanged.class,this::onPairStateChanged)
                .build();
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
        long epoch = s.epoch();
        if (evaluatedEpochs.contains(epoch))
            return this;
        if (!pendingSketches.containsKey(epoch)) {
            //Unnecessary epoch sent
            return this;
        }

        Map<Integer, SketchSummary> bucket = pendingSketches.get(epoch);
        if (bucket == null) {
            return this;
        }
        bucket.putIfAbsent(s.colId(), s);

        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(), msg.summary().colId(),
                    "-", String.valueOf(Debug.State.NONE),
                    "Sketch arrived for epoch {}, kmv: {}, colid: {} , cardinaliy: {}", msg.summary().epoch(),
                    msg.summary().kmv(), msg.summary().colId(), msg.summary().distinctValues());
        if (bucket.size() == metadata.totalCols()) {
            evaluatePairs(epoch, bucket);
            pendingSketches.remove(epoch);
            evaluatedEpochs.add(epoch);
        }
        return this;
    }

    private Behavior<AppraiserCommand> onEpochComplete(AppraiserCommand.EpochComplete msg) {
        long epoch = msg.epoch();
        pendingSketches.computeIfAbsent(epoch, e -> new HashMap<>());
        for (int c = 0; c < metadata.totalCols(); c++) {
            sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(c)).tell(
                            new AACommand.EmitSketch(msg.epoch(), getContext().getSelf()));
        }
        return this;
    }

    private void evaluatePairs(long epoch, Map<Integer, SketchSummary> summaries) {
        List<Integer> cols = new ArrayList<>(summaries.keySet());
        Collections.sort(cols);

        int emitted = 0, prunedType = 0, prunedHeuristic = 0;

        for (int lhs : cols) {
            for (int rhs : cols) {
                if (lhs == rhs)
                    continue;
                SketchSummary ls = summaries.get(lhs);
                SketchSummary rs = summaries.get(rhs);

                if (ls.distinctValues() == 0)
                    continue;

                //already being tracked, so no more retracking request.
                if(activeCandidates.contains(new UnaryPair(lhs, rhs)))
                    continue;

                //Data Type Pruning
                if(!testCompatibility(metadata.colTypes().get(lhs),metadata.colTypes().get(rhs))){
                    prunedType++;
                    continue;
                }

                //Distinct Count Pruning - HLL
                if (ls.distinctValues() > rs.distinctValues()) {
                    prunedHeuristic++;
                    continue;
                }

                //KMV Pruning
                double containment = ls.kmv().containmentIn(rs.kmv());
                if (containment < UserConfig.KMV_PRUN_THRESHOLD) {
                    prunedHeuristic++;
                    continue;
                }

                emitted++;
                if(Debug.MESSAGE)
                    formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(),-1,
                            Debug.pair(lhs,rhs), String.valueOf(Debug.State.NONE),
                            " Proposing pair: {} , {} , containment: {}",lhs,rhs,containment);
                activeCandidates.add(new UnaryPair(lhs, rhs));
                EntityRef<CMCommand> cmShard = sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhs));
                cmShard.tell(new CMCommand.UnaryCandidateProposed(new UnaryCandidate(new UnaryPair(lhs, rhs), epoch)));
            }
            if(Debug.INTERNAL)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.app(),-1,
                        "-", String.valueOf(Debug.State.NONE),
                        "Pruned {} distinct values for type and distinct count heuristic,{} distinct values for containment heuristic" +
                                " Evaluated {} pairs",prunedType,prunedHeuristic,emitted);

            }
        }
}
