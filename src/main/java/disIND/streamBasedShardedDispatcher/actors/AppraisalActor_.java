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

import java.util.*;

import static disIND.streamBasedShardedDispatcher.utility.ColTypeCompatibility.testCompatibility;
import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;


public class AppraisalActor_ extends AbstractBehavior<AppraiserCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final ClusterSharding sharding;
    private final ActorRef<SketchSummary> sketchAdapter;

    private final HashMap<Integer,SketchSummary> sketchSummaries= new HashMap<>();
    private long currentEpoch = 0L;
    private long lastEvaluatedEpoch = -1L;

    private HashSet<UnaryPair> activeCandidates;

    public static Behavior<AppraiserCommand> create( ClusterSharding sharding,
                                                    DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new AppraisalActor_(ctx, sharding,
                metadata,statsRef));
    }

    private AppraisalActor_(ActorContext<AppraiserCommand> ctx, ClusterSharding sharding
            , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
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
                .build();
    }

    private Behavior<AppraiserCommand> onSketchArrived(AppraiserCommand.SketchArrived sketchArrived) {

        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(),sketchArrived.summary().colId(),
                    "-", String.valueOf(Debug.State.NONE),
                    "Sketch arrived for epoch  {}, kmv: {}, colid: {} , cardinaliy: {}",sketchArrived.summary().epoch(),
                    sketchArrived.summary().kmv(),sketchArrived.summary().colId(), sketchArrived.summary().distinctValues());
        sketchSummaries.put(sketchArrived.summary().colId(),sketchArrived.summary());
        if(sketchSummaries.size()==metadata.totalCols())
            evaluatePairs();
        return this;
    }

    private Behavior<AppraiserCommand> onEpochComplete(AppraiserCommand.EpochComplete epochComplete) {
        for (int c = 0; c < metadata.totalCols(); c++) {
            sharding.entityRefFor(AttributeActor.TYPE_KEY, AACommand.entityId(c)).tell(
                            new AACommand.EmitSketch(epochComplete.epoch(), getContext().getSelf()));
        }
        currentEpoch = epochComplete.epoch();
        return this;
    }

    private void evaluatePairs() {
        if (currentEpoch == lastEvaluatedEpoch) 
            return;
        lastEvaluatedEpoch = currentEpoch;

        List<Integer> cols = new ArrayList<>(sketchSummaries.keySet());
        Collections.sort(cols);

        int emitted = 0, prunedType = 0, prunedHeuristic = 0;

        for (int lhs : cols) {
            for (int rhs : cols) {
                if (lhs == rhs)
                    continue;
                SketchSummary ls = sketchSummaries.get(lhs);
                SketchSummary rs = sketchSummaries.get(rhs);

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
                if (containment < 0.70) {
                    prunedHeuristic++;
                    continue;
                }

                emitted++;
                if(Debug.MESSAGE)
                    formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.app(),-1,
                            Debug.pair(lhs,rhs), String.valueOf(Debug.State.NONE),
                            " Proposing pair: {} , {} , containment: {}",lhs,rhs,containment);
                activeCandidates.add(new UnaryPair(lhs, rhs));
                //cmRef.tell(new CMCommand.UnaryCandidateProposed(new UnaryCandidate(new UnaryPair(lhs, rhs), currentEpoch)));
                EntityRef<CMCommand> cmShard = sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhs));
                cmShard.tell(new CMCommand.UnaryCandidateProposed(new UnaryCandidate(new UnaryPair(lhs, rhs), currentEpoch)));
            }
            if(Debug.INTERNAL)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.app(),-1,
                        "-", String.valueOf(Debug.State.NONE),
                        "Pruned {} distinct values for type and distinct count heuristic,{} distinct values for containment heuristic" +
                                " Evaluated {} pairs",prunedType,prunedHeuristic,emitted);

            }
        }
}
