package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static disIND.streamBasedShardedDispatcher.utility.ColTypeCompatibility.testCompatibility;


public class AppraisalActor_ extends AbstractBehavior<AppraiserCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final ClusterSharding sharding;
    private final ActorRef<CMCommand> cmRef;
    private final ActorRef<SketchSummary> sketchAdapter;

    private final HashMap<Integer,SketchSummary> sketchSummaries= new HashMap<>();
    private long currentEpoch       = 0L;
    private long lastEvaluatedEpoch = -1L;

    public static Behavior<AppraiserCommand> create( ClusterSharding sharding, ActorRef<CMCommand> cmRef,
                                                    DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new AppraisalActor_(ctx, sharding, cmRef,
                metadata,statsRef));
    }

    private AppraisalActor_(ActorContext<AppraiserCommand> ctx, ClusterSharding sharding, ActorRef<CMCommand> cmRef
            , DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.sharding = sharding;
        this.cmRef    = cmRef;
        this.sketchAdapter = ctx.messageAdapter(SketchSummary.class, AppraiserCommand.SketchArrived::new);
        this.metadata = metadata;
        this.statsRef = statsRef;
    }

    @Override
    public Receive<AppraiserCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(AppraiserCommand.EpochComplete.class,this::onEpochComplete)
                .onMessage(AppraiserCommand.SketchArrived.class,this::onSketchArrived)
                .build();
    }

    private Behavior<AppraiserCommand> onSketchArrived(AppraiserCommand.SketchArrived sketchArrived) {

        getContext().getLog().info("[APP] Sketch arrived for epoch  {}, kmv: {}, colid: {} , cardinaliy: {}"
                ,sketchArrived.summary().epoch(), sketchArrived.summary().kmv(),sketchArrived.summary().colId(),
                sketchArrived.summary().distinctValues());
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
                getContext().getLog().info("[APP] Proposing pair: {} , {} , containment: {}",lhs,rhs,containment);
                cmRef.tell(new CMCommand.UnaryCandidateProposed(new UnaryCandidate(new UnaryPair(lhs, rhs), currentEpoch)));
            }
            getContext().getLog().info("[APP] Pruned {} distinct values for type and distinct count heuristic",prunedType);
            getContext().getLog().info("[APP] Pruned {} distinct values for containment heuristic",prunedHeuristic);
            getContext().getLog().info("[APP] Evaluated {} pairs",emitted);

            }
        }
}
