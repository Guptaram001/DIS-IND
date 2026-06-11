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
import disIND.streamBasedShardedDispatcher.structures.*;

import java.util.HashMap;
import java.util.Map;

public class BatchDispatcherActor_  extends AbstractBehavior<BDCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final  DatasetMetadata metadata;
    private final int numCols;
    private final ValueIdMap valueIdMap;
    private final ClusterSharding sharding;
    private final ActorRef<AppraiserCommand> appraiserRef;
    private final ActorRef<BDCommand> selfRef;
    private int[]  cursors;


    public static Behavior<BDCommand> create(int numCols, ValueIdMap vidMap, ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                             DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx ->
                new BatchDispatcherActor_(ctx, numCols, vidMap, sharding, appraiserRef,metadata,statsRef));
    }

    private BatchDispatcherActor_(ActorContext<BDCommand> ctx, int numCols, ValueIdMap valueIdMap,
                                  ClusterSharding sharding, ActorRef<AppraiserCommand> appraiserRef,
                                  DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.numCols      = numCols;
        this.valueIdMap   = valueIdMap;
        this.sharding     = sharding;
        this.appraiserRef = appraiserRef;
        this.selfRef      = ctx.getSelf();
        this.cursors      = new int[numCols];
        this.metadata     = metadata;
        this.statsRef     = statsRef;
    }

    @Override
    public Receive<BDCommand> createReceive() {

        return newReceiveBuilder()
                .onMessage(BDCommand.IngestBatch.class,this::ingestBatch)
                .build();
    }

    private Behavior<BDCommand> ingestBatch(BDCommand.IngestBatch batch) {
        return this;
    }
}
