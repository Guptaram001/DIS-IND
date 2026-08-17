package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.SingletonActor;
import disIND.valueBased.model.SharedModel.*;
import disIND.valueBased.monitor.DiscoveryStatsActor;
import disIND.valueBased.monitor.StatsCommand;
import disIND.valueBased.structures.*;
import disIND.valueBased.utility.Debug;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.file.Path;
import disIND.valueBased.utility.UserConfig;

import static disIND.valueBased.utility.Debug.formLog;

public final class INDGuardian extends AbstractBehavior<BDCommand> {

    public record Config(int numCols, int maxArity, int maxConcurrentNra, int cleanThreshold,DatasetMetadata metadata,
        DataOrientation orientation,CandidateTrackingMode candidateTracking) {

        public static Config withAll(DatasetMetadata metadata, DataOrientation orientation,CandidateTrackingMode candidateTracking) {
            return new Config(metadata.totalCols(), 3, 32, 1, metadata,orientation,candidateTracking);
        }
    }

    private final ActorRef<RCCommand> rcRef;
    private final ClusterSharding sharding;
    private final DatasetMetadata metadata;
    private final AtomicLong totalRows = new AtomicLong(0L);
    private final ValueOwnerMembershipStore valueOwnerMembershipStore;

    public static Behavior<BDCommand> create(Config cfg) {
        return Behaviors.setup(ctx -> new INDGuardian(ctx, cfg));
    }

    private INDGuardian(ActorContext<BDCommand> ctx, Config cfg) {
        super(ctx);
        DatasetMetadata metadata = cfg.metadata();
        this.metadata = metadata;
        ClusterSingleton singletons = ClusterSingleton.get(ctx.getSystem());
        
        ActorRef<StatsCommand> statsRef = singletons.init(SingletonActor.of(DiscoveryStatsActor.create(), "discovery-stats"));
        this.rcRef = singletons.init(SingletonActor.of(ResultCollectorActor.create(metadata, statsRef), "result-collector"));
        
        String nodeId = Cluster.get(ctx.getSystem()).selfMember().address().toString().replaceAll("[^A-Za-z0-9._-]", "_");
        
        this.valueOwnerMembershipStore = new ValueOwnerMembershipStore(Path.of(UserConfig.VALUE_OWNER_DISK_DIR, nodeId),
                UserConfig.VALUE_OWNER_HOT_ENTRIES,cfg.candidateTracking);
        
        WorkerValueIdStore workerValueIdStore = new WorkerValueIdStore(Path.of(UserConfig.VALUE_ID_DISK_DIR, nodeId),
                UserConfig.VALUE_ID_HOT_ENTRIES, UserConfig.VALUE_OWNER_BUCKETS);
        
        ClusterSharding sharding = ClusterSharding.get(ctx.getSystem());
        this.sharding = sharding;
        
        sharding.init(Entity.of(ValueOwnerActor.TYPE_KEY,entityCtx -> {return ValueOwnerActor.create(entityCtx.getEntityId(), 
            sharding, metadata,valueOwnerMembershipStore, workerValueIdStore,cfg.orientation(), cfg.candidateTracking());
                }).withRole("worker")
                .withEntityProps(Props.empty().withDispatcherFromConfig("checkpoint-io-dispatcher")));
        
        sharding.init(Entity.of(DirectBatchAggregatorActor.TYPE_KEY,entityCtx -> DirectBatchAggregatorActor.create())
        .withRole("worker"));
        
        if(Debug.INTERNAL)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.guardian(),-1,"-",
                    String.valueOf(Debug.State.NONE), " Sharding init: {} columns",cfg.numCols());


        sharding.init(Entity.of(CandidateManagerActor_.TYPE_KEY, entityCtx -> {
                    int lhsCol = Integer.parseInt(entityCtx.getEntityId().substring("cm-lhs-".length()));
                    return CandidateManagerActor_.create(lhsCol, rcRef, metadata);
                }).withRole("worker")
        );

        if(Debug.INTERNAL)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.guardian(),-1,"-",
                    String.valueOf(Debug.State.NONE), "All actors spawned. Ready.");

    }

    @Override
    public Receive<BDCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(BDCommand.IngestBatch.class, msg -> {
                    totalRows.addAndGet(msg.numRows());
                    return this;
                })
                .onMessage(BDCommand.FinishDiscovery.class, msg -> {
                    rcRef.tell(new RCCommand.AwaitDiscoveryFinished(msg.finalRound(), msg.replyTo()));
                    for (int ownerId = 0; ownerId < UserConfig.VALUE_OWNER_BUCKETS; ownerId++) {
                        sharding.entityRefFor(ValueOwnerActor.TYPE_KEY, ValueOwnerActor.entityId(ownerId))
                                .tell(new ValueOwnerActor.FinalizeMembership( msg.finalRound(), UserConfig.VALUE_OWNER_BUCKETS,
                                        metadata.totalCols()));
                    }
                    return this;
                })
                .onMessage(BDCommand.GetResultCollector.class, msg -> {
                    msg.replyTo().tell(rcRef);
                    return this;
                })
                .onMessage(BDCommand.GetIngestionReady.class, msg -> {
                    msg.replyTo().tell(new IngestionReady());
                    return this;
                })
                .onMessage(BDCommand.Shutdown.class, msg -> {
                    return Behaviors.stopped();
                })
                .build();
    }
}
