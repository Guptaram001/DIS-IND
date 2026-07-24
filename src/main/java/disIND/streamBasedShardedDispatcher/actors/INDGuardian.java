package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.SingletonActor;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.DiscoveryStatsActor;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.structures.*;
import disIND.streamBasedShardedDispatcher.utility.Debug;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;

public final class INDGuardian extends AbstractBehavior<BDCommand> {

    public record Config(int numCols, int maxArity, int maxConcurrentNra, int cleanThreshold,DatasetMetadata metadata) {

        public static Config withAll(DatasetMetadata metadata) {
            return new Config(metadata.totalCols(), 3, 32, 1, metadata);
        }
    }

    private final ActorRef<BDCommand> bdRef;
    private final ActorRef<RCCommand> rcRef;
    private final AtomicLong totalRows = new AtomicLong(0L);

    public static Behavior<BDCommand> create(Config cfg) {
        return Behaviors.setup(ctx -> new INDGuardian(ctx, cfg));
    }

    private INDGuardian(ActorContext<BDCommand> ctx, Config cfg) {
        super(ctx);
        ValueIdMap vidMap = new ValueIdMap();
        AtomicReference<ActorRef<CMCommand>> cmRefHolder = new AtomicReference<>();
        DatasetMetadata metadata = cfg.metadata();
        ClusterSingleton singletons = ClusterSingleton.get(ctx.getSystem());
        ActorRef<StatsCommand> statsRef = singletons.init(SingletonActor.of(DiscoveryStatsActor.create(), "discovery-stats"));
        this.rcRef = singletons.init(SingletonActor.of(ResultCollectorActor.create(metadata, statsRef), "result-collector"));

        ClusterSharding sharding = ClusterSharding.get(ctx.getSystem());
        sharding.init(Entity.of(AttributeActor.TYPE_KEY, entityCtx -> {
                    int colId = Integer.parseInt(entityCtx.getEntityId().substring("col-".length()));
                    return AttributeActor.create(colId, vidMap, cmRefHolder,statsRef,metadata);
                }).withRole("worker")
        );
        if(Debug.INTERNAL)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.guardian(),-1,"-",
                    String.valueOf(Debug.State.NONE), " Sharding init: {} columns",cfg.numCols());

        ActorRef<RACommand> raRef = singletons.init(SingletonActor.of(RebuildActor_.create(sharding, metadata, statsRef), 
                    "rebuild-actor"));

        ActorRef<LMCommand> lmRef = singletons.init(SingletonActor.of(LatticeManagerActor.create(cfg.maxArity(), cfg.maxConcurrentNra(),
                        metadata, statsRef), "lattice-manager"));

        ActorRef<AppraiserCommand> apRef = singletons.init(SingletonActor.of(AppraisalActor_.create(sharding, metadata, statsRef, rcRef),
                     "appraisal-actor"));

        sharding.init(Entity.of(CandidateManagerActor_.TYPE_KEY, entityCtx -> {
                    int lhsCol = Integer.parseInt(entityCtx.getEntityId().substring("cm-lhs-".length()));
                    return CandidateManagerActor_.create(lhsCol, sharding, apRef,raRef, lmRef, rcRef, cfg.cleanThreshold(), metadata,
                            statsRef);
                }).withRole("worker")
        );

        ActorRef<NRACommand> nraRef = singletons.init(SingletonActor.of(NaryRebuildActor.create(sharding, lmRef, 2, metadata,
                        statsRef), "nary-rebuild"));

        lmRef.tell(new LMCommand.InjectNra(nraRef));

        this.bdRef = singletons.init(SingletonActor.of(BatchDispatcherActor_.create(vidMap, sharding, apRef, metadata,
                        statsRef, rcRef), "batch-dispatcher"));

        if(Debug.INTERNAL)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.guardian(),-1,"-",
                    String.valueOf(Debug.State.NONE), "All actors spawned. Ready.");

    }

    @Override
    public Receive<BDCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(BDCommand.IngestBatch.class, msg -> {
                    totalRows.addAndGet(msg.numRows());
                    bdRef.tell(msg);
                    return this;
                })
                .onMessage(BDCommand.GetBatchDispatcher.class, msg -> {
                    msg.replyTo().tell(bdRef);
                    getContext().getLog().info("[Guardian] Sending BD ref {}", bdRef);
                    return this;
                })
                .onMessage(BDCommand.GetResultCollector.class, msg -> {
                    msg.replyTo().tell(rcRef);
                    return this;
                })
                .onMessage(BDCommand.Shutdown.class, msg -> {
                    bdRef.tell(msg);
                    return Behaviors.stopped();
                })
                .build();
    }
}
