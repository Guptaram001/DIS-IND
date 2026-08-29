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
import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.model.SharedModel.*;
import disIND.valueBased.protocol.ValueOwnerProtocol.FinalizeMembership;
import disIND.valueBased.protocol.DrainProtocol;
import disIND.valueBased.protocol.MembershipWriteProtocol;
import disIND.valueBased.structures.*;
import disIND.valueBased.utility.Debug;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.file.Path;
import disIND.valueBased.utility.UserConfig;
import disIND.valueBased.monitor.WorkerPhaseMetrics;
import disIND.valueBased.monitor.WorkerMembershipMetrics;
import disIND.valueBased.monitor.WorkerMetricsFlusher;
import disIND.valueBased.monitor.WorkerMetricsWriter;
import static disIND.valueBased.utility.Debug.formLog;
import disIND.valueBased.monitor.WorkerValueIdMetrics;

public final class INDGuardian extends AbstractBehavior<BDCommand> {
    private static final String DISPATCHER_DEFAULT = "akka.actor.default-dispatcher";
    // private static final String DISPATCHER_INTERNAL =
    // "akka.actor.internal-dispatcher";
    private static final String DISPATCHER_VO = "akka.actor.vo-work-dispatcher";
    private static final String DISPATCHER_IO = "akka.actor.io-dispatcher";
    private static final String DISPATCHER_CPU_INTENSIVE = "akka.actor.cpu-intensive-dispatcher";

    public record Config(int numCols, int maxArity, int maxConcurrentNra, int cleanThreshold, DatasetMetadata metadata,
            DataOrientation orientation, CandidateTrackingMode candidateTracking) {

        public static Config withAll(DatasetMetadata metadata, DataOrientation orientation,
                CandidateTrackingMode candidateTracking) {
            return new Config(metadata.totalCols(), 3, 32, 1, metadata, orientation, candidateTracking);
        }
    }

    private final ActorRef<RCCommand> rcRef;
    private final ClusterSharding sharding;
    private final DatasetMetadata metadata;
    private final AtomicLong totalRows = new AtomicLong(0L);
    private final ValueOwnerMembershipStore valueOwnerMembershipStore;
    private final ActorRef<DrainProtocol.Command> drainDispatcher;
    private final ActorRef<MembershipWriteProtocol.Command> membershipWriter;
    private final WorkerValueIdStore workerValueIdStore;
    private final WorkerMetricsWriter metricsWriter;
    private final WorkerPhaseMetrics workerPhaseMetrics;
    private final WorkerValueIdMetrics valueIdMetrics;
    private final WorkerMembershipMetrics membershipMetrics;
    private final WorkerMetricsFlusher workerMetricsFlusher;

    public static Behavior<BDCommand> create(Config cfg) {
        return Behaviors.setup(ctx -> new INDGuardian(ctx, cfg));
    }

    private INDGuardian(ActorContext<BDCommand> ctx, Config cfg) {
        super(ctx);
        DatasetMetadata metadata = cfg.metadata();
        CandidateDomain candidateDomain = new CandidateDomain(metadata);
        this.valueIdMetrics = new WorkerValueIdMetrics();
        this.membershipMetrics = new WorkerMembershipMetrics();
        this.metadata = metadata;
        ClusterSingleton singletons = ClusterSingleton.get(ctx.getSystem());

        this.rcRef = singletons
                .init(SingletonActor.of(ResultCollectorActor.create(metadata), "result-collector")
                        .withProps(Props.empty().withDispatcherFromConfig(DISPATCHER_DEFAULT)));

        String nodeId = Cluster.get(ctx.getSystem()).selfMember().address().toString().replaceAll("[^A-Za-z0-9._-]",
                "_");

        this.workerPhaseMetrics = new WorkerPhaseMetrics();
        this.metricsWriter = new WorkerMetricsWriter(nodeId, ctx.getLog());

        this.valueOwnerMembershipStore = new ValueOwnerMembershipStore(Path.of(UserConfig.VALUE_OWNER_DISK_DIR, nodeId),
                UserConfig.VALUE_OWNER_HOT_ENTRIES, cfg.candidateTracking,
                UserConfig.VALUE_OWNER_BUCKETS, membershipMetrics);

        this.membershipWriter = ctx.spawn(MembershipWriterActor.create(valueOwnerMembershipStore),
                "membership-writer", Props.empty().withDispatcherFromConfig(DISPATCHER_IO));

        this.workerValueIdStore = new WorkerValueIdStore(Path.of(UserConfig.VALUE_ID_DISK_DIR, nodeId),
                UserConfig.VALUE_ID_HOT_ENTRIES, UserConfig.VALUE_OWNER_BUCKETS, valueIdMetrics);

        this.workerMetricsFlusher = new WorkerMetricsFlusher(metricsWriter, workerValueIdStore,
                valueOwnerMembershipStore, workerPhaseMetrics);

        ClusterSharding sharding = ClusterSharding.get(ctx.getSystem());
        this.sharding = sharding;
        this.drainDispatcher = ctx.spawn(DrainDispatcherActor.create(sharding), "drainer",
                Props.empty().withDispatcherFromConfig(DISPATCHER_DEFAULT));

        sharding.init(Entity.of(ValueOwnerActor.TYPE_KEY, entityCtx -> {
            return ValueOwnerActor.create(entityCtx.getEntityId(),
                    sharding, metadata, valueOwnerMembershipStore, workerValueIdStore, cfg.orientation(),
                    cfg.candidateTracking(), drainDispatcher, membershipWriter, candidateDomain, workerPhaseMetrics,
                    workerMetricsFlusher);
        }).withRole("worker").withEntityProps(Props.empty().withDispatcherFromConfig(DISPATCHER_VO)));

        sharding.init(Entity.of(DirectBatchAggregatorActor.TYPE_KEY, entityCtx -> DirectBatchAggregatorActor.create())
                .withRole("worker").withEntityProps(Props.empty()
                        .withDispatcherFromConfig(DISPATCHER_DEFAULT)));

        if (Debug.INTERNAL)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.guardian(), -1, "-",
                    String.valueOf(Debug.State.NONE), " Sharding init: {} columns", cfg.numCols());

        // No spawn of CM beforehand to save computation of unnecessary CMs.
        sharding.init(Entity.of(CandidateManagerActor_.TYPE_KEY, entityCtx -> {
            int partitionId = Integer.parseInt(entityCtx.getEntityId().substring("cm-part-".length()));
            return CandidateManagerActor_.create(partitionId, rcRef, metadata);
        }).withRole("worker").withEntityProps(Props.empty().withDispatcherFromConfig(DISPATCHER_CPU_INTENSIVE)));

        if (Debug.INTERNAL)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.guardian(), -1, "-",
                    String.valueOf(Debug.State.NONE), "All actors spawned. Ready.");

    }

    private void initializeAllLhsPartitions(int finalRound) {
        for (int partitionId = 0; partitionId < UserConfig.DEFAULT_CM_PARTITIONS; partitionId++) {

            sharding.entityRefFor(
                    CandidateManagerActor_.TYPE_KEY,
                    CMCommand.entityId(partitionId))
                    .tell(new CMCommand.EnsurePartitionInitialized(
                            partitionId,
                            finalRound));
        }
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

                    // Initialize every LHS exactly once, including empty columns.
                    initializeAllLhsPartitions(msg.finalRound());

                    // After initialization, start the VO drain/finalization process.
                    for (int ownerId = 0; ownerId < UserConfig.VALUE_OWNER_BUCKETS; ownerId++) {
                        sharding.entityRefFor(ValueOwnerActor.TYPE_KEY, ValueOwnerActor.entityId(ownerId))
                                .tell(new FinalizeMembership(msg.finalRound(), UserConfig.VALUE_OWNER_BUCKETS,
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
                    workerMetricsFlusher.flushOnce();
                    workerValueIdStore.close();
                    valueOwnerMembershipStore.close();
                    return Behaviors.stopped();
                })
                .build();
    }

}
