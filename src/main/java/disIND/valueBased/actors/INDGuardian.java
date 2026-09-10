package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.typed.Cluster;
import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.model.ClusterOptions;
import disIND.valueBased.model.SharedModel.*;
import disIND.valueBased.protocol.ValueOwnerProtocol.FinalizeMembership;
import disIND.valueBased.protocol.DrainProtocol;
import disIND.valueBased.protocol.MembershipWriteProtocol;
import disIND.valueBased.structures.*;
import disIND.valueBased.utility.Debug;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
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
            DataOrientation orientation, CandidateTrackingMode candidateTracking, ClusterOptions clusterOptions) {

        public static Config withAll(DatasetMetadata metadata, DataOrientation orientation,
                CandidateTrackingMode candidateTracking) {
            return withAll(metadata, orientation, candidateTracking, ClusterOptions.configured());
        }

        public static Config withAll(DatasetMetadata metadata, DataOrientation orientation,
                CandidateTrackingMode candidateTracking, ClusterOptions options) {
            return new Config(metadata.totalCols(), 3, 32, 1, metadata, orientation, candidateTracking, options);
        }
    }

    private final ActorRef<RCCommand> rcRef;
    private final ClusterSharding sharding;
    private final DatasetMetadata metadata;
    private final AtomicLong totalRows = new AtomicLong(0L);

    private record WorkerResources(ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore,
            ActorRef<MembershipWriteProtocol.Command> membershipWriter,
            ActorRef<DrainProtocol.Command> drainDispatcher, CandidateDomain candidateDomain,
            WorkerPhaseMetrics phaseMetrics, WorkerMetricsWriter metricsWriter, WorkerMetricsFlusher metricsFlusher) {
    }

    private final Optional<WorkerResources> workerResources;
    private final AtomicBoolean storesClosed = new AtomicBoolean();

    public static Behavior<BDCommand> create(Config cfg) {
        return create(cfg, null);
    }

    public static Behavior<BDCommand> create(Config cfg, ActorRef<RCCommand> collector) {
        return Behaviors.setup(ctx -> new INDGuardian(ctx, cfg, collector));
    }

    private INDGuardian(ActorContext<BDCommand> ctx, Config cfg, ActorRef<RCCommand> collector) {
        super(ctx);
        DatasetMetadata metadata = cfg.metadata();
        this.metadata = metadata;
        if (collector == null && !Cluster.get(ctx.getSystem()).selfMember().hasRole("coordinator"))
            throw new IllegalStateException("Workers require the coordinator-owned result collector");
        this.rcRef = collector != null ? collector : ctx.spawn(ResultCollectorActor.create(metadata),
                "result-collector", Props.empty().withDispatcherFromConfig(DISPATCHER_DEFAULT));
        if (collector == null)
            ctx.getLog().info("IND settings: mode={} calculation={} clusterChangeDetection={} validation=lhs-intersection",
                    cfg.candidateTracking(), cfg.clusterOptions().calculation(), cfg.clusterOptions().changeDetection());

        ClusterSharding sharding = ClusterSharding.get(ctx.getSystem());
        this.sharding = sharding;
        // Use the same role as sharding, including nodes with both coordinator and
        // worker roles.
        this.workerResources = Cluster.get(ctx.getSystem()).selfMember().hasRole("worker")
                ? Optional.of(createWorkerResources(ctx, cfg, sharding))
                : Optional.empty();

        sharding.init(Entity.of(ValueOwnerActor.TYPE_KEY, entityCtx -> {
            WorkerResources worker = workerResources.orElseThrow(
                    () -> new IllegalStateException("Value owners require worker-local resources"));
            return ValueOwnerActor.create(entityCtx.getEntityId(),
                    sharding, metadata, worker.membershipStore(), worker.valueIdStore(), cfg.orientation(),
                    cfg.candidateTracking(), worker.drainDispatcher(), worker.membershipWriter(),
                    worker.candidateDomain(), worker.phaseMetrics(), cfg.clusterOptions());
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

    private static WorkerResources createWorkerResources(ActorContext<BDCommand> ctx, Config cfg,
            ClusterSharding sharding) {
        String nodeId = Cluster.get(ctx.getSystem()).selfMember().address().toString().replaceAll("[^A-Za-z0-9._-]",
                "_");
        WorkerValueIdMetrics valueIdMetrics = new WorkerValueIdMetrics();
        WorkerMembershipMetrics membershipMetrics = new WorkerMembershipMetrics();
        WorkerPhaseMetrics phaseMetrics = new WorkerPhaseMetrics();
        WorkerMetricsWriter metricsWriter = new WorkerMetricsWriter(nodeId, ctx.getLog());
        ValueOwnerMembershipStore membershipStore = new ValueOwnerMembershipStore(
                Path.of(UserConfig.VALUE_OWNER_DISK_DIR, nodeId), UserConfig.VALUE_OWNER_HOT_ENTRIES,
                cfg.candidateTracking(), UserConfig.VALUE_OWNER_BUCKETS, membershipMetrics);
        WorkerValueIdStore valueIdStore = new WorkerValueIdStore(Path.of(UserConfig.VALUE_ID_DISK_DIR, nodeId),
                UserConfig.VALUE_ID_HOT_ENTRIES, UserConfig.VALUE_OWNER_BUCKETS, valueIdMetrics);
        ActorRef<MembershipWriteProtocol.Command> membershipWriter = ctx.spawn(
                MembershipWriterActor.create(membershipStore), "membership-writer",
                Props.empty().withDispatcherFromConfig(DISPATCHER_IO));
        ActorRef<DrainProtocol.Command> drainDispatcher = ctx.spawn(DrainDispatcherActor.create(sharding), "drainer",
                Props.empty().withDispatcherFromConfig(DISPATCHER_DEFAULT));
        return new WorkerResources(membershipStore, valueIdStore, membershipWriter, drainDispatcher,
                new CandidateDomain(cfg.metadata()), phaseMetrics, metricsWriter,
                new WorkerMetricsFlusher(metricsWriter, valueIdStore, membershipStore, phaseMetrics));
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
                    closeStores();
                    return Behaviors.stopped();
                })
                .onSignal(PostStop.class, signal -> {
                    closeStores();
                    return Behaviors.same();
                })
                .build();
    }

    private void closeStores() {
        if (!storesClosed.compareAndSet(false, true))
            return;
        workerResources.ifPresent(worker -> {
            worker.metricsFlusher().flushOnce();
            WorkerValueIdStore.DBSnapshot valueIdStorage = worker.valueIdStore().finalStorageSnapshot();
            ValueOwnerMembershipStore.DBSnapshot membershipStorage = worker.membershipStore().finalStorageSnapshot();
            worker.metricsWriter().writeAuxiliaryStorage(valueIdStorage, membershipStorage);
            worker.valueIdStore().close();
            worker.membershipStore().close();
        });
    }

}
