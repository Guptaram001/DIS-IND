package disIND.valueBased.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import disIND.valueBased.ingestion.BatchProcessor;
import disIND.valueBased.ingestion.ColumnMajorProcessor;
import disIND.valueBased.ingestion.ValueMajorProcessor;
import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.membership.ColumnSetFactory;
import disIND.valueBased.membership.MembershipBatchResult;
import disIND.valueBased.membership.MembershipUpdater;
import disIND.valueBased.model.SharedModel.CMCommand;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.model.SharedModel.DataOrientation;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import disIND.valueBased.model.SharedModel.MembershipUpdates;
import disIND.valueBased.model.SharedModel.ValueUpdates;
import disIND.valueBased.protocol.ValueOwnerProtocol.Command;
import disIND.valueBased.protocol.ValueOwnerProtocol.FinalizeMembership;
import disIND.valueBased.protocol.ValueOwnerProtocol.StoreBatch;
import disIND.valueBased.protocol.ValueOwnerProtocol.RetryDrainProbe;
import disIND.valueBased.protocol.ValueOwnerProtocol.PartitionDrainQueued;
import disIND.valueBased.protocol.ValueOwnerProtocol.PartitionCandidateManagerReady;
import disIND.valueBased.protocol.DrainProtocol;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.WorkerValueIdStore;
import disIND.valueBased.tracking.CandidateViolationAfterApplyingUpdates;
import disIND.valueBased.tracking.CandidateEvaluator;
import disIND.valueBased.tracking.TrackingResult;
import disIND.valueBased.tracking.ModeSpecificContext;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.utility.UserConfig;
import disIND.valueBased.monitor.WorkerMetricsFlusher;
import disIND.valueBased.monitor.WorkerPhaseMetrics;
import disIND.valueBased.monitor.WorkerPhaseMetrics.Phase;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2BooleanLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.time.Duration;
import akka.actor.typed.ActorRef;

//@Dispatcher("internal-dispatcher")
public final class ValueOwnerActor extends AbstractBehavior<Command> {
    public static final EntityTypeKey<Command> TYPE_KEY = EntityTypeKey.create(Command.class, "ValueOwnerActor");

    private final String entityId;
    private final int bucketId;
    private final ClusterSharding sharding;
    private final ValueOwnerMembershipStore membershipStore;
    private final WorkerValueIdStore valueIdStore;
    private final ModeSpecificContext modeSpecificContext;
    private final MembershipUpdater membershipUpdater;
    private final CandidateEvaluator candidateEvaluator;
    private final int recentBatchLimit;
    private final Long2BooleanLinkedOpenHashMap resolvedBatches;
    private final DataOrientation orientation;
    private final BatchProcessor batchProcessor;
    private int statusSequence;
    private final ActorRef<DrainProtocol.Command> drainDispatcher;
    private final TimerScheduler<Command> timers;
    private FinalizeMembership finalization;
    private int nextDrainPartition;
    private List<DrainProtocol.DrainRecord> awaitingPartitionDrain;
    private boolean awaitingPartitionReady;
    private final WorkerPhaseMetrics phaseMetrics;
    private final WorkerMetricsFlusher workerMetricsFlusher;

    public static String entityId(int bucketId) {
        return "value-bucket-" + bucketId;
    }

    public static Behavior<Command> create(
            String entityId, ClusterSharding sharding, DatasetMetadata metadata,
            ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore, DataOrientation orientation,
            CandidateTrackingMode candidateTracking, ActorRef<DrainProtocol.Command> drainDispatcher,
            CandidateDomain candidateDomain, WorkerPhaseMetrics phaseMetrics,
            WorkerMetricsFlusher workerMetricsFlusher) {
        return Behaviors.withTimers(timers -> Behaviors.setup(ctx -> new ValueOwnerActor(
                ctx, entityId, sharding, metadata, membershipStore, valueIdStore, orientation,
                candidateTracking, drainDispatcher, timers, candidateDomain, phaseMetrics, workerMetricsFlusher)));
    }

    private ValueOwnerActor(ActorContext<Command> context, String entityId, ClusterSharding sharding,
            DatasetMetadata metadata, ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore,
            DataOrientation orientation, CandidateTrackingMode candidateTrackingMode,
            ActorRef<DrainProtocol.Command> drainDispatcher, TimerScheduler<Command> timers,
            CandidateDomain candidateDomain, WorkerPhaseMetrics phaseMetrics,
            WorkerMetricsFlusher workerMetricsFlusher) {
        super(context);
        this.entityId = entityId;
        this.bucketId = Integer.parseInt(entityId.substring("value-bucket-".length()));
        this.sharding = sharding;
        this.membershipStore = membershipStore;
        this.valueIdStore = valueIdStore;
        this.drainDispatcher = Objects.requireNonNull(drainDispatcher, "drainDispatcher");
        this.timers = timers;
        this.recentBatchLimit = UserConfig.DEFAULT_VO_BATCH_EVICTION_LIMIT;
        this.orientation = orientation;
        this.batchProcessor = newProcessor(orientation);
        this.modeSpecificContext = ModeSpecificContext.create(
                Objects.requireNonNull(candidateTrackingMode, "candidateTrackingMode"),
                bucketId, metadata.totalCols());
        this.workerMetricsFlusher = Objects.requireNonNull(workerMetricsFlusher);

        ColumnSetFactory columnSets = new ColumnSetFactory(metadata.totalCols());
        this.phaseMetrics = Objects.requireNonNull(phaseMetrics);
        this.membershipUpdater = new MembershipUpdater(bucketId, membershipStore,
                columnSets, modeSpecificContext, phaseMetrics);
        this.candidateEvaluator = new CandidateEvaluator(metadata, columnSets, modeSpecificContext, candidateDomain);

        this.resolvedBatches = new Long2BooleanLinkedOpenHashMap(recentBatchLimit);

        if (Debug.INTERNAL) {
            getContext().getLog().info(
                    "[PLACEMENT] type=VO bucket={} entity={} node={}",
                    bucketId, entityId, Cluster.get(context.getSystem()).selfMember().address());
        }
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StoreBatch.class, this::onStoreBatch)
                // .onMessage(GetBucket.class, this::onGetBucket)
                .onMessage(FinalizeMembership.class, this::onFinalizeMembership)
                .onMessage(PartitionDrainQueued.class, this::onPartitionDrainQueued)
                .onMessage(PartitionCandidateManagerReady.class, this::onPartitionCandidateManagerReady)
                .onMessageEquals(RetryDrainProbe.INSTANCE, this::onRetryDrainProbe)
                .build();
    }

    private Behavior<Command> onStoreBatch(StoreBatch message) {
        if (Debug.INTERNAL) {
            getContext().getLog().info(
                    "[VO] round={} bucket={} epoch={} tableId={} batchId={}",
                    message.round(), message.bucketId(), message.epoch(), message.tableId(), message.batchId());
        }

        if (message.bucketId() != bucketId) {
            throw new IllegalArgumentException(
                    "Message for bucket " + message.bucketId() + " sent to value owner " + entityId);
        }

        if (message.orientation() != orientation) {
            throw new IllegalArgumentException(
                    "VO configured for " + orientation + " but received " + message.orientation());
        }

        long batchKey = ((long) message.tableId() << 32) | (message.batchId() & 0xFFFFFFFFL);
        // Message already handled.
        if (resolvedBatches.getAndMoveToLast(batchKey)) {
            acknowledge(message);
            return this;
        }

        // handles the incoming message into valueId-colId-count for both cases as
        // deltas
        long started = System.nanoTime();
        MembershipUpdates updates = batchProcessor.process(bucketId, message.body(), valueIdStore);
        phaseMetrics.record(Phase.BATCH_PREPARATION, System.nanoTime() - started);
        statusSequence = Math.incrementExact(statusSequence);

        applyUpdates(message, updates, statusSequence);
        resolvedBatches.putAndMoveToLast(batchKey, true);
        if (resolvedBatches.size() > recentBatchLimit) {
            resolvedBatches.removeFirstBoolean();
        }
        acknowledge(message);
        return this;
    }

    private void applyUpdates(StoreBatch message, MembershipUpdates updates, int voSequence) {
        if (updates instanceof ValueUpdates valueUpdates) {
            applyValueUpdates(message, valueUpdates.byValue(), voSequence);
            return;
        }

        throw new IllegalArgumentException(
                "Unsupported membership update type: " + updates.getClass().getName());
    }

    private void applyValueUpdates(StoreBatch message, Int2ObjectMap<Int2IntMap> updates,
            int voSequence) {
        // Applies the updates to the store and obtains the final changes merging to
        // previous records with added/removed columns
        MembershipBatchResult membership = membershipUpdater.apply(updates);

        // Selects the specific mode changes to handle the violation further.
        CandidateViolationAfterApplyingUpdates candidateViolationAfterApplyingUpdates = modeSpecificContext
                .tracker().newChanges(bucketId);
        // Find which IND pairs might have been changed.
        long started = System.nanoTime();
        candidateEvaluator.evaluate(membership.newlyAddedColumnsByValue(),
                membership.updatedRecordsByValue(), candidateViolationAfterApplyingUpdates);
        phaseMetrics.record(Phase.CANDIDATE_EVALUATION, System.nanoTime() - started);

        started = System.nanoTime();
        TrackingResult trackingResult = modeSpecificContext.tracker().apply(candidateViolationAfterApplyingUpdates,
                membership.updatedRecordsByValue(), membershipStore);
        phaseMetrics.record(Phase.TRACKER_RESOLUTION, System.nanoTime() - started);

        started = System.nanoTime();
        membershipStore.writeBatch(bucketId, membership.updatedRecordsByValue(), trackingResult.changedStates());
        phaseMetrics.record(Phase.ROCKSDB_WRITE, System.nanoTime() - started);

        modeSpecificContext.candidateStatesChanged(trackingResult.changedStates());

        sendCandidateStatusTransitions(message, voSequence, trackingResult.transitionsByLhs(),
                trackingResult.changedStates().size());
    }

    private void sendCandidateStatusTransitions(StoreBatch batch, int voSequence,
            Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs,
            int affectedCandidateCount) {

        int transitionCount = 0;
        int affectedCms = 0;
        ObjectIterator<Int2ObjectMap.Entry<List<CandidateLocalStatus>>> iterator = Int2ObjectMaps
                .fastIterator(transitionsByLhs);
        while (iterator.hasNext()) {
            Int2ObjectMap.Entry<List<CandidateLocalStatus>> entry = iterator.next();
            int lhsCol = entry.getIntKey();
            List<CandidateLocalStatus> transitions = entry.getValue();

            if (transitions == null || transitions.isEmpty())
                continue;

            if (Debug.INTERNAL) {
                transitionCount += transitions.size();
                affectedCms++;
            }
            int cmPartition = CMCommand.partitionFor(lhsCol, UserConfig.DEFAULT_CM_PARTITIONS);
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(cmPartition))
                    .tell(new CMCommand.ValueOwnerCandidateStatusUpdate(
                            lhsCol, batch.epoch(), voSequence, batch.round(), bucketId, transitions));
        }
        if (Debug.INTERNAL) {
            getContext().getLog().info(
                    "[VO] round={} bucket={} epoch={} voSequence={} affectedCandidates={} transitions={} affectedCms={}",
                    batch.round(), bucketId, batch.epoch(), voSequence, affectedCandidateCount,
                    transitionCount, affectedCms);
        }
    }

    private Behavior<Command> onFinalizeMembership(FinalizeMembership message) {
        if (finalization != null && finalization.finalRound() == message.finalRound())
            return this;
        boolean metricsWritten = workerMetricsFlusher.flushOnce();
        if (metricsWritten) {
            getContext().getLog().info("[WORKER-METRICS] flushed before drain node={} bucket={} round={}",
                    Cluster.get(getContext().getSystem()).selfMember().address(),
                    bucketId, message.finalRound());
        }
        finalization = message;
        nextDrainPartition = 0;
        prepareNextPartitionDrain();
        return this;
    }

    private Behavior<Command> onPartitionDrainQueued(PartitionDrainQueued message) {
        if (finalization == null || awaitingPartitionDrain == null
                || message.finalRound() != finalization.finalRound()
                || message.partitionId() != nextDrainPartition || message.bucketId() != bucketId)
            return this;
        timers.cancel(RetryDrainProbe.INSTANCE);
        awaitingPartitionDrain = null;
        awaitingPartitionReady = false;
        nextDrainPartition++;
        prepareNextPartitionDrain();
        return this;
    }

    private Behavior<Command> onPartitionCandidateManagerReady(PartitionCandidateManagerReady message) {
        if (finalization == null || awaitingPartitionDrain == null || !awaitingPartitionReady
                || message.finalRound() != finalization.finalRound()
                || message.partitionId() != nextDrainPartition || message.bucketId() != bucketId)
            return this;
        timers.cancel(RetryDrainProbe.INSTANCE);
        awaitingPartitionReady = false;
        enqueueAwaitingPartitionDrain();
        return this;
    }

    private void prepareNextPartitionDrain() {
        if (finalization == null)
            return;
        if (nextDrainPartition >= UserConfig.DEFAULT_CM_PARTITIONS) {
            if (Debug.INTERNAL)
                getContext().getLog().info(
                        "[VO] bucket={} finalRound={} queuedCmPartitions={} localRejectedCandidates={}",
                        bucketId, finalization.finalRound(), UserConfig.DEFAULT_CM_PARTITIONS,
                        modeSpecificContext.locallyRejectedCount());
            return;
        }

        List<DrainProtocol.DrainRecord> records = new ArrayList<>();
        for (int lhs = nextDrainPartition; lhs < finalization.totalColumns(); lhs += UserConfig.DEFAULT_CM_PARTITIONS) {
            records.add(new DrainProtocol.DrainRecord(
                    finalization.finalRound(), lhs, bucketId, finalization.expectedBuckets(), new RoaringBitmap(),
                    candidateEvaluator.candidateEvaluationsFor(lhs), candidateEvaluator.exactComparisonsFor(lhs),
                    modeSpecificContext.metricsFor(lhs),
                    lhs == 0 ? modeSpecificContext.activeClusterSignatures() : List.of()));
        }

        if (records.isEmpty()) {
            nextDrainPartition++;
            prepareNextPartitionDrain();
            return;
        }
        awaitingPartitionDrain = List.copyOf(records);
        awaitingPartitionReady = true;
        probeAwaitingPartition();
    }

    private void probeAwaitingPartition() {
        sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(nextDrainPartition))
                .tell(new CMCommand.PartitionDrainReadyProbe(finalization.finalRound(), nextDrainPartition, bucketId,
                        getContext().getSelf()));
        timers.startSingleTimer(RetryDrainProbe.INSTANCE, Duration.ofSeconds(UserConfig.DRAIN_RETRY_SECONDS));
    }

    private void enqueueAwaitingPartitionDrain() {
        drainDispatcher.tell(
                new DrainProtocol.EnqueuePartition(nextDrainPartition, awaitingPartitionDrain, getContext().getSelf()));
        timers.startSingleTimer(RetryDrainProbe.INSTANCE, Duration.ofSeconds(UserConfig.DRAIN_RETRY_SECONDS));
    }

    private Behavior<Command> onRetryDrainProbe() {
        if (awaitingPartitionDrain != null) {
            if (awaitingPartitionReady)
                probeAwaitingPartition();
            else
                enqueueAwaitingPartitionDrain();
        }
        return this;
    }

    private void acknowledge(StoreBatch message) {
        if (message.ackTo() != null)
            message.ackTo().tell(new DirectBatchAggregatorActor.ValueOwnerPersisted(bucketId));
    }

    private static BatchProcessor newProcessor(DataOrientation orientation) {
        return switch (orientation) {
            case COLUMN_MAJOR -> new ColumnMajorProcessor();
            case VALUE_MAJOR -> new ValueMajorProcessor();
        };
    }
}
