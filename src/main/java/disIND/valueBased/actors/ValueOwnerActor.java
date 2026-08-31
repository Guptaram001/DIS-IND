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
import disIND.valueBased.protocol.ValueOwnerProtocol.MembershipWriteAcknowledged;
import disIND.valueBased.protocol.ValueOwnerProtocol.MembershipWriteFailed;
import disIND.valueBased.protocol.ValueOwnerProtocol.RetryMembershipWrite;
import disIND.valueBased.protocol.DrainProtocol;
import disIND.valueBased.protocol.MembershipWriteProtocol;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.ValueOwnerMembershipStore.InFlightWrite;
import disIND.valueBased.structures.ValueOwnerMembershipStore.PreparedWriteBatch;
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
import java.util.LinkedHashMap;
import java.util.Map;
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
    private final int[] statusSequenceByPartition = new int[UserConfig.DEFAULT_CM_PARTITIONS];
    private final ActorRef<DrainProtocol.Command> drainDispatcher;
    private final ActorRef<MembershipWriteProtocol.Command> membershipWriter;
    private final TimerScheduler<Command> timers;
    private FinalizeMembership finalization;
    private int nextDrainPartition;
    private List<DrainProtocol.DrainRecord> awaitingPartitionDrain;
    private boolean awaitingPartitionReady;
    private final WorkerPhaseMetrics phaseMetrics;
    private final WorkerMetricsFlusher workerMetricsFlusher;
    private int awaitingPartitionFinalSequence = -1;
    private long nextMembershipBatchId;
    private InFlightWrite inFlightWrite;
    static final class DelayedInputAcknowledgments {
        private record InputBatchKey(long epoch, int tableId, int batchId) {
        }

        private final Map<InputBatchKey, ActorRef<DirectBatchAggregatorActor.Command>> acknowledgments =
                new LinkedHashMap<>();

        boolean isEmpty() {
            return acknowledgments.isEmpty();
        }

        void add(StoreBatch message) {
            acknowledgments.putIfAbsent(new InputBatchKey(message.epoch(), message.tableId(), message.batchId()),
                    message.ackTo());
        }

        void release(int bucketId) {
            for (ActorRef<DirectBatchAggregatorActor.Command> acknowledgment : acknowledgments.values())
                acknowledgment.tell(new DirectBatchAggregatorActor.ValueOwnerPersisted(bucketId));
            acknowledgments.clear();
        }
    }

    private final DelayedInputAcknowledgments delayedInputAcknowledgments = new DelayedInputAcknowledgments();

    public static String entityId(int bucketId) {
        return "value-bucket-" + bucketId;
    }

    public static Behavior<Command> create(
            String entityId, ClusterSharding sharding, DatasetMetadata metadata,
            ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore, DataOrientation orientation,
            CandidateTrackingMode candidateTracking, ActorRef<DrainProtocol.Command> drainDispatcher,
            ActorRef<MembershipWriteProtocol.Command> membershipWriter,
            CandidateDomain candidateDomain, WorkerPhaseMetrics phaseMetrics,
            WorkerMetricsFlusher workerMetricsFlusher) {
        return Behaviors.withTimers(timers -> Behaviors.setup(ctx -> new ValueOwnerActor(
                ctx, entityId, sharding, metadata, membershipStore, valueIdStore, orientation,
                candidateTracking, drainDispatcher, membershipWriter, timers, candidateDomain, phaseMetrics,
                workerMetricsFlusher)));
    }

    private ValueOwnerActor(ActorContext<Command> context, String entityId, ClusterSharding sharding,
            DatasetMetadata metadata, ValueOwnerMembershipStore membershipStore, WorkerValueIdStore valueIdStore,
            DataOrientation orientation, CandidateTrackingMode candidateTrackingMode,
            ActorRef<DrainProtocol.Command> drainDispatcher,
            ActorRef<MembershipWriteProtocol.Command> membershipWriter, TimerScheduler<Command> timers,
            CandidateDomain candidateDomain, WorkerPhaseMetrics phaseMetrics,
            WorkerMetricsFlusher workerMetricsFlusher) {
        super(context);
        this.entityId = entityId;
        this.bucketId = Integer.parseInt(entityId.substring("value-bucket-".length()));
        this.sharding = sharding;
        this.membershipStore = membershipStore;
        this.valueIdStore = valueIdStore;
        this.drainDispatcher = Objects.requireNonNull(drainDispatcher, "drainDispatcher");
        this.membershipWriter = Objects.requireNonNull(membershipWriter, "membershipWriter");
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
                .onMessage(MembershipWriteAcknowledged.class, this::onMembershipWriteAcknowledged)
                .onMessage(MembershipWriteFailed.class, this::onMembershipWriteFailed)
                .onMessageEquals(RetryMembershipWrite.INSTANCE, this::onRetryMembershipWrite)
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

        applyUpdates(message, updates);
        resolvedBatches.putAndMoveToLast(batchKey, true);
        if (resolvedBatches.size() > recentBatchLimit) {
            resolvedBatches.removeFirstBoolean();
        }
        acknowledge(message);
        return this;
    }

    private void applyUpdates(StoreBatch message, MembershipUpdates updates) {
        if (updates instanceof ValueUpdates valueUpdates) {
            applyValueUpdates(message, valueUpdates.byValue());
            return;
        }

        throw new IllegalArgumentException(
                "Unsupported membership update type: " + updates.getClass().getName());
    }

    private void applyValueUpdates(StoreBatch message, Int2ObjectMap<Int2IntMap> updates) {
        // Applies the updates to the store and obtains the final changes merging to
        // previous records with added/removed columns
        MembershipBatchResult membership = membershipUpdater.apply(updates);

        // Selects the specific mode changes to handle the violation further.
        CandidateViolationAfterApplyingUpdates candidateViolationAfterApplyingUpdates = modeSpecificContext
                .tracker().newChanges(bucketId);
        // Find which IND pairs might have been changed.
        long started = System.nanoTime();
        candidateEvaluator.evaluate(membership.updatedRecordsByValue(),
                membership.newlyAddedColumnsByValue(), membership.newlyRemovedColumnsByValue(),
                candidateViolationAfterApplyingUpdates);
        phaseMetrics.record(Phase.CANDIDATE_EVALUATION, System.nanoTime() - started);

        started = System.nanoTime();
        TrackingResult trackingResult = modeSpecificContext.tracker().apply(candidateViolationAfterApplyingUpdates,
                membership.updatedRecordsByValue(), membershipStore);
        phaseMetrics.record(Phase.TRACKER_RESOLUTION, System.nanoTime() - started);

        started = System.nanoTime();
        membershipStore.stage(bucketId, membership.updatedRecordsByValue(), trackingResult.changedStates());
        phaseMetrics.record(Phase.ROCKSDB_WRITE, System.nanoTime() - started);
        tryStartMembershipWrite();

        modeSpecificContext.candidateStatesChanged(trackingResult.changedStates());

        sendCandidateStatusTransitions(message, trackingResult.transitionsByLhs(),
                trackingResult.changedStates().size());
    }

    private void sendCandidateStatusTransitions(StoreBatch batch,
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
            int partitionSequence = Math.incrementExact(statusSequenceByPartition[cmPartition]);
            statusSequenceByPartition[cmPartition] = partitionSequence;
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(cmPartition))
                    .tell(new CMCommand.ValueOwnerCandidateStatusUpdate(
                            lhsCol, batch.epoch(), partitionSequence, batch.round(), bucketId, transitions));
        }
        if (Debug.INTERNAL) {
            getContext().getLog().info(
                    "[VO] round={} bucket={} epoch={} affectedCandidates={} transitions={} affectedCms={}",
                    batch.round(), bucketId, batch.epoch(), affectedCandidateCount,
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
        tryStartMembershipWrite();
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
        awaitingPartitionFinalSequence = -1;
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

        awaitingPartitionFinalSequence = statusSequenceByPartition[nextDrainPartition];
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
                        awaitingPartitionFinalSequence, getContext().getSelf()));
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

    private void tryStartMembershipWrite() {
        if (inFlightWrite != null)
            return;
        long batchId = ++nextMembershipBatchId;
        PreparedWriteBatch batch = membershipStore.prepareWriteBatch(bucketId, batchId,
                UserConfig.DEFAULT_VO_WRITE_BATCH_MAX_ENTRIES,
                UserConfig.DEFAULT_VO_WRITE_BATCH_MAX_BYTES, getContext().getSelf());
        if (batch.isEmpty())
            return;
        inFlightWrite = batch.cleanup();
        membershipWriter.tell(batch.message());
    }

    private Behavior<Command> onMembershipWriteAcknowledged(MembershipWriteAcknowledged message) {
        if (!matchesInFlight(message.bucketId(), message.batchId()))
            return this;
        membershipStore.acknowledgeWrite(bucketId, inFlightWrite);
        inFlightWrite = null;
        releaseDelayedInputAcknowledgmentIfPossible();
        tryStartMembershipWrite();
        return this;
    }

    private Behavior<Command> onMembershipWriteFailed(MembershipWriteFailed message) {
        if (!matchesInFlight(message.bucketId(), message.batchId()))
            return this;
        getContext().getLog().warn("VO membership write failed; retrying bucket={} batchId={} reason={}",
                bucketId, message.batchId(), message.reason());
        membershipStore.failWrite(bucketId, inFlightWrite);
        inFlightWrite = null;
        timers.startSingleTimer(RetryMembershipWrite.INSTANCE,
                Duration.ofMillis(UserConfig.DEFAULT_VO_WRITE_RETRY_DELAY_MS));
        return this;
    }

    private boolean matchesInFlight(int messageBucketId, long batchId) {
        return messageBucketId == bucketId && inFlightWrite != null && inFlightWrite.batchId() == batchId;
    }

    private Behavior<Command> onRetryMembershipWrite() {
        releaseDelayedInputAcknowledgmentIfPossible();
        tryStartMembershipWrite();
        if (!delayedInputAcknowledgments.isEmpty())
            timers.startSingleTimer(RetryMembershipWrite.INSTANCE,
                    Duration.ofMillis(UserConfig.DEFAULT_VO_WRITE_RETRY_DELAY_MS));
        return this;
    }

    private void releaseDelayedInputAcknowledgmentIfPossible() {
        if (delayedInputAcknowledgments.isEmpty()
                || membershipStore.pinnedEstimatedBytes() > UserConfig.DEFAULT_VO_PINNED_LOW_WATERMARK_BYTES)
            return;
        delayedInputAcknowledgments.release(bucketId);
    }

    private void acknowledge(StoreBatch message) {
        if (message.ackTo() == null)
            return;
        if (!delayedInputAcknowledgments.isEmpty()
                || membershipStore.pinnedEstimatedBytes() >= UserConfig.DEFAULT_VO_PINNED_HIGH_WATERMARK_BYTES) {
            delayedInputAcknowledgments.add(message);
            timers.startSingleTimer(RetryMembershipWrite.INSTANCE,
                    Duration.ofMillis(UserConfig.DEFAULT_VO_WRITE_RETRY_DELAY_MS));
            return;
        }
        message.ackTo().tell(new DirectBatchAggregatorActor.ValueOwnerPersisted(bucketId));
    }

    private static BatchProcessor newProcessor(DataOrientation orientation) {
        return switch (orientation) {
            case COLUMN_MAJOR -> new ColumnMajorProcessor();
            case VALUE_MAJOR -> new ValueMajorProcessor();
        };
    }
}
