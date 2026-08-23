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
import disIND.valueBased.protocol.ValueOwnerProtocol.BucketSnapshot;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnCount;
import disIND.valueBased.protocol.ValueOwnerProtocol.Command;
import disIND.valueBased.protocol.ValueOwnerProtocol.FinalizeMembership;
import disIND.valueBased.protocol.ValueOwnerProtocol.GetBucket;
import disIND.valueBased.protocol.ValueOwnerProtocol.StoreBatch;
import disIND.valueBased.protocol.ValueOwnerProtocol.CandidateManagerReady;
import disIND.valueBased.protocol.ValueOwnerProtocol.RetryDrainProbe;
import disIND.valueBased.protocol.ValueOwnerProtocol.DrainQueued;
import disIND.valueBased.protocol.DrainProtocol;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.WorkerValueIdStore;
import disIND.valueBased.tracking.CandidateViolationAfterApplyingUpdates;
import disIND.valueBased.tracking.CandidateEvaluator;
import disIND.valueBased.tracking.TrackingResult;
import disIND.valueBased.tracking.ModeSpecificContext;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2BooleanLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;
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
    private int statusSequence;
    private final ActorRef<DrainProtocol.Command> drainDispatcher;
    private final TimerScheduler<Command> timers;
    private FinalizeMembership finalization;
    private int nextDrainLhs;
    private DrainProtocol.DrainRecord awaitingDrainAdmission;

    public static String entityId(int bucketId) {
        return "value-bucket-" + bucketId;
    }

    public static Behavior<Command> create(
            String entityId,
            ClusterSharding sharding,
            DatasetMetadata metadata,
            ValueOwnerMembershipStore membershipStore,
            WorkerValueIdStore valueIdStore,
            DataOrientation orientation,
            CandidateTrackingMode candidateTracking,
            ActorRef<DrainProtocol.Command> drainDispatcher) {
        return Behaviors.withTimers(timers -> Behaviors.setup(ctx -> new ValueOwnerActor(
                ctx,
                entityId,
                sharding,
                metadata,
                membershipStore,
                valueIdStore,
                orientation,
                candidateTracking, drainDispatcher, timers)));
    }

    private ValueOwnerActor(
            ActorContext<Command> context,
            String entityId,
            ClusterSharding sharding,
            DatasetMetadata metadata,
            ValueOwnerMembershipStore membershipStore,
            WorkerValueIdStore valueIdStore,
            DataOrientation orientation,
            CandidateTrackingMode candidateTrackingMode,
            ActorRef<DrainProtocol.Command> drainDispatcher,
            TimerScheduler<Command> timers) {
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

        ColumnSetFactory columnSets = new ColumnSetFactory(metadata.totalCols());
        this.membershipUpdater = new MembershipUpdater(bucketId, membershipStore,
                columnSets, modeSpecificContext);
        this.candidateEvaluator = new CandidateEvaluator(metadata, columnSets, modeSpecificContext);

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
                .onMessage(GetBucket.class, this::onGetBucket)
                .onMessage(FinalizeMembership.class, this::onFinalizeMembership)
                .onMessage(CandidateManagerReady.class, this::onCandidateManagerReady)
                .onMessage(DrainQueued.class, this::onDrainQueued)
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
        MembershipUpdates updates = batchProcessor.process(bucketId, message.body(), valueIdStore);
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
        candidateEvaluator.evaluate(membership.newlyAddedColumnsByValue(),
                membership.updatedRecordsByValue(), candidateViolationAfterApplyingUpdates);
        TrackingResult trackingResult = modeSpecificContext.tracker().apply(candidateViolationAfterApplyingUpdates,
                membership.updatedRecordsByValue(), membershipStore);

        membershipStore.writeBatch(bucketId, membership.updatedRecordsByValue(), trackingResult.changedStates());
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
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsCol))
                    .tell(new CMCommand.ValueOwnerCandidateStatusUpdate(
                            batch.epoch(), voSequence, batch.round(), bucketId, transitions));
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
        finalization = message;
        nextDrainLhs = 0;
        probeNextCandidateManager();
        return this;
    }

    private Behavior<Command> onCandidateManagerReady(CandidateManagerReady message) {
        if (finalization == null || message.finalRound() != finalization.finalRound()
                || message.bucketId() != bucketId || message.lhsCol() != nextDrainLhs)
            return this;
        timers.cancel(RetryDrainProbe.INSTANCE);
        int lhs = nextDrainLhs;
        awaitingDrainAdmission = new DrainProtocol.DrainRecord(
                finalization.finalRound(), lhs, bucketId, finalization.expectedBuckets(), new RoaringBitmap(),
                candidateEvaluator.candidateEvaluationsFor(lhs), candidateEvaluator.exactComparisonsFor(lhs),
                modeSpecificContext.metricsFor(lhs));
        enqueueAwaitingDrain();
        return this;
    }

    private Behavior<Command> onDrainQueued(DrainQueued message) {
        if (awaitingDrainAdmission == null || message.finalRound() != awaitingDrainAdmission.finalRound()
                || message.lhsCol() != awaitingDrainAdmission.lhsCol() || message.bucketId() != bucketId)
            return this;
        timers.cancel(RetryDrainProbe.INSTANCE);
        awaitingDrainAdmission = null;
        nextDrainLhs++;
        if (nextDrainLhs < finalization.totalColumns())
            probeNextCandidateManager();
        else if (Debug.INTERNAL)
            getContext().getLog().info(
                    "[VO] bucket={} finalRound={} queuedCms={} localRejectedCandidates={} finalStatus=ack-only",
                    bucketId, finalization.finalRound(), finalization.totalColumns(),
                    modeSpecificContext.locallyRejectedCount());
        return this;
    }

    private Behavior<Command> onRetryDrainProbe() {
        if (awaitingDrainAdmission == null)
            probeNextCandidateManager();
        else
            enqueueAwaitingDrain();
        return this;
    }

    private void probeNextCandidateManager() {
        if (finalization == null || nextDrainLhs >= finalization.totalColumns())
            return;
        sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(nextDrainLhs))
                .tell(new CMCommand.DrainReadyProbe(finalization.finalRound(), nextDrainLhs, bucketId,
                        getContext().getSelf()));
        timers.startSingleTimer(RetryDrainProbe.INSTANCE, Duration.ofSeconds(UserConfig.DRAIN_RETRY_SECONDS));
    }

    private void enqueueAwaitingDrain() {
        drainDispatcher.tell(new DrainProtocol.Enqueue(awaitingDrainAdmission, getContext().getSelf()));
        timers.startSingleTimer(RetryDrainProbe.INSTANCE, Duration.ofSeconds(UserConfig.DRAIN_RETRY_SECONDS));
    }

    private Behavior<Command> onGetBucket(GetBucket message) {
        Int2ObjectMap<Int2IntMap> bucket = membershipStore.snapshotBucket(bucketId);
        Int2ObjectMap<List<ColumnCount>> snapshot = new Int2ObjectOpenHashMap<>(bucket.size());

        ObjectIterator<Int2ObjectMap.Entry<Int2IntMap>> valueIterator = Int2ObjectMaps.fastIterator(bucket);
        while (valueIterator.hasNext()) {
            Int2ObjectMap.Entry<Int2IntMap> valueEntry = valueIterator.next();
            int valueId = valueEntry.getIntKey();
            Int2IntMap columns = valueEntry.getValue();
            List<ColumnCount> columnCounts = new ArrayList<>(columns.size());
            ObjectIterator<Int2IntMap.Entry> columnIterator = Int2IntMaps.fastIterator(columns);
            while (columnIterator.hasNext()) {
                Int2IntMap.Entry columnEntry = columnIterator.next();
                columnCounts.add(new ColumnCount(columnEntry.getIntKey(), columnEntry.getIntValue()));
            }

            snapshot.put(valueId, List.copyOf(columnCounts));
        }
        message.replyTo().tell(new BucketSnapshot(bucketId, Map.copyOf(snapshot)));
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
