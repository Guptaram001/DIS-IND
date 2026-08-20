package disIND.valueBased.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
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
import disIND.valueBased.model.SharedModel.ColumnUpdates;
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
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.WorkerValueIdStore;
import disIND.valueBased.tracking.CandidateViolationAfterApplyingUpdates;
import disIND.valueBased.tracking.CandidateEvaluator;
import disIND.valueBased.tracking.TrackingResult;
import disIND.valueBased.tracking.ModeSpecificContext;
import disIND.valueBased.tracking.CandidateViolationAfterApplyingUpdates;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private final Map<String, Boolean> resolvedBatches;
    private final DataOrientation orientation;
    private final BatchProcessor batchProcessor;
    private int statusSequence;

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
            CandidateTrackingMode candidateTracking) {
        return Behaviors.setup(ctx -> new ValueOwnerActor(
                ctx,
                entityId,
                sharding,
                metadata,
                membershipStore,
                valueIdStore,
                orientation,
                candidateTracking));
    }

    private ValueOwnerActor(
            ActorContext<Command> context,
            String entityId,
            ClusterSharding sharding,
            DatasetMetadata metadata,
            ValueOwnerMembershipStore membershipStore,
            WorkerValueIdStore valueIdStore,
            DataOrientation orientation,
            CandidateTrackingMode candidateTrackingMode) {
        super(context);
        this.entityId = entityId;
        this.bucketId = Integer.parseInt(entityId.substring("value-bucket-".length()));
        this.sharding = sharding;
        this.membershipStore = membershipStore;
        this.valueIdStore = valueIdStore;
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

        this.resolvedBatches = new LinkedHashMap<>(recentBatchLimit, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > ValueOwnerActor.this.recentBatchLimit;
            }
        };

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

        String batchKey = message.tableId() + ":" + message.batchId();
        // Message already handled.
        if (resolvedBatches.containsKey(batchKey)) {
            acknowledge(message);
            return this;
        }

        // handles the incoming message into valueId-colId-count for both cases as
        // deltas
        MembershipUpdates updates = batchProcessor.process(bucketId, message.body(), valueIdStore);
        statusSequence = Math.incrementExact(statusSequence);

        applyUpdates(message, updates, statusSequence);
        resolvedBatches.put(batchKey, Boolean.TRUE);
        acknowledge(message);
        return this;
    }

    private void applyUpdates(StoreBatch message, MembershipUpdates updates, int voSequence) {
        if (updates instanceof ValueUpdates valueUpdates) {
            applyValueUpdates(message, valueUpdates.byValue(), voSequence);
            return;
        }

        if (updates instanceof ColumnUpdates columnUpdates) {
            applyValueUpdates(message, columnUpdates.byValue(), voSequence);
            return;
        }

        throw new IllegalArgumentException(
                "Unsupported membership update type: " + updates.getClass().getName());
    }

    private void applyValueUpdates(StoreBatch message, Map<Integer, Int2IntMap> updates,
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

    private void sendCandidateStatusTransitions(
            StoreBatch batch, int voSequence, Map<Integer, List<CandidateLocalStatus>> transitionsByLhs,
            int affectedCandidateCount) {
        transitionsByLhs.forEach((lhsCol, transitions) -> {
            if (transitions.isEmpty())
                return;
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsCol))
                    .tell(new CMCommand.ValueOwnerCandidateStatusUpdate(
                            batch.epoch(), voSequence, batch.round(), bucketId, transitions));
        });

        if (Debug.INTERNAL) {
            int transitionCount = transitionsByLhs.values().stream().mapToInt(List::size).sum();
            getContext().getLog().info(
                    "[VO] round={} bucket={} epoch={} voSequence={} affectedCandidates={} transitions={} affectedCms={}",
                    batch.round(), bucketId, batch.epoch(), voSequence, affectedCandidateCount,
                    transitionCount, transitionsByLhs.size());
        }
    }

    private Behavior<Command> onFinalizeMembership(FinalizeMembership message) {
        RoaringBitmap emptyFinalStatus = new RoaringBitmap();
        for (int lhsCol = 0; lhsCol < message.totalColumns(); lhsCol++) {
            sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(lhsCol))
                    .tell(new CMCommand.ValueOwnerDrained(message.finalRound(), bucketId,
                            message.expectedBuckets(), emptyFinalStatus,
                            candidateEvaluator.candidateEvaluationsFor(lhsCol),
                            candidateEvaluator.exactComparisonsFor(lhsCol),
                            modeSpecificContext.metricsFor(lhsCol)));
        }

        if (Debug.INTERNAL) {
            getContext().getLog().info(
                    "[VO] bucket={} finalRound={} notifiedCms={} localRejectedCandidates={} finalStatus=ack-only",
                    bucketId, message.finalRound(), message.totalColumns(),
                    modeSpecificContext.locallyRejectedCount());
        }
        return this;
    }

    private Behavior<Command> onGetBucket(GetBucket message) {
        Map<Integer, List<ColumnCount>> snapshot = new HashMap<>();
        membershipStore.snapshotBucket(bucketId).forEach((valueId, columns) -> {
            List<ColumnCount> columnCounts = columns.entrySet().stream()
                    .map(entry -> new ColumnCount(entry.getKey(), entry.getValue()))
                    .toList();
            snapshot.put(valueId, columnCounts);
        });
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
