package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import disIND.valueBased.utility.UserConfig;
import disIND.valueBased.model.SharedModel.CMCommand;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import disIND.valueBased.model.SharedModel.NaryPair;
import disIND.valueBased.model.SharedModel.PruneMetrics;
import disIND.valueBased.model.SharedModel.RCCommand;
import disIND.valueBased.model.SharedModel.UnaryPair;
import disIND.valueBased.utility.Debug;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.BitSet;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.roaringbitmap.RoaringBitmap;

import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;

public final class CandidateManagerActor_ extends AbstractBehavior<CMCommand> {
    public static final EntityTypeKey<CMCommand> TYPE_KEY = EntityTypeKey.create(CMCommand.class,
            "CandidateManagerActor");

    private final int partitionId;
    private final DatasetMetadata metadata;
    private final ActorRef<RCCommand> rcRef;
    private final Map<Integer, LhsState> states = new HashMap<>();

    private static final class LhsState {
        final int lhsCol;
        final int[] violationCountByRhs;
        final int[] latestVoSequenceByBucket;
        final RoaringBitmap drainedValueOwners = new RoaringBitmap();
        int expectedValueOwnerDrains = -1;
        int finalRound = -1;
        boolean finishedReported;
        long exactComparisonsWithoutPruning;
        long candidateEvaluationsWithoutPruning;
        PruneMetrics pruneMetrics = PruneMetrics.empty();
        long activeClusterEntriesAcrossBuckets;
        final Set<BitSet> distinctActiveClusterSignatures = new HashSet<>();

        LhsState(int lhsCol, int totalCols) {
            this.lhsCol = lhsCol;
            this.violationCountByRhs = new int[totalCols];
            this.latestVoSequenceByBucket = new int[UserConfig.VALUE_OWNER_BUCKETS];
            Arrays.fill(latestVoSequenceByBucket, -1);
        }
    }

    public static Behavior<CMCommand> create(int partitionId, ActorRef<RCCommand> rcRef, DatasetMetadata metadata) {
        return Behaviors.setup(ctx -> new CandidateManagerActor_(ctx, partitionId, rcRef, metadata));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> context, int partitionId, ActorRef<RCCommand> rcRef,
            DatasetMetadata metadata) {
        super(context);
        this.partitionId = partitionId;
        this.rcRef = rcRef;
        this.metadata = metadata;
        getContext().getLog().info(
                "[PLACEMENT] type=CM partition={} node={}", partitionId,
                Cluster.get(context.getSystem()).selfMember().address());
    }

    @Override
    public Receive<CMCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CMCommand.ValueOwnerCandidateStatusUpdate.class, this::onCandidateStatusUpdate)
                .onMessage(CMCommand.DrainReadyProbe.class, this::onDrainReadyProbe)
                .onMessage(CMCommand.OwnersDrained.class, this::onOwnersDrained)
                .onMessage(CMCommand.NoMoreCandidates.class, this::onNoMoreCandidates)
                .build();
    }

    private Behavior<CMCommand> onCandidateStatusUpdate(CMCommand.ValueOwnerCandidateStatusUpdate msg) {
        LhsState state = stateFor(msg.lhsCol());
        int bucketId = msg.bucketId();
        if (bucketId < 0 || bucketId >= state.latestVoSequenceByBucket.length)
            return this;
        if (msg.voSequence() <= state.latestVoSequenceByBucket[bucketId])
            return this;
        state.latestVoSequenceByBucket[bucketId] = msg.voSequence();

        for (CandidateLocalStatus status : msg.statuses()) {
            int rhsCol = status.rhsCol();
            if (rhsCol < 0 || rhsCol >= state.violationCountByRhs.length)
                continue;

            if (status.valid()) {
                if (state.violationCountByRhs[rhsCol] == 0) {
                    getContext().getLog().warn("Unexpected valid transition: rhs={} bucket={} sequence={}",
                            rhsCol, bucketId, msg.voSequence());
                    continue;
                }
                state.violationCountByRhs[rhsCol]--;
            } else
                state.violationCountByRhs[rhsCol]++;
        }
        return this;
    }

    private Behavior<CMCommand> onNoMoreCandidates(CMCommand.NoMoreCandidates msg) {
        for (LhsState state : states.values())
            state.finalRound = Math.max(state.finalRound, msg.finalRound());
        return this;
    }

    private void onValueOwnerDrained(int lhsCol, CMCommand.ValueOwnerDrained msg) {
        LhsState state = stateFor(lhsCol);
        if (state.finalRound >= 0 && msg.finalRound() != state.finalRound) {
            getContext().getLog().warn("Ignoring stale VO drain round={} currentFinalRound={} bucket={}",
                    msg.finalRound(), state.finalRound, msg.bucketId());
            return;
        }
        state.finalRound = msg.finalRound();
        state.expectedValueOwnerDrains = msg.expectedBuckets();
        if (!state.drainedValueOwners.contains(msg.bucketId())) {
            state.candidateEvaluationsWithoutPruning = Math.addExact(state.candidateEvaluationsWithoutPruning,
                    msg.candidateEvaluationsWithoutPruning());
            state.exactComparisonsWithoutPruning = Math.addExact(state.exactComparisonsWithoutPruning,
                    msg.exactValueProbesWithoutPruning());
            state.pruneMetrics = state.pruneMetrics.plus(msg.pruneMetrics());
            state.activeClusterEntriesAcrossBuckets = Math.addExact(
                    state.activeClusterEntriesAcrossBuckets, msg.activeClusterSignatures().size());
            for (long[] words : msg.activeClusterSignatures())
                state.distinctActiveClusterSignatures.add(BitSet.valueOf(words));
        }
        state.drainedValueOwners.add(msg.bucketId());
        if (state.drainedValueOwners.getCardinality() == state.expectedValueOwnerDrains)
            reportFinished(state);
    }

    private Behavior<CMCommand> onDrainReadyProbe(CMCommand.DrainReadyProbe msg) {
        stateFor(msg.lhsCol());
        msg.replyTo().tell(new disIND.valueBased.protocol.ValueOwnerProtocol.CandidateManagerReady(
                msg.finalRound(), msg.lhsCol(), msg.bucketId()));
        return this;
    }

    private Behavior<CMCommand> onOwnersDrained(CMCommand.OwnersDrained message) {
        var batch = message.batch();
        int acknowledgedLhs = batch.owners().get(0).lhsCol();
        for (var record : batch.owners()) {
            if (CMCommand.partitionFor(record.lhsCol(), UserConfig.DEFAULT_CM_PARTITIONS) != partitionId) {
                getContext().getLog().warn("Ignoring misrouted drain batch={} lhs={} partition={}",
                        batch.batchId(), record.lhsCol(), partitionId);
                continue;
            }
            onValueOwnerDrained(record.lhsCol(), new CMCommand.ValueOwnerDrained(record.finalRound(), record.bucketId(),
                    record.expectedBuckets(), record.locallyRejectedRhs(),
                    record.candidateEvaluationsWithoutPruning(), record.exactValueProbesWithoutPruning(),
                    record.pruneMetrics(), record.activeClusterSignatures()));
        }
        batch.replyTo().tell(new disIND.valueBased.protocol.DrainProtocol.BatchAcknowledged(
                batch.batchId(), acknowledgedLhs));
        return this;
    }

    private void reportFinished(LhsState state) {
        if (state.finishedReported)
            return;
        state.finishedReported = true;

        List<UnaryPair> clean = new ArrayList<>();
        for (int rhsCol = 0; rhsCol < state.violationCountByRhs.length; rhsCol++) {
            if (rhsCol != state.lhsCol && testCompatibility(metadata.typeOf(state.lhsCol), metadata.typeOf(rhsCol))
                    && state.violationCountByRhs[rhsCol] == 0) {
                clean.add(new UnaryPair(state.lhsCol, rhsCol));
            }
        }

        rcRef.tell(new RCCommand.CmDiscoveryComplete(state.lhsCol, state.finalRound,
                List.copyOf(clean), List.<NaryPair>of(),
                state.candidateEvaluationsWithoutPruning, state.exactComparisonsWithoutPruning, state.pruneMetrics,
                state.activeClusterEntriesAcrossBuckets,
                state.distinctActiveClusterSignatures.stream()
                        .map(BitSet::toLongArray)
                        .toList()));

        if (Debug.INTERNAL) {
            getContext().getLog().info(
                    "[CM-DRAINED] partition={} lhs={} finalRound={} valueOwners={}/{} cleanCandidates={}",
                    partitionId, state.lhsCol, state.finalRound, state.drainedValueOwners.getCardinality(),
                    state.expectedValueOwnerDrains, clean.size());
        }
    }

    private LhsState stateFor(int lhsCol) {
        int expectedPartition = CMCommand.partitionFor(lhsCol, UserConfig.DEFAULT_CM_PARTITIONS);
        if (expectedPartition != partitionId)
            throw new IllegalArgumentException("LHS " + lhsCol + " belongs to partition " + expectedPartition
                    + ", not " + partitionId);
        return states.computeIfAbsent(lhsCol, lhs -> new LhsState(lhs, metadata.totalCols()));
    }

}
