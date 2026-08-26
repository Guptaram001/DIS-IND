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
import java.util.Set;
import org.roaringbitmap.RoaringBitmap;

import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;

public final class CandidateManagerActor_ extends AbstractBehavior<CMCommand> {
    public static final EntityTypeKey<CMCommand> TYPE_KEY = EntityTypeKey.create(CMCommand.class,
            "CandidateManagerActor");

    private final int lhsOwnerCol;
    private final DatasetMetadata metadata;
    private final ActorRef<RCCommand> rcRef;

    private final int[] violationCountByRhs;
    private final int[] latestVoSequenceByBucket;
    private final RoaringBitmap drainedValueOwners = new RoaringBitmap();
    private int expectedValueOwnerDrains = -1;
    private int finalRound = -1;
    private boolean finishedReported;
    private long exactComparisonsWithoutPruning;
    private long candidateEvaluationsWithoutPruning;
    private PruneMetrics pruneMetrics = PruneMetrics.empty();
    private long activeClusterEntriesAcrossBuckets;
    private final Set<BitSet> distinctActiveClusterSignatures = new HashSet<>();

    public static Behavior<CMCommand> create(int lhsOwnerCol, ActorRef<RCCommand> rcRef, DatasetMetadata metadata) {
        return Behaviors.setup(ctx -> new CandidateManagerActor_(ctx, lhsOwnerCol, rcRef, metadata));
    }

    private CandidateManagerActor_(ActorContext<CMCommand> context, int lhsOwnerCol, ActorRef<RCCommand> rcRef,
            DatasetMetadata metadata) {
        super(context);
        this.lhsOwnerCol = lhsOwnerCol;
        this.rcRef = rcRef;
        this.metadata = metadata;
        this.violationCountByRhs = new int[metadata.totalCols()];
        Arrays.fill(violationCountByRhs, 0);
        this.latestVoSequenceByBucket = new int[UserConfig.VALUE_OWNER_BUCKETS];
        Arrays.fill(latestVoSequenceByBucket, -1);

        var column = metadata.column(lhsOwnerCol);
        getContext().getLog().info(
                "[PLACEMENT] type=CM col={} tableId={} table={} localCol={} columnName={} qualifiedName={} dataType={} node={}",
                lhsOwnerCol, column.tableId(), token(column.tableName()), column.localColumnId(),
                token(column.columnName()), token(column.qualifiedName()), column.type(),
                Cluster.get(context.getSystem()).selfMember().address());
    }

    private static String token(String value) {
        return value.replaceAll("\\s+", "_");
    }

    @Override
    public Receive<CMCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(CMCommand.ValueOwnerCandidateStatusUpdate.class, this::onCandidateStatusUpdate)
                .onMessage(CMCommand.ValueOwnerDrained.class, this::onValueOwnerDrained)
                .onMessage(CMCommand.DrainReadyProbe.class, this::onDrainReadyProbe)
                .onMessage(CMCommand.OwnersDrained.class, this::onOwnersDrained)
                .onMessage(CMCommand.NoMoreCandidates.class, this::onNoMoreCandidates)
                .build();
    }

    private Behavior<CMCommand> onCandidateStatusUpdate(CMCommand.ValueOwnerCandidateStatusUpdate msg) {
        int bucketId = msg.bucketId();
        if (bucketId < 0 || bucketId >= latestVoSequenceByBucket.length)
            return this;
        if (msg.voSequence() <= latestVoSequenceByBucket[bucketId])
            return this;
        latestVoSequenceByBucket[bucketId] = msg.voSequence();

        for (CandidateLocalStatus status : msg.statuses()) {
            int rhsCol = status.rhsCol();
            if (rhsCol < 0 || rhsCol >= violationCountByRhs.length)
                continue;

            if (status.valid()) {
                if (violationCountByRhs[rhsCol] == 0) {
                    getContext().getLog().warn("Unexpected valid transition: rhs={} bucket={} sequence={}",
                            rhsCol, bucketId, msg.voSequence());
                    continue;
                }
                violationCountByRhs[rhsCol]--;
            } else
                violationCountByRhs[rhsCol]++;
        }
        return this;
    }

    private Behavior<CMCommand> onNoMoreCandidates(CMCommand.NoMoreCandidates msg) {
        finalRound = Math.max(finalRound, msg.finalRound());
        return this;
    }

    private Behavior<CMCommand> onValueOwnerDrained(CMCommand.ValueOwnerDrained msg) {
        if (finalRound >= 0 && msg.finalRound() != finalRound) {
            getContext().getLog().warn("Ignoring stale VO drain round={} currentFinalRound={} bucket={}",
                    msg.finalRound(), finalRound, msg.bucketId());
            return this;
        }
        finalRound = msg.finalRound();
        expectedValueOwnerDrains = msg.expectedBuckets();
        if (!drainedValueOwners.contains(msg.bucketId())) {
            candidateEvaluationsWithoutPruning = Math.addExact(candidateEvaluationsWithoutPruning,
                    msg.candidateEvaluationsWithoutPruning());
            exactComparisonsWithoutPruning = Math.addExact(exactComparisonsWithoutPruning,
                    msg.exactValueProbesWithoutPruning());
            pruneMetrics = pruneMetrics.plus(msg.pruneMetrics());
            activeClusterEntriesAcrossBuckets = Math.addExact(
                    activeClusterEntriesAcrossBuckets, msg.activeClusterSignatures().size());
            for (long[] words : msg.activeClusterSignatures())
                distinctActiveClusterSignatures.add(BitSet.valueOf(words));
        }
        drainedValueOwners.add(msg.bucketId());
        if (drainedValueOwners.getCardinality() == expectedValueOwnerDrains)
            reportFinished();
        return this;
    }

    private Behavior<CMCommand> onDrainReadyProbe(CMCommand.DrainReadyProbe msg) {
        msg.replyTo().tell(new disIND.valueBased.protocol.ValueOwnerProtocol.CandidateManagerReady(
                msg.finalRound(), lhsOwnerCol, msg.bucketId()));
        return this;
    }

    private Behavior<CMCommand> onOwnersDrained(CMCommand.OwnersDrained message) {
        var batch = message.batch();
        for (var record : batch.owners()) {
            if (record.lhsCol() != lhsOwnerCol) {
                getContext().getLog().warn("Ignoring misrouted drain batch={} lhs={} expectedLhs={}",
                        batch.batchId(), record.lhsCol(), lhsOwnerCol);
                continue;
            }
            onValueOwnerDrained(new CMCommand.ValueOwnerDrained(record.finalRound(), record.bucketId(),
                    record.expectedBuckets(), record.locallyRejectedRhs(),
                    record.candidateEvaluationsWithoutPruning(), record.exactValueProbesWithoutPruning(),
                    record.pruneMetrics(), record.activeClusterSignatures()));
        }
        batch.replyTo().tell(new disIND.valueBased.protocol.DrainProtocol.BatchAcknowledged(
                batch.batchId(), lhsOwnerCol));
        return this;
    }

    private void reportFinished() {
        if (finishedReported)
            return;
        finishedReported = true;

        List<UnaryPair> clean = new ArrayList<>();
        for (int rhsCol = 0; rhsCol < violationCountByRhs.length; rhsCol++) {
            if (rhsCol != lhsOwnerCol && testCompatibility(metadata.typeOf(lhsOwnerCol), metadata.typeOf(rhsCol))
                    && violationCountByRhs[rhsCol] == 0) {
                clean.add(new UnaryPair(lhsOwnerCol, rhsCol));
            }
        }

        rcRef.tell(new RCCommand.CmDiscoveryComplete(lhsOwnerCol, finalRound,
                List.copyOf(clean), List.<NaryPair>of(),
                candidateEvaluationsWithoutPruning, exactComparisonsWithoutPruning, pruneMetrics,
                activeClusterEntriesAcrossBuckets,
                distinctActiveClusterSignatures.stream()
                        .map(BitSet::toLongArray)
                        .toList()));

        if (Debug.INTERNAL) {
            getContext().getLog().info("[CM-DRAINED] lhs={} finalRound={} valueOwners={}/{} cleanCandidates={}",
                    lhsOwnerCol, finalRound, drainedValueOwners.getCardinality(),
                    expectedValueOwnerDrains, clean.size());
        }
    }

}
