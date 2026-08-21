package disIND.valueBased.tracking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.PruneMetricsCollector;
import disIND.valueBased.structures.ValueOwnerClusterIndex;
import disIND.valueBased.structures.ValueOwnerCqf;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.PruneState;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

public final class PruneCandidateTracker implements CandidateTracker {
    private final ValueOwnerCqf cqf;
    private final ValueOwnerClusterIndex clusters;
    private final int[] localDistinctCounts;
    private final int[][] localDistinctCountsByPartition;
    private final PruneMetricsCollector metrics;

    public PruneCandidateTracker(ValueOwnerCqf cqf, ValueOwnerClusterIndex clusters,
            int[] localDistinctCounts, int[][] localDistinctCountsByPartition,
            PruneMetricsCollector metrics) {
        this.cqf = cqf;
        this.clusters = Objects.requireNonNull(clusters, "clusters");
        this.localDistinctCounts = Objects.requireNonNull(localDistinctCounts, "localDistinctCounts");
        this.localDistinctCountsByPartition = localDistinctCountsByPartition;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        if (localDistinctCountsByPartition == null)
            return;
        if (localDistinctCountsByPartition.length != localDistinctCounts.length)
            throw new IllegalArgumentException("Partition-count columns must match distinct-count columns");
        int partitionCount = -1;
        for (int[] counts : localDistinctCountsByPartition) {
            Objects.requireNonNull(counts, "partition count row");
            if (counts.length == 0 || (counts.length & (counts.length - 1)) != 0)
                throw new IllegalArgumentException("Partition count must be a positive power of two");
            if (partitionCount < 0)
                partitionCount = counts.length;
            else if (counts.length != partitionCount)
                throw new IllegalArgumentException("Every column must use the same partition count");
        }
    }

    private static final class PruneDelta {
        private boolean violationCreated;

        private boolean hasCreatedViolation() {
            return violationCreated;
        }
    }

    private static final class PruneChanges implements CandidateViolationAfterApplyingUpdates {
        private final int bucketId;
        private final Map<CandidateKey, PruneDelta> deltas = new HashMap<>();

        private PruneChanges(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(int lhsCol, int rhsCol, int valueId) {
            delta(lhsCol, rhsCol).violationCreated = true;
        }

        @Override
        public void violationRepaired(int lhsCol, int rhsCol, int valueId) {
            delta(lhsCol, rhsCol);
        }

        private PruneDelta delta(int lhsCol, int rhsCol) {
            CandidateKey key = new CandidateKey(bucketId, lhsCol, rhsCol);
            return deltas.computeIfAbsent(key, ignored -> new PruneDelta());
        }
    }

    @Override
    public CandidateViolationAfterApplyingUpdates newChanges(int bucketId) {
        return new PruneChanges(bucketId);
    }

    @Override
    public TrackingResult apply(CandidateViolationAfterApplyingUpdates changes,
            Map<Integer, Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store) {
        Objects.requireNonNull(updatedMembership, "updatedMembership");
        Objects.requireNonNull(store, "store");

        if (!(changes instanceof PruneChanges pruneChanges))
            throw new IllegalArgumentException("Prune tracker received incompatible changes");

        Set<CandidateKey> keys = pruneChanges.deltas.keySet();
        if (keys.isEmpty())
            return new TrackingResult(Map.of(), Map.of());

        Map<CandidateKey, CandidateState> previousStates = store.loadCandidates(keys, CandidateTrackingMode.PRUNE);
        Map<CandidateKey, PruneState> nextStates = new HashMap<>();

        Set<CandidateKey> unresolved = new LinkedHashSet<>();
        for (Map.Entry<CandidateKey, PruneDelta> entry : pruneChanges.deltas.entrySet()) {
            CandidateKey key = entry.getKey();
            PruneDelta delta = entry.getValue();
            PruneState before = (PruneState) previousStates.get(key);

            if (delta.hasCreatedViolation()) {
                nextStates.put(key, PruneState.rejectedByCluster());
                metrics.directLhsRejected(key.lhsCol());
                continue;
            }

            if (!before.rejected()) {
                nextStates.put(key, before);
                continue;
            }

            int lhsDistinct = distinctCount(key.lhsCol());
            int rhsDistinct = distinctCount(key.rhsCol());
            if (lhsDistinct > rhsDistinct) {
                nextStates.put(key, PruneState.rejectedByCardinality());
                metrics.wholeCountPruned(key.lhsCol());
                continue;
            }

            if (rejectedByPartitionCounts(key.lhsCol(), key.rhsCol())) {
                nextStates.put(key, PruneState.rejectedByCardinality());
                metrics.partitionCountPruned(key.lhsCol());
                continue;
            }

            unresolved.add(key);
        }

        Set<CandidateKey> cqfViolations = cqf == null ? Set.of() : cqf.proposeWitnesses(unresolved).keySet();
        Set<CandidateKey> clusterCandidates = new LinkedHashSet<>(unresolved);
        clusterCandidates.removeAll(cqfViolations);
        Set<CandidateKey> clusterViolations = clusters.findViolations(clusterCandidates);

        cqfViolations.forEach(key -> metrics.cqfPruned(key.lhsCol()));
        clusterCandidates.forEach(key -> metrics.exactTested(key.lhsCol()));
        clusterViolations.forEach(key -> metrics.exactRejected(key.lhsCol()));
        clusterCandidates.stream().filter(key -> !clusterViolations.contains(key))
                .forEach(key -> metrics.exactValidated(key.lhsCol()));

        for (CandidateKey key : unresolved) {
            PruneState after = cqfViolations.contains(key) || clusterViolations.contains(key)
                    ? PruneState.rejectedByCluster()
                    : PruneState.valid();
            nextStates.put(key, after);
        }

        Map<CandidateKey, CandidateState> changedStates = new HashMap<>();

        Map<Integer, List<CandidateLocalStatus>> transitionsByLhs = new HashMap<>();

        for (CandidateKey key : keys) {
            PruneState before = (PruneState) previousStates.get(key);
            PruneState after = nextStates.get(key);
            if (after == null)
                throw new IllegalStateException("No prune result for candidate " + key);

            if (!after.equals(before))
                changedStates.put(key, after);

            if (before.rejected() != after.rejected()) {
                transitionsByLhs.computeIfAbsent(key.lhsCol(), ignored -> new ArrayList<>())
                        .add(new CandidateLocalStatus(key.rhsCol(), !after.rejected()));
            }
        }

        return new TrackingResult(changedStates, transitionsByLhs);
    }

    public boolean cqfEnabled() {
        return cqf != null;
    }

    private int distinctCount(int columnId) {
        if (columnId < 0 || columnId >= localDistinctCounts.length) {
            throw new IllegalArgumentException("Column ID outside local distinct-count array: " + columnId);
        }
        return localDistinctCounts[columnId];
    }

    private boolean rejectedByPartitionCounts(int lhsCol, int rhsCol) {
        if (localDistinctCountsByPartition == null)
            return false;
        int[] lhsCounts = localDistinctCountsByPartition[lhsCol];
        int[] rhsCounts = localDistinctCountsByPartition[rhsCol];
        for (int partition = 0; partition < lhsCounts.length; partition++) {
            if (lhsCounts[partition] > rhsCounts[partition])
                return true;
        }
        return false;
    }
}
