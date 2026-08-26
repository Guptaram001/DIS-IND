package disIND.valueBased.tracking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.BitSet;

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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public final class PruneCandidateTracker implements CandidateTracker {
    private final ValueOwnerCqf cqf;
    private final ValueOwnerClusterIndex clusters;
    private final int[] localDistinctCounts;
    private final int[][] localDistinctCountsByPartition;
    private final PruneMetricsCollector metrics;
    private final TransitiveValidityIndex transitiveValidity;
    private long transitivelyValidated;

    public PruneCandidateTracker(ValueOwnerCqf cqf, ValueOwnerClusterIndex clusters,
            int[] localDistinctCounts, int[][] localDistinctCountsByPartition,
            PruneMetricsCollector metrics) {
        this.cqf = cqf;
        this.clusters = Objects.requireNonNull(clusters, "clusters");
        this.localDistinctCounts = Objects.requireNonNull(localDistinctCounts, "localDistinctCounts");
        this.localDistinctCountsByPartition = localDistinctCountsByPartition;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.transitiveValidity = new TransitiveValidityIndex(localDistinctCounts.length);
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

    private static final class PruneChanges implements CandidateViolationAfterApplyingUpdates {
        private final int bucketId;
        private static final byte REPAIRED_ONLY = 0;
        private static final byte VIOLATION_CREATED = 1;
        private final Long2ByteOpenHashMap deltas = new Long2ByteOpenHashMap();

        private PruneChanges(int bucketId) {
            deltas.defaultReturnValue(REPAIRED_ONLY);
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(int lhsCol, int rhsCol, int valueId) {
            long key = CandidateEvaluator.candidateKey(lhsCol, rhsCol);
            deltas.put(key, VIOLATION_CREATED);
        }

        @Override
        public void violationRepaired(int lhsCol, int rhsCol, int valueId) {
            long key = CandidateEvaluator.candidateKey(lhsCol, rhsCol);
            if (!deltas.containsKey(key))
                deltas.put(key, REPAIRED_ONLY);
        }

    }

    @Override
    public CandidateViolationAfterApplyingUpdates newChanges(int bucketId) {
        return new PruneChanges(bucketId);
    }

    @Override
    public TrackingResult apply(CandidateViolationAfterApplyingUpdates changes,
            Int2ObjectMap<Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store) {
        Objects.requireNonNull(updatedMembership, "updatedMembership");
        Objects.requireNonNull(store, "store");

        if (!(changes instanceof PruneChanges pruneChanges))
            throw new IllegalArgumentException("Prune tracker received incompatible changes");

        BitSet[] affectedRhsByLhs = buildAffectedCandidates(pruneChanges);
        Set<CandidateKey> keys = new HashSet<>(pruneChanges.deltas.size());
        LongIterator keyIterator = pruneChanges.deltas.keySet().iterator();
        while (keyIterator.hasNext()) {
            long compactKey = keyIterator.nextLong();
            keys.add(new CandidateKey(pruneChanges.bucketId, lhsColumn(compactKey), rhsColumn(compactKey)));
        }
        if (keys.isEmpty())
            return new TrackingResult(Map.of(), new Int2ObjectOpenHashMap<>());

        Map<CandidateKey, CandidateState> previousStates = store.loadCandidates(keys, CandidateTrackingMode.PRUNE);
        Map<CandidateKey, CandidateState> changedStates = new HashMap<>(keys.size());
        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs = new Int2ObjectOpenHashMap<>();

        Set<CandidateKey> unresolved = new LinkedHashSet<>();
        for (CandidateKey key : keys) {
            long compactKey = CandidateEvaluator.candidateKey(key.lhsCol(), key.rhsCol());
            byte delta = pruneChanges.deltas.get(compactKey);
            PruneState before = (PruneState) previousStates.get(key);
            if (before == null)
                throw new IllegalStateException("No previous prune state for candidate " + key);

            if (delta == PruneChanges.VIOLATION_CREATED) {
                transitiveValidity.setValid(key.lhsCol(), key.rhsCol(), false);
                PruneState after = PruneState.rejectedByCluster();
                metrics.directLhsRejected(key.lhsCol());
                recordResult(key, before, after, changedStates, transitionsByLhs);
                continue;
            }

            if (!before.rejected()) {
                continue;
            }

            int lhsCol = key.lhsCol();
            int rhsCol = key.rhsCol();
            if (distinctCount(lhsCol) > distinctCount(rhsCol)) {
                transitiveValidity.setValid(lhsCol, rhsCol, false);
                metrics.wholeCountPruned(lhsCol);
                recordResult(key, before, PruneState.rejectedByCardinality(), changedStates, transitionsByLhs);
                continue;
            }

            if (rejectedByPartitionCounts(lhsCol, rhsCol)) {
                transitiveValidity.setValid(lhsCol, rhsCol, false);
                metrics.partitionCountPruned(lhsCol);
                recordResult(key, before, PruneState.rejectedByCardinality(), changedStates, transitionsByLhs);
                continue;
            }

            unresolved.add(key);
        }

        if (!unresolved.isEmpty()) {
            Set<CandidateKey> cqfViolations = cqf == null ? Set.of() : cqf.proposeWitnesses(unresolved).keySet();

            /*
             * CQF-resolved candidates do not need exact cluster
             * evaluation.
             */
            for (CandidateKey key : cqfViolations) {
                transitiveValidity.setValid(key.lhsCol(), key.rhsCol(), false);
                metrics.cqfPruned(key.lhsCol());
                PruneState before = (PruneState) previousStates.get(key);
                recordResult(key, before, PruneState.rejectedByCluster(), changedStates, transitionsByLhs);
            }

            /*
             * Reuse unresolved as the exact-evaluation set instead
             * of allocating clusterCandidates.
             */
            unresolved.removeAll(cqfViolations);

            BitSet[] reachableByLhs = new BitSet[localDistinctCounts.length];
            Set<CandidateKey> transitivelyValid = new HashSet<>();
            for (CandidateKey key : unresolved) {
                int lhs = key.lhsCol();
                int rhs = key.rhsCol();
                BitSet reachable = reachableByLhs[lhs];
                if (reachable == null) {
                    reachable = transitiveValidity.reachableFrom(lhs, affectedRhsByLhs);
                    reachableByLhs[lhs] = reachable;
                }

                if (reachable.get(rhs)) {
                    transitivelyValid.add(key);
                }
            }
            for (CandidateKey key : transitivelyValid) {
                transitivelyValidated = Math.incrementExact(transitivelyValidated);
                metrics.transitivelyValidated(key.lhsCol());
                transitiveValidity.setValid(key.lhsCol(), key.rhsCol(), true);
                PruneState before = (PruneState) previousStates.get(key);
                recordResult(key, before, PruneState.valid(), changedStates, transitionsByLhs);
            }

            unresolved.removeAll(transitivelyValid);

            if (!unresolved.isEmpty()) {
                LongOpenHashSet exactCandidateKeys = new LongOpenHashSet(unresolved.size());
                for (CandidateKey key : unresolved) {
                    exactCandidateKeys.add(CandidateEvaluator.candidateKey(key.lhsCol(), key.rhsCol()));
                }

                LongSet clusterViolationKeys = clusters.findViolationKeys(exactCandidateKeys);
                for (CandidateKey key : unresolved) {
                    metrics.exactTested(key.lhsCol());
                    long compactKey = CandidateEvaluator.candidateKey(key.lhsCol(), key.rhsCol());

                    boolean rejected = clusterViolationKeys.contains(
                            compactKey);
                    transitiveValidity.setValid(key.lhsCol(), key.rhsCol(), !rejected);
                    PruneState after;
                    if (rejected) {
                        metrics.exactRejected(key.lhsCol());
                        after = PruneState.rejectedByCluster();
                    } else {
                        metrics.exactValidated(key.lhsCol());
                        after = PruneState.valid();
                    }
                    PruneState before = (PruneState) previousStates.get(key);
                    recordResult(key, before, after, changedStates, transitionsByLhs);
                }
            }
        }

        return new TrackingResult(changedStates, transitionsByLhs);

    }

    private BitSet[] buildAffectedCandidates(PruneChanges changes) {

        BitSet[] affected = new BitSet[localDistinctCounts.length];
        LongIterator iterator = changes.deltas.keySet().iterator();

        while (iterator.hasNext()) {
            long key = iterator.nextLong();
            int lhs = lhsColumn(key);
            int rhs = rhsColumn(key);
            BitSet rhsSet = affected[lhs];
            if (rhsSet == null) {
                rhsSet = new BitSet(localDistinctCounts.length);
                affected[lhs] = rhsSet;
            }
            rhsSet.set(rhs);
        }
        return affected;
    }

    private static void recordResult(CandidateKey key, PruneState before, PruneState after,
            Map<CandidateKey, CandidateState> changedStates,
            Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs) {

        if (!after.equals(before))
            changedStates.put(key, after);
        if (before.rejected() != after.rejected()) {
            int lhsCol = key.lhsCol();
            List<CandidateLocalStatus> transitions = transitionsByLhs.get(lhsCol);
            if (transitions == null) {
                transitions = new ArrayList<>();
                transitionsByLhs.put(lhsCol, transitions);
            }
            transitions.add(new CandidateLocalStatus(key.rhsCol(), !after.rejected()));
        }
    }

    private static int lhsColumn(long compactKey) {
        return (int) (compactKey >>> Integer.SIZE);
    }

    private static int rhsColumn(long compactKey) {
        return (int) compactKey;
    }

    public long transitivelyValidated() {
        return transitivelyValidated;
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
