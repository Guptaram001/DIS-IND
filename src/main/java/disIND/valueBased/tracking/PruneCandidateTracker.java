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
import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.membership.CandidateIndex;
import disIND.valueBased.membership.CandidateSet;
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
    private final PartitionCountHierarchy partitionCounts;
    private final PruneMetricsCollector metrics;
    private final TransitiveValidityIndex transitiveValidity;
    private long transitivelyValidated;
    private final CandidateIndex candidateIndex;
    private final CandidateSet locallyRejectedCandidates;
    private final boolean transitiveEnabled;

    public PruneCandidateTracker(ValueOwnerCqf cqf, ValueOwnerClusterIndex clusters,
            int[] localDistinctCounts, PartitionCountHierarchy partitionCounts,
            PruneMetricsCollector metrics, CandidateIndex candidateIndex,
            CandidateSet locallyRejectedCandidates, CandidateDomain candidateDomain,
            boolean transitiveEnabled) {
        this.cqf = cqf;
        this.clusters = Objects.requireNonNull(clusters, "clusters");
        this.localDistinctCounts = Objects.requireNonNull(localDistinctCounts, "localDistinctCounts");
        this.partitionCounts = partitionCounts;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.candidateIndex = Objects.requireNonNull(candidateIndex, "candidateIndex");
        this.locallyRejectedCandidates = Objects.requireNonNull(locallyRejectedCandidates, "locallyRejectedCandidates");
        this.transitiveEnabled = transitiveEnabled;
        this.transitiveValidity = transitiveEnabled ? new TransitiveValidityIndex(localDistinctCounts.length) : null;
        if (transitiveEnabled) {
            Objects.requireNonNull(candidateDomain, "candidateDomain");
            for (int lhs = 0; lhs < localDistinctCounts.length; lhs++)
                transitiveValidity.initializeValid(lhs, candidateDomain.compatibleRhsSnapshot(lhs));
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

        BitSet[] affectedRhsByLhs = transitiveEnabled ? buildAffectedCandidates(pruneChanges) : null;
        Set<CandidateKey> keys = new HashSet<>(pruneChanges.deltas.size());
        LongIterator keyIterator = pruneChanges.deltas.keySet().iterator();
        while (keyIterator.hasNext()) {
            long compactKey = keyIterator.nextLong();
            keys.add(new CandidateKey(pruneChanges.bucketId, lhsColumn(compactKey), rhsColumn(compactKey)));
        }
        if (keys.isEmpty())
            return new TrackingResult(Map.of(), new Int2ObjectOpenHashMap<>());

        // Map<CandidateKey, CandidateState> previousStates = store.loadCandidates(keys,
        // CandidateTrackingMode.PRUNE);

        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs = new Int2ObjectOpenHashMap<>();

        Set<CandidateKey> unresolved = new LinkedHashSet<>();
        for (CandidateKey key : keys) {
            long compactKey = CandidateEvaluator.candidateKey(key.lhsCol(), key.rhsCol());
            byte delta = pruneChanges.deltas.get(compactKey);
            // PruneState before = (PruneState) previousStates.get(key);
            PruneState before = previousState(key);
            // if (before == null)
            // throw new IllegalStateException("No previous prune state for candidate " +
            // key);

            if (delta == PruneChanges.VIOLATION_CREATED) {
                setTransitiveValid(key.lhsCol(), key.rhsCol(), false);
                PruneState after = PruneState.rejectedByCluster();
                metrics.directLhsRejected(key.lhsCol());
                recordResult(key, before, after, transitionsByLhs);
                continue;
            }

            if (!before.rejected()) {
                continue;
            }

            int lhsCol = key.lhsCol();
            int rhsCol = key.rhsCol();
            if (distinctCount(lhsCol) > distinctCount(rhsCol)) {
                setTransitiveValid(lhsCol, rhsCol, false);
                metrics.wholeCountPruned(lhsCol);
                recordResult(key, before, PruneState.rejectedByCardinality(), transitionsByLhs);
                continue;
            }

            PartitionCountHierarchy.CheckResult partitionResult = checkPartitionCounts(lhsCol, rhsCol);
            if (partitionResult.rejected()) {
                setTransitiveValid(lhsCol, rhsCol, false);
                metrics.partitionCountPruned(lhsCol, partitionResult.rejectionLevel());
                recordResult(key, before, PruneState.rejectedByCardinality(), transitionsByLhs);
                continue;
            }

            unresolved.add(key);
        }

        if (!unresolved.isEmpty()) {
            Set<CandidateKey> cqfViolations = cqf == null ? Set.of() : cqf.proposeWitnesses(unresolved).keySet();

            for (CandidateKey key : cqfViolations) {
                setTransitiveValid(key.lhsCol(), key.rhsCol(), false);
                metrics.cqfPruned(key.lhsCol());
                // PruneState before = (PruneState) previousStates.get(key);
                PruneState before = previousState(key);
                recordResult(key, before, PruneState.rejectedByCluster(), transitionsByLhs);
            }

            unresolved.removeAll(cqfViolations);

            Set<CandidateKey> transitivelyValid = new HashSet<>();
            if (transitiveEnabled) {
                BitSet[] reachableByLhs = new BitSet[localDistinctCounts.length];
                for (CandidateKey key : unresolved) {
                    int lhs = key.lhsCol();
                    int rhs = key.rhsCol();
                    BitSet reachable = reachableByLhs[lhs];
                    if (reachable == null) {
                        reachable = transitiveValidity.reachableFrom(lhs, affectedRhsByLhs);
                        reachableByLhs[lhs] = reachable;
                    }

                    if (reachable.get(rhs))
                        transitivelyValid.add(key);
                }
            }
            for (CandidateKey key : transitivelyValid) {
                transitivelyValidated = Math.incrementExact(transitivelyValidated);
                metrics.transitivelyValidated(key.lhsCol());
                setTransitiveValid(key.lhsCol(), key.rhsCol(), true);
                PruneState before = previousState(key);
                recordResult(key, before, PruneState.valid(), transitionsByLhs);
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
                    setTransitiveValid(key.lhsCol(), key.rhsCol(), !rejected);
                    PruneState after;
                    if (rejected) {
                        metrics.exactRejected(key.lhsCol());
                        after = PruneState.rejectedByCluster();
                    } else {
                        metrics.exactValidated(key.lhsCol());
                        after = PruneState.valid();
                    }
                    PruneState before = previousState(key);
                    recordResult(key, before, after, transitionsByLhs);
                }
            }
        }

        return new TrackingResult(Map.of(), transitionsByLhs);

    }

    @Override
    public boolean persistsCandidateState() {
        return false;
    }

    private PruneState previousState(CandidateKey key) {
        int index = candidateIndex.index(key.lhsCol(), key.rhsCol());
        return locallyRejectedCandidates.contains(index) ? PruneState.rejectedByCluster() : PruneState.valid();
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

    private void recordResult(CandidateKey key, PruneState before, PruneState after,
            Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs) {

        int index = candidateIndex.index(key.lhsCol(), key.rhsCol());
        if (after.rejected()) {
            locallyRejectedCandidates.add(index);
        } else {
            locallyRejectedCandidates.remove(index);
        }

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

    private void setTransitiveValid(int lhs, int rhs, boolean valid) {
        if (transitiveEnabled) {
            transitiveValidity.setValid(lhs, rhs, valid);
        }
    }

    private int distinctCount(int columnId) {
        if (columnId < 0 || columnId >= localDistinctCounts.length) {
            throw new IllegalArgumentException("Column ID outside local distinct-count array: " + columnId);
        }
        return localDistinctCounts[columnId];
    }

    private PartitionCountHierarchy.CheckResult checkPartitionCounts(int lhsCol, int rhsCol) {
        if (partitionCounts == null)
            return new PartitionCountHierarchy.CheckResult(false, 0, 0, 0, 0);
        PartitionCountHierarchy.CheckResult result = partitionCounts.check(lhsCol, rhsCol);
        metrics.partitionComparisons(lhsCol, result.comparisons4(), result.comparisons16(),
                result.comparisonsFine());
        return result;
    }
}
