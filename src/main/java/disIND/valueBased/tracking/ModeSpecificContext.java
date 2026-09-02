package disIND.valueBased.tracking;

import disIND.valueBased.membership.CandidateIndex;
import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.membership.CandidateSet;
import disIND.valueBased.membership.CandidateSetFactory;
import disIND.valueBased.membership.ColumnSet;
import disIND.valueBased.membership.RowBitSetCandidateSet;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.model.SharedModel.PruneMetrics;
import disIND.valueBased.structures.PruneMetricsCollector;
import disIND.valueBased.structures.ValueOwnerClusterIndex;
import disIND.valueBased.structures.ValueOwnerCqf;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public sealed interface ModeSpecificContext
        permits ModeSpecificContext.CountContext, ModeSpecificContext.WitnessContext, ModeSpecificContext.PruneContext,
        ModeSpecificContext.ExactContext {

    CandidateTracker tracker();

    default boolean pruningEnabled() {
        return false;
    }

    default void membershipAdded(int columnId, int valueId) {
    }

    default void membershipRemoved(int columnId, int valueId) {
    }

    default void membershipChanged(Int2IntMap membershipAfter, ColumnSet addedColumns, ColumnSet removedColumns) {
    }

    default boolean locallyRejected(int candidateIndex) {
        return false;
    }

    /** PRUNE-only bulk removal; other modes deliberately leave candidates unchanged. */
    default void removeLocallyRejected(int lhsCol, BitSet candidates) {
    }

    /** Records candidates removed in bulk because they were rejected earlier this batch. */
    default void sameBatchSkipped(int lhsCol, long count) {
    }

    default void candidateStatesChanged(Map<CandidateKey, CandidateState> changedStates) {
    }

    default void invalidLhsSkipped(int lhsCol) {
    }

    default void validRhsSkipped(int lhsCol) {
    }

    default void sameBatchSkipped(int lhsCol) {
    }

    default PruneMetrics metricsFor(int lhsCol) {
        return PruneMetrics.empty();
    }

    default List<long[]> activeClusterSignatures() {
        return List.of();
    }

    default int locallyRejectedCount() {
        return 0;
    }

    private static void loadSignature(Int2IntMap membership, BitSet destination) {
        destination.clear();
        IntIterator iterator = membership.keySet().iterator();
        while (iterator.hasNext())
            destination.set(iterator.nextInt());
    }

    static ModeSpecificContext create(CandidateTrackingMode mode, int bucketId, int totalColumns,
            CandidateDomain candidateDomain) {
        return switch (mode) {
            case COUNT -> new CountContext(new CountCandidateTracker());
            case WITNESS -> new WitnessContext(
                    new WitnessCandidateTracker(UserConfig.MAX_TRACKED_VIOLATIONS));
            case PRUNE -> new PruneContext(bucketId, totalColumns, candidateDomain);
            case EXACT -> new ExactContext(bucketId, totalColumns);
        };
    }

    record CountContext(CountCandidateTracker tracker) implements ModeSpecificContext {
    }

    record WitnessContext(WitnessCandidateTracker tracker) implements ModeSpecificContext {
    }

    final class ExactContext implements ModeSpecificContext {

        private final ValueOwnerClusterIndex clusters;
        private final ExactCandidateTracker tracker;

        private final BitSet beforeSignature;
        private final BitSet afterSignature;
        private final CandidateIndex candidateIndex;
        private final CandidateSet locallyRejectedCandidates;

        private ExactContext(int bucketId, int totalColumns) {
            clusters = new ValueOwnerClusterIndex(bucketId, totalColumns);
            beforeSignature = new BitSet(totalColumns);
            afterSignature = new BitSet(totalColumns);
            candidateIndex = new CandidateIndex(totalColumns);
            locallyRejectedCandidates = CandidateSetFactory.create(totalColumns, candidateIndex.capacity());
            tracker = new ExactCandidateTracker(clusters, candidateIndex, locallyRejectedCandidates);

        }

        @Override
        public boolean locallyRejected(int candidateIndex) {
            return locallyRejectedCandidates.contains(candidateIndex);
        }

        @Override
        public int locallyRejectedCount() {
            return locallyRejectedCandidates.size();
        }

        @Override
        public ExactCandidateTracker tracker() {
            return tracker;
        }

        @Override
        public void membershipChanged(Int2IntMap membershipAfter, ColumnSet addedColumns, ColumnSet removedColumns) {
            loadSignature(membershipAfter, afterSignature);
            beforeSignature.clear();
            beforeSignature.or(afterSignature);
            if (addedColumns != null) {
                for (int column = addedColumns.nextSetBit(0); column >= 0; column = addedColumns
                        .nextSetBit(column + 1)) {
                    beforeSignature.clear(column);
                }
            }
            if (removedColumns != null) {
                for (int column = removedColumns.nextSetBit(0); column >= 0; column = removedColumns
                        .nextSetBit(column + 1)) {
                    beforeSignature.set(column);
                }
            }
            clusters.moveMembership(beforeSignature, afterSignature);
        }

        @Override
        public List<long[]> activeClusterSignatures() {
            return clusters.activeSignaturesSnapshot();
        }
    }

    final class PruneContext implements ModeSpecificContext {
        private final PruneCandidateTracker tracker;
        private final int[] localDistinctCounts;
        private final int[][] localDistinctCountsByPartition;
        private final ValueOwnerClusterIndex clusters;
        private final ValueOwnerCqf cqf;
        private final PruneMetricsCollector metrics;
        private final CandidateIndex candidateIndex;
        private final CandidateSet locallyRejectedCandidates;

        private final BitSet beforeSignature;
        private final BitSet afterSignature;

        private PruneContext(int bucketId, int totalColumns, CandidateDomain candidateDomain) {
            localDistinctCounts = new int[totalColumns];
            localDistinctCountsByPartition = UserConfig.PRUNE_PARTITION_COUNTS_ENABLED
                    ? new int[totalColumns][UserConfig.PRUNE_COUNT_PARTITIONS]
                    : null;
            clusters = new ValueOwnerClusterIndex(bucketId, totalColumns);
            cqf = UserConfig.PRUNE_CQF_ENABLED ? new ValueOwnerCqf(bucketId, totalColumns) : null;
            metrics = new PruneMetricsCollector(totalColumns);

            beforeSignature = new BitSet(totalColumns);
            afterSignature = new BitSet(totalColumns);
            candidateIndex = new CandidateIndex(totalColumns);
            locallyRejectedCandidates = new RowBitSetCandidateSet(totalColumns);
            tracker = new PruneCandidateTracker(cqf, clusters, localDistinctCounts, localDistinctCountsByPartition,
                    metrics, candidateIndex, locallyRejectedCandidates, candidateDomain,
                    UserConfig.PRUNE_TRANSITIVE_ENABLED);
        }

        @Override
        public PruneCandidateTracker tracker() {
            return tracker;
        }

        @Override
        public boolean pruningEnabled() {
            return true;
        }

        @Override
        public void membershipAdded(int columnId, int valueId) {
            localDistinctCounts[columnId] = Math.addExact(localDistinctCounts[columnId], 1);
            if (localDistinctCountsByPartition != null) {
                int partition = countPartition(valueId, localDistinctCountsByPartition[columnId].length);
                localDistinctCountsByPartition[columnId][partition] = Math.addExact(
                        localDistinctCountsByPartition[columnId][partition], 1);
            }
            if (cqf != null)
                cqf.addMembership(columnId, valueId);
        }

        @Override
        public void membershipChanged(Int2IntMap membershipAfter, ColumnSet addedColumns, ColumnSet removedColumns) {
            loadSignature(membershipAfter, afterSignature);
            beforeSignature.clear();
            beforeSignature.or(afterSignature);
            if (addedColumns != null) {
                for (int column = addedColumns.nextSetBit(0); column >= 0; column = addedColumns
                        .nextSetBit(column + 1)) {
                    beforeSignature.clear(column);
                }
            }
            if (removedColumns != null) {
                for (int column = removedColumns.nextSetBit(0); column >= 0; column = removedColumns
                        .nextSetBit(column + 1)) {
                    beforeSignature.set(column);
                }
            }
            clusters.moveMembership(beforeSignature, afterSignature);
        }

        @Override
        public void membershipRemoved(int columnId, int valueId) {
            int previousDistinctCount = localDistinctCounts[columnId];

            if (previousDistinctCount <= 0)
                throw new IllegalStateException("Cannot remove missing distinct membership:"
                        + " columnId=" + columnId + ", valueId=" + valueId);

            localDistinctCounts[columnId] = previousDistinctCount - 1;
            if (localDistinctCountsByPartition != null) {
                int partition = countPartition(valueId, localDistinctCountsByPartition[columnId].length);
                int previousPartitionCount = localDistinctCountsByPartition[columnId][partition];
                if (previousPartitionCount <= 0) {
                    throw new IllegalStateException("Cannot remove missing partition membership:"
                            + " columnId=" + columnId + ", valueId=" + valueId + ", partition=" + partition);
                }
                localDistinctCountsByPartition[columnId][partition] = previousPartitionCount - 1;
            }
            if (cqf != null)
                cqf.removeMembership(columnId, valueId);

        }

        @Override
        public boolean locallyRejected(int candidateIndex) {
            return locallyRejectedCandidates.contains(candidateIndex);
        }

        @Override
        public void removeLocallyRejected(int lhsCol, BitSet candidates) {
            int removed = ((RowBitSetCandidateSet) locallyRejectedCandidates).removeRowFrom(lhsCol, candidates);
            metrics.invalidLhsSkipped(lhsCol, removed);
        }

        @Override
        public void candidateStatesChanged(Map<CandidateKey, CandidateState> changedStates) {
            for (Map.Entry<CandidateKey, CandidateState> entry : changedStates.entrySet()) {
                CandidateKey key = entry.getKey();
                int candidadateIndex = candidateIndex.index(key.lhsCol(), key.rhsCol());
                CandidateState state = entry.getValue();
                if (state.rejected())
                    locallyRejectedCandidates.add(candidadateIndex);
                else
                    locallyRejectedCandidates.remove(candidadateIndex);
            }
        }

        @Override
        public List<long[]> activeClusterSignatures() {
            return clusters.activeSignaturesSnapshot();
        }

        @Override
        public void invalidLhsSkipped(int lhsCol) {
            metrics.invalidLhsSkipped(lhsCol);
        }

        @Override
        public void validRhsSkipped(int lhsCol) {
            metrics.validRhsSkipped(lhsCol);
        }

        @Override
        public void sameBatchSkipped(int lhsCol) {
            metrics.sameBatchSkipped(lhsCol);
        }

        @Override
        public void sameBatchSkipped(int lhsCol, long count) {
            metrics.sameBatchSkipped(lhsCol, count);
        }

        @Override
        public PruneMetrics metricsFor(int lhsCol) {
            return metrics.snapshot(lhsCol);
        }

        @Override
        public int locallyRejectedCount() {
            return locallyRejectedCandidates.size();
        }

        private static int countPartition(int valueId, int partitionCount) {
            int hash = valueId;
            hash ^= hash >>> 16;
            hash *= 0x7feb352d;
            hash ^= hash >>> 15;
            hash *= 0x846ca68b;
            hash ^= hash >>> 16;
            return hash & (partitionCount - 1);
        }
    }
}
