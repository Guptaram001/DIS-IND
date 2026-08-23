package disIND.valueBased.tracking;

import disIND.valueBased.membership.ColumnSet;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.model.SharedModel.PruneMetrics;
import disIND.valueBased.structures.PruneMetricsCollector;
import disIND.valueBased.structures.ValueOwnerClusterIndex;
import disIND.valueBased.structures.ValueOwnerCqf;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.BitSet;
import java.util.Map;

public sealed interface ModeSpecificContext
        permits ModeSpecificContext.CountContext, ModeSpecificContext.WitnessContext,
        ModeSpecificContext.PruneContext {

    CandidateTracker tracker();

    default boolean pruningEnabled() {
        return false;
    }

    default void membershipAdded(int columnId, int valueId) {
    }

    default void membershipChanged(Int2IntMap membershipAfter, ColumnSet addedColumns) {
    }

    default boolean locallyRejected(long candidateKey) {
        return false;
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

    default int locallyRejectedCount() {
        return 0;
    }

    static ModeSpecificContext create(CandidateTrackingMode mode, int bucketId, int totalColumns) {
        return switch (mode) {
            case COUNT -> new CountContext(new CountCandidateTracker());
            case WITNESS -> new WitnessContext(
                    new WitnessCandidateTracker(UserConfig.MAX_TRACKED_VIOLATIONS));
            case PRUNE -> new PruneContext(bucketId, totalColumns);
        };
    }

    record CountContext(CountCandidateTracker tracker) implements ModeSpecificContext {
    }

    record WitnessContext(WitnessCandidateTracker tracker) implements ModeSpecificContext {
    }

    final class PruneContext implements ModeSpecificContext {
        private final PruneCandidateTracker tracker;
        private final LongSet locallyRejectedCandidates = new LongOpenHashSet();
        private final int[] localDistinctCounts;
        private final int[][] localDistinctCountsByPartition;
        private final ValueOwnerClusterIndex clusters;
        private final ValueOwnerCqf cqf;
        private final PruneMetricsCollector metrics;

        private final BitSet beforeSignature;
        private final BitSet afterSignature;

        private PruneContext(int bucketId, int totalColumns) {
            localDistinctCounts = new int[totalColumns];
            localDistinctCountsByPartition = UserConfig.PRUNE_PARTITION_COUNTS_ENABLED
                    ? new int[totalColumns][UserConfig.PRUNE_COUNT_PARTITIONS]
                    : null;
            clusters = new ValueOwnerClusterIndex(bucketId, totalColumns);
            cqf = UserConfig.PRUNE_CQF_ENABLED ? new ValueOwnerCqf(bucketId, totalColumns) : null;
            metrics = new PruneMetricsCollector(totalColumns);
            tracker = new PruneCandidateTracker(cqf, clusters, localDistinctCounts, localDistinctCountsByPartition,
                    metrics);

            beforeSignature = new BitSet(totalColumns);
            afterSignature = new BitSet(totalColumns);
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
        public void membershipChanged(Int2IntMap membershipAfter, ColumnSet addedColumns) {
            afterSignature.clear();
            IntIterator membershipIterator = membershipAfter.keySet().iterator();
            while (membershipIterator.hasNext())
                afterSignature.set(membershipIterator.nextInt());
            beforeSignature.clear();
            beforeSignature.or(afterSignature);
            for (int column = addedColumns.nextSetBit(0); column >= 0; column = addedColumns.nextSetBit(column + 1)) {
                beforeSignature.clear(column);
            }
            clusters.moveMembership(beforeSignature, afterSignature);
        }

        @Override
        public boolean locallyRejected(long candidateKey) {
            return locallyRejectedCandidates.contains(candidateKey);
        }

        @Override
        public void candidateStatesChanged(Map<CandidateKey, CandidateState> changedStates) {
            for (Map.Entry<CandidateKey, CandidateState> entry : changedStates.entrySet()) {
                CandidateKey key = entry.getKey();
                long compactKey = candidateKey(key.lhsCol(), key.rhsCol());
                CandidateState state = entry.getValue();
                if (state.rejected())
                    locallyRejectedCandidates.add(compactKey);
                else
                    locallyRejectedCandidates.remove(compactKey);
            }
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

        private static long candidateKey(int lhsCol, int rhsCol) {
            return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
        }
    }
}
