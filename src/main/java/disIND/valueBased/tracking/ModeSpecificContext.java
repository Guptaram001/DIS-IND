package disIND.valueBased.tracking;

import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.membership.ColumnSet;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.model.SharedModel.PruneMetrics;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public sealed interface ModeSpecificContext
        permits ModeSpecificContext.CountContext, ModeSpecificContext.WitnessContext, ClusterContext {

    CandidateTracker tracker();

    default boolean clusterBased() { return false; }
    default boolean derivesAtDrain() { return false; }
    default TrackingResult finishBatch() { throw new UnsupportedOperationException(); }
    default org.roaringbitmap.RoaringBitmap validRhsSnapshot(int lhs) { return null; }
    default long[] derivationMetrics() { return new long[disIND.valueBased.monitor.WorkerPhaseMetrics.DerivationWork.values().length]; }


    default boolean pruningEnabled() {
        return false;
    }

    default boolean usesAuxiliaryFilters() {
        return false;
    }

    default boolean candidateEventFilteringEnabled() {
        return pruningEnabled();
    }

    default void membershipAdded(int columnId, int valueId) {
    }

    default void membershipRemoved(int columnId, int valueId) {
    }

    default void auxiliaryMembershipAdded(int columnId, int valueId) {}

    default void auxiliaryMembershipRemoved(int columnId, int valueId) {}

    default void membershipChanged(Int2IntMap membershipAfter, ColumnSet addedColumns, ColumnSet removedColumns) {
    }

    default boolean locallyRejected(int candidateIndex) {
        return false;
    }

    default void removeLocallyRejected(int lhsCol, BitSet candidates) {
    }

    default void sameBatchRejectedCandidateSkipped(int lhsCol, long count) {
    }

    default void candidateStatesChanged(Map<CandidateKey, CandidateState> changedStates) {
    }

    default void rhsDeletionInvalidSkipped(int lhsCol) {
    }

    default void lhsDeletionValidSkipped(int lhsCol) {
    }

    default void mixedUpdateSkipped(int lhsCol) {
    }

    default void lhsInsertionInvalidSkipped(int lhsCol) {
    }

    default void rhsInsertionValidSkipped(int lhsCol) {
    }

    default void sameBatchRejectedCandidateSkipped(int lhsCol) {
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

    static ModeSpecificContext init(CandidateTrackingMode mode, int bucketId, int totalColumns,
            CandidateDomain candidateDomain) {
        return init(mode, bucketId, totalColumns, candidateDomain, disIND.valueBased.model.ClusterOptions.configured());
    }

    static ModeSpecificContext init(CandidateTrackingMode mode, int bucketId, int totalColumns,
            CandidateDomain candidateDomain, disIND.valueBased.model.ClusterOptions options) {
        return switch (mode) {
            case COUNT -> new CountContext(new CountCandidateTracker());
            case WITNESS -> new WitnessContext(
                    new WitnessCandidateTracker(UserConfig.MAX_TRACKED_VIOLATIONS));
            case PRUNE, EXACT -> new ClusterContext(mode, bucketId, totalColumns, candidateDomain, options);
        };
    }

    final class CountContext implements ModeSpecificContext {

        private final CountCandidateTracker tracker;

        CountContext(CountCandidateTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public CountCandidateTracker tracker() {
            return tracker;
        }
    }

    final class WitnessContext implements ModeSpecificContext {

        private final WitnessCandidateTracker tracker;

        WitnessContext(WitnessCandidateTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public WitnessCandidateTracker tracker() {
            return tracker;
        }
    }

}
