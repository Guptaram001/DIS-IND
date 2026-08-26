package disIND.valueBased.structures;

import disIND.valueBased.model.SharedModel.PruneMetrics;

public final class PruneMetricsCollector {
    private static final int INVALID_LHS = 0;
    private static final int VALID_RHS = 1;
    private static final int SAME_BATCH = 2;
    private static final int DIRECT_LHS = 3;
    private static final int WHOLE_COUNT = 4;
    private static final int PARTITION_COUNT = 5;
    private static final int CQF = 6;
    private static final int TRANSITIVELY_VALIDATED = 7;
    private static final int EXACT_TESTED = 8;
    private static final int EXACT_REJECTED = 9;
    private static final int EXACT_VALIDATED = 10;

    private final long[][] countsByLhs;

    public PruneMetricsCollector(int totalColumns) {
        countsByLhs = new long[totalColumns][11];
    }

    public void invalidLhsSkipped(int lhs) {
        increment(lhs, INVALID_LHS);
    }

    public void validRhsSkipped(int lhs) {
        increment(lhs, VALID_RHS);
    }

    public void sameBatchSkipped(int lhs) {
        increment(lhs, SAME_BATCH);
    }

    public void directLhsRejected(int lhs) {
        increment(lhs, DIRECT_LHS);
    }

    public void wholeCountPruned(int lhs) {
        increment(lhs, WHOLE_COUNT);
    }

    public void partitionCountPruned(int lhs) {
        increment(lhs, PARTITION_COUNT);
    }

    public void cqfPruned(int lhs) {
        increment(lhs, CQF);
    }

    public void transitivelyValidated(int lhs) {
        increment(lhs, TRANSITIVELY_VALIDATED);
    }

    public void exactTested(int lhs) {
        increment(lhs, EXACT_TESTED);
    }

    public void exactRejected(int lhs) {
        increment(lhs, EXACT_REJECTED);
    }

    public void exactValidated(int lhs) {
        increment(lhs, EXACT_VALIDATED);
    }

    public PruneMetrics snapshot(int lhs) {
        long[] counts = countsByLhs[lhs];
        return new PruneMetrics(counts[INVALID_LHS], counts[VALID_RHS], counts[SAME_BATCH],
                counts[DIRECT_LHS], counts[WHOLE_COUNT], counts[PARTITION_COUNT], counts[CQF],
                counts[TRANSITIVELY_VALIDATED],
                counts[EXACT_TESTED], counts[EXACT_REJECTED], counts[EXACT_VALIDATED]);
    }

    private void increment(int lhs, int metric) {
        countsByLhs[lhs][metric] = Math.addExact(countsByLhs[lhs][metric], 1L);
    }
}
