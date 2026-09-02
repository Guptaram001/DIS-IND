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
    private static final int METRIC_COUNT = 11;

    private final long[] counts;

    public PruneMetricsCollector(int totalColumns) {
        counts = new long[Math.multiplyExact(totalColumns, METRIC_COUNT)];
    }

    public void invalidLhsSkipped(int lhs) {
        increment(lhs, INVALID_LHS);
    }

    public void invalidLhsSkipped(int lhs, long count) {
        add(lhs, INVALID_LHS, count);
    }

    public void validRhsSkipped(int lhs) {
        increment(lhs, VALID_RHS);
    }

    public void sameBatchSkipped(int lhs) {
        increment(lhs, SAME_BATCH);
    }

    public void sameBatchSkipped(int lhs, long count) {
        add(lhs, SAME_BATCH, count);
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
        int base = lhs * METRIC_COUNT;

        return new PruneMetrics(counts[base + INVALID_LHS], counts[base + VALID_RHS], counts[base + SAME_BATCH],
                counts[base + DIRECT_LHS], counts[base + WHOLE_COUNT], counts[base + PARTITION_COUNT],
                counts[base + CQF], counts[base + TRANSITIVELY_VALIDATED], counts[base + EXACT_TESTED],
                counts[base + EXACT_REJECTED], counts[base + EXACT_VALIDATED]);
    }

    private void increment(int lhs, int metric) {
        counts[lhs * METRIC_COUNT + metric]++;
    }

    private void add(int lhs, int metric, long count) {
        if (count < 0)
            throw new IllegalArgumentException("Metric increment cannot be negative: " + count);
        int index = lhs * METRIC_COUNT + metric;
        counts[index] = Math.addExact(counts[index], count);
    }
}
