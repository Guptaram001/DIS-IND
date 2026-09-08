package disIND.valueBased.structures;

import disIND.valueBased.model.SharedModel.PruneMetrics;

public final class PruneMetricsCollector {
    private static final int LHS_INSERTION_INVALID = 0;
    private static final int RHS_INSERTION_VALID = 1;
    private static final int SAME_BATCH_REJECTED_CANDIDATE = 2;
    private static final int DIRECT_LHS = 3;
    private static final int WHOLE_COUNT = 4;
    private static final int PARTITION_COUNT = 5;
    private static final int CQF = 6;
    private static final int TRANSITIVELY_VALIDATED = 7;
    private static final int EXACT_TESTED = 8;
    private static final int EXACT_REJECTED = 9;
    private static final int EXACT_VALIDATED = 10;
    private static final int PARTITION_4_PRUNED = 11;
    private static final int PARTITION_16_PRUNED = 12;
    private static final int PARTITION_FINE_PRUNED = 13;
    private static final int PARTITION_4_COMPARISONS = 14;
    private static final int PARTITION_16_COMPARISONS = 15;
    private static final int PARTITION_FINE_COMPARISONS = 16;
    private static final int RHS_DELETION_INVALID = 17;
    private static final int LHS_DELETION_VALID = 18;
    private static final int MIXED_UPDATE = 19;
    private static final int METRIC_COUNT = 20;

    private final long[] counts;

    public PruneMetricsCollector(int totalColumns) {
        counts = new long[Math.multiplyExact(totalColumns, METRIC_COUNT)];
    }

    public void lhsInsertionInvalidSkipped(int lhs) {
        increment(lhs, LHS_INSERTION_INVALID);
    }

    public void lhsInsertionInvalidSkipped(int lhs, long count) {
        add(lhs, LHS_INSERTION_INVALID, count);
    }

    public void rhsInsertionValidSkipped(int lhs) {
        increment(lhs, RHS_INSERTION_VALID);
    }

    public void sameBatchRejectedCandidateSkipped(int lhs) {
        increment(lhs, SAME_BATCH_REJECTED_CANDIDATE);
    }

    public void sameBatchRejectedCandidateSkipped(int lhs, long count) {
        add(lhs, SAME_BATCH_REJECTED_CANDIDATE, count);
    }

    public void rhsDeletionInvalidSkipped(int lhs) {
        increment(lhs, RHS_DELETION_INVALID);
    }

    public void lhsDeletionValidSkipped(int lhs) {
        increment(lhs, LHS_DELETION_VALID);
    }

    public void mixedUpdateSkipped(int lhs) {
        increment(lhs, MIXED_UPDATE);
    }

    public void directLhsRejected(int lhs) {
        increment(lhs, DIRECT_LHS);
    }

    public void wholeCountPruned(int lhs) {
        increment(lhs, WHOLE_COUNT);
    }

    public void partitionCountPruned(int lhs, int level) {
        increment(lhs, PARTITION_COUNT);
        if (level == 4)
            increment(lhs, PARTITION_4_PRUNED);
        else if (level == 16)
            increment(lhs, PARTITION_16_PRUNED);
        else
            increment(lhs, PARTITION_FINE_PRUNED);
    }

    public void partitionComparisons(int lhs, long comparisons4, long comparisons16, long comparisonsFine) {
        add(lhs, PARTITION_4_COMPARISONS, comparisons4);
        add(lhs, PARTITION_16_COMPARISONS, comparisons16);
        add(lhs, PARTITION_FINE_COMPARISONS, comparisonsFine);
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

        return new PruneMetrics(counts[base + LHS_INSERTION_INVALID], counts[base + RHS_INSERTION_VALID], counts[base + SAME_BATCH_REJECTED_CANDIDATE],
                counts[base + DIRECT_LHS], counts[base + WHOLE_COUNT], counts[base + PARTITION_COUNT],
                counts[base + CQF], counts[base + TRANSITIVELY_VALIDATED], counts[base + EXACT_TESTED],
                counts[base + EXACT_REJECTED], counts[base + EXACT_VALIDATED],
                counts[base + PARTITION_4_PRUNED], counts[base + PARTITION_16_PRUNED],
                counts[base + PARTITION_FINE_PRUNED], counts[base + PARTITION_4_COMPARISONS],
                counts[base + PARTITION_16_COMPARISONS], counts[base + PARTITION_FINE_COMPARISONS], counts[base + RHS_DELETION_INVALID],
                counts[base + LHS_DELETION_VALID], counts[base + MIXED_UPDATE]);
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
