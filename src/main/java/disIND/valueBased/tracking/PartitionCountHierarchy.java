package disIND.valueBased.tracking;

public final class PartitionCountHierarchy {
    public record CheckResult(boolean rejected, int rejectionLevel, int comparisons4, int comparisons16,
            int comparisonsFine) {
    }

    private final int finePartitions;
    private final int shiftTo4;
    private final int shiftTo16;
    private final int[][] counts4;
    private final int[][] counts16;
    private final int[][] countsFine;

    public PartitionCountHierarchy(int totalColumns, int finePartitions, boolean hierarchyEnabled) {
        if (totalColumns <= 0)
            throw new IllegalArgumentException("totalColumns must be positive");
        if (finePartitions <= 0 || (finePartitions & (finePartitions - 1)) != 0)
            throw new IllegalArgumentException("Fine partition count must be a positive power of two");
        this.finePartitions = finePartitions;
        this.counts4 = hierarchyEnabled && finePartitions > 4 ? new int[totalColumns][4] : null;
        this.counts16 = hierarchyEnabled && finePartitions > 16 ? new int[totalColumns][16] : null;
        this.countsFine = new int[totalColumns][finePartitions];
        this.shiftTo4 = counts4 == null ? 0
                : Integer.numberOfTrailingZeros(finePartitions / counts4[0].length);
        this.shiftTo16 = counts16 == null ? 0
                : Integer.numberOfTrailingZeros(finePartitions / counts16[0].length);
    }

    public void add(int columnId, int valueId) {
        update(columnId, valueId, 1);
    }

    public void remove(int columnId, int valueId) {
        update(columnId, valueId, -1);
    }

    public CheckResult check(int lhsCol, int rhsCol) {
        int comparisons4 = 0;
        int comparisons16 = 0;
        int comparisonsFine = 0;

        if (counts4 != null) {
            int[] lhs = counts4[lhsCol];
            int[] rhs = counts4[rhsCol];
            for (int partition = 0; partition < lhs.length; partition++) {
                comparisons4++;
                if (lhs[partition] > rhs[partition])
                    return new CheckResult(true, 4, comparisons4, 0, 0);
            }
        }

        if (counts16 != null) {
            int[] lhs = counts16[lhsCol];
            int[] rhs = counts16[rhsCol];
            for (int partition = 0; partition < lhs.length; partition++) {
                comparisons16++;
                if (lhs[partition] > rhs[partition])
                    return new CheckResult(true, 16, comparisons4, comparisons16, 0);
            }
        }

        int[] lhs = countsFine[lhsCol];
        int[] rhs = countsFine[rhsCol];
        for (int partition = 0; partition < lhs.length; partition++) {
            comparisonsFine++;
            if (lhs[partition] > rhs[partition])
                return new CheckResult(true, finePartitions, comparisons4, comparisons16, comparisonsFine);
        }
        return new CheckResult(false, 0, comparisons4, comparisons16, comparisonsFine);
    }

    private void update(int columnId, int valueId, int delta) {
        int finePartition = countPartition(valueId, finePartitions);
        updateCount(countsFine[columnId], finePartition, delta, columnId, valueId);
        if (counts16 != null)
            updateCount(counts16[columnId], finePartition >>> shiftTo16, delta, columnId, valueId);
        if (counts4 != null)
            updateCount(counts4[columnId], finePartition >>> shiftTo4, delta, columnId, valueId);
    }

    private static void updateCount(int[] counts, int partition, int delta, int columnId, int valueId) {
        int next = Math.addExact(counts[partition], delta);
        if (next < 0)
            throw new IllegalStateException("Cannot remove missing partition membership: columnId=" + columnId);
        counts[partition] = next;
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
