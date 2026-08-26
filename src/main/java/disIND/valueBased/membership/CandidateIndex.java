package disIND.valueBased.membership;

public final class CandidateIndex {

    private final int totalColumns;
    private final int capacity;

    public CandidateIndex(int totalColumns) {
        if (totalColumns <= 0)
            throw new IllegalArgumentException("totalColumns must be positive");

        this.totalColumns = totalColumns;
        this.capacity = Math.multiplyExact(totalColumns, totalColumns);
    }

    public int capacity() {
        return capacity;
    }

    public int index(int lhs, int rhs) {
        if (lhs < 0 || lhs >= totalColumns || rhs < 0 || rhs >= totalColumns)
            throw new IndexOutOfBoundsException("lhs=" + lhs + ", rhs=" + rhs);
        return lhs * totalColumns + rhs;
    }

    public int lhs(int index) {
        return index / totalColumns;
    }

    public int rhs(int index) {
        return index % totalColumns;
    }
}