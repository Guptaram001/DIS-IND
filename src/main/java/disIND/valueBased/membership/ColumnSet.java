package disIND.valueBased.membership;

public interface ColumnSet {
    void add(int column);

    boolean contains(int column);

    boolean isEmpty();

    int cardinality();

    int nextSetBit(int fromColumn);

    ColumnSet copy();

    void andNot(ColumnSet other);
}
