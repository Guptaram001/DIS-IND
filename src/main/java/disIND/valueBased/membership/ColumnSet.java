package disIND.valueBased.membership;

public interface ColumnSet {
    void add(int column);

    boolean contains(int column);

    int nextSetBit(int fromColumn);

    void clear();
}
