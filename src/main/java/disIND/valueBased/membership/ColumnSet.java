package disIND.valueBased.membership;

import java.util.function.IntConsumer;

public interface ColumnSet {
    void add(int column);

    boolean contains(int column);

    boolean isEmpty();

    int cardinality();

    void forEach(IntConsumer action);

    ColumnSet copy();

    void andNot(ColumnSet other);
}
