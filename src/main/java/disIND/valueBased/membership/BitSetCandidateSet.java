package disIND.valueBased.membership;

import java.util.BitSet;

public class BitSetCandidateSet implements CandidateSet {

    private final BitSet bits;

    BitSetCandidateSet(int capacity) {
        bits = new BitSet(capacity);
    }

    @Override
    public boolean add(int index) {
        boolean added = !bits.get(index);
        bits.set(index);
        return added;
    }

    @Override
    public boolean contains(int index) {
        return bits.get(index);
    }

    @Override
    public boolean remove(int index) {
        boolean removed = bits.get(index);
        bits.clear(index);
        return removed;
    }

    @Override
    public void clear() {
        bits.clear();
    }

    @Override
    public int size() {
        return bits.cardinality();
    }

}
