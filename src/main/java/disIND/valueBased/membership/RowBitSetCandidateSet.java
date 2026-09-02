package disIND.valueBased.membership;

import java.util.BitSet;

/**
 * Candidate set stored as one RHS bitmap per LHS. Besides ordinary candidate
 * lookups, this representation can subtract an entire rejected LHS row from a
 * candidate bitmap without visiting candidates individually.
 */
public final class RowBitSetCandidateSet implements CandidateSet {
    private final CandidateIndex index;
    private final BitSet[] rhsByLhs;
    private int size;

    public RowBitSetCandidateSet(int totalColumns) {
        index = new CandidateIndex(totalColumns);
        rhsByLhs = new BitSet[totalColumns];
        for (int lhs = 0; lhs < totalColumns; lhs++)
            rhsByLhs[lhs] = new BitSet(totalColumns);
    }

    @Override
    public boolean add(int candidateIndex) {
        BitSet row = rhsByLhs[index.lhs(candidateIndex)];
        int rhs = index.rhs(candidateIndex);
        if (row.get(rhs))
            return false;
        row.set(rhs);
        size = Math.incrementExact(size);
        return true;
    }

    @Override
    public boolean contains(int candidateIndex) {
        return rhsByLhs[index.lhs(candidateIndex)].get(index.rhs(candidateIndex));
    }

    @Override
    public boolean remove(int candidateIndex) {
        BitSet row = rhsByLhs[index.lhs(candidateIndex)];
        int rhs = index.rhs(candidateIndex);
        if (!row.get(rhs))
            return false;
        row.clear(rhs);
        size--;
        return true;
    }

    @Override
    public void clear() {
        for (BitSet row : rhsByLhs)
            row.clear();
        size = 0;
    }

    @Override
    public int size() {
        return size;
    }

    /** Removes this set's rejected RHS row and returns the number removed. */
    public int removeRowFrom(int lhs, BitSet candidates) {
        int before = candidates.cardinality();
        candidates.andNot(rhsByLhs[lhs]);
        return before - candidates.cardinality();
    }
}
