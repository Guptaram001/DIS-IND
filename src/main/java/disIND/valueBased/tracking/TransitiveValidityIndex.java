package disIND.valueBased.tracking;

import java.util.BitSet;

public final class TransitiveValidityIndex {

    private final int totalColumns;
    private final BitSet[] validRhsByLhs;

    public TransitiveValidityIndex(int totalColumns) {
        if (totalColumns <= 0) {
            throw new IllegalArgumentException(
                    "totalColumns must be positive");
        }

        this.totalColumns = totalColumns;
        this.validRhsByLhs = new BitSet[totalColumns];

        for (int lhs = 0; lhs < totalColumns; lhs++) {

            validRhsByLhs[lhs] = new BitSet(totalColumns);
        }
    }

    public void setValid(int lhs, int rhs, boolean valid) {

        validateColumn(lhs);
        validateColumn(rhs);

        if (valid) {
            validRhsByLhs[lhs].set(rhs);
        } else {
            validRhsByLhs[lhs].clear(rhs);
        }
    }

    public void initializeValid(int lhs, BitSet validRhs) {
        validateColumn(lhs);
        if (validRhs.length() > totalColumns)
            throw new IllegalArgumentException("Valid RHS contains a column outside the domain");
        validRhsByLhs[lhs].clear();
        validRhsByLhs[lhs].or(validRhs);
    }

    public BitSet reachableFrom(int lhs, BitSet[] affectedRhsByLhs) {

        validateColumn(lhs);
        BitSet visited = new BitSet(totalColumns);
        BitSet pending = new BitSet(totalColumns);
        visited.set(lhs);
        pending.set(lhs);

        while (!pending.isEmpty()) {
            int current = pending.nextSetBit(0);
            pending.clear(current);
            BitSet next = (BitSet) validRhsByLhs[current].clone();
            BitSet affected = affectedRhsByLhs[current];
            if (affected != null) {
                next.andNot(affected);
            }
            next.andNot(visited);
            if (next.isEmpty()) {
                continue;
            }

            visited.or(next);
            pending.or(next);
        }

        return visited;
    }

    private void validateColumn(int column) {
        if (column < 0 || column >= totalColumns) {

            throw new IllegalArgumentException("Invalid column " + column);
        }
    }
}
