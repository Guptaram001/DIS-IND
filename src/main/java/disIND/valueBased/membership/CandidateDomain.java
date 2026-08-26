package disIND.valueBased.membership;

import disIND.valueBased.model.SharedModel.DatasetMetadata;
import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;
import java.util.BitSet;

public final class CandidateDomain {
    // Candidate domain for unary with compatibility
    private final int totalColumns;
    private final BitSet[] compatibleRhsByLhs;

    public CandidateDomain(DatasetMetadata metadata) {
        this.totalColumns = metadata.totalCols();
        this.compatibleRhsByLhs = new BitSet[totalColumns];
        for (int lhs = 0; lhs < totalColumns; lhs++) {
            BitSet compatible = new BitSet(totalColumns);
            for (int rhs = 0; rhs < totalColumns; rhs++) {
                if (lhs != rhs && testCompatibility(metadata.typeOf(lhs), metadata.typeOf(rhs)))
                    compatible.set(rhs);
            }
            compatibleRhsByLhs[lhs] = compatible;
        }
    }

    public boolean isCompatible(int lhs, int rhs) {
        return compatibleRhsByLhs[lhs].get(rhs);
    }

    public int firstCompatibleRhs(int lhs) {
        return compatibleRhsByLhs[lhs].nextSetBit(0);
    }

    public int nextCompatibleRhs(int lhs, int currentRhs) {
        return compatibleRhsByLhs[lhs].nextSetBit(currentRhs + 1);
    }
}
