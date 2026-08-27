package disIND.valueBased.membership;

import disIND.valueBased.model.SharedModel.DatasetMetadata;
import disIND.valueBased.model.SharedModel.ColType;
import disIND.valueBased.utility.UserConfig;

import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;
import java.util.BitSet;

public final class CandidateDomain {
    // Candidate domain for unary with compatibility
    private final int totalColumns;
    private final BitSet[] compatibleRhsByLhs;

    public CandidateDomain(DatasetMetadata metadata) {
        boolean useTypeCompatibility = UserConfig.TYPE_COMPATIBILITY_ENABLED;
        this.totalColumns = metadata.totalCols();
        this.compatibleRhsByLhs = new BitSet[totalColumns];
        for (int lhs = 0; lhs < totalColumns; lhs++) {
            BitSet candidates = new BitSet(totalColumns);

            if (!useTypeCompatibility) {
                candidates.set(0, totalColumns);
                candidates.clear(lhs);
            } else {
                ColType lhsType = metadata.typeOf(lhs);
                for (int rhs = 0; rhs < totalColumns; rhs++) {
                    if (lhs != rhs && testCompatibility(lhsType, metadata.typeOf(rhs))) {
                        candidates.set(rhs);
                    }
                }
            }

            compatibleRhsByLhs[lhs] = candidates;
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
