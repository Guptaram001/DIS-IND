package disIND.valueBased.structures;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

public final class ValueOwnerClusterIndex {
    private final int totalColumns;
    private final Object2IntOpenHashMap<BitSet> counts = new Object2IntOpenHashMap<>();
    private final BitSet[] commonRhs;
    private final BitSet affectedLhs = new BitSet();
    private long intersections;
    private long signatureVisits;

    public ValueOwnerClusterIndex(int bucketId, int totalColumns) {
        if (bucketId < 0 || totalColumns <= 0)
            throw new IllegalArgumentException("Invalid cluster dimensions");
        this.totalColumns = totalColumns;
        this.commonRhs = new BitSet[totalColumns];
    }

    public void moveMembership(BitSet before, BitSet after) {
        Objects.requireNonNull(before);
        Objects.requireNonNull(after);
        if (before.length() > totalColumns || after.length() > totalColumns)
            throw new IllegalArgumentException("Cluster column outside dataset");
        if (before.equals(after))
            return;
        if (!before.isEmpty()) {
            int previous = counts.getInt(before);
            if (previous <= 0)
                throw new IllegalStateException("Missing old cluster");
            if (previous == 1) {
                counts.removeInt(before);
                affectedLhs.or(before);
                for (int lhs = before.nextSetBit(0); lhs >= 0; lhs = before.nextSetBit(lhs + 1))
                    commonRhs[lhs] = null; // Removal may restore previously rejected RHSs.
            } else
                counts.put(before, previous - 1);
        }
        if (!after.isEmpty()) {
            int previous = counts.getInt(after);
            if (previous == 0) {
                BitSet signature = (BitSet) after.clone();
                counts.put(signature, 1);
                affectedLhs.or(signature);
                for (int lhs = signature.nextSetBit(0); lhs >= 0; lhs = signature.nextSetBit(lhs + 1))
                    if (commonRhs[lhs] != null)
                        commonRhs[lhs].and(signature);
            } else
                counts.put(after, Math.incrementExact(previous));
        }
    }

    public BitSet takeAffectedLhs() {
        BitSet result = (BitSet) affectedLhs.clone();
        affectedLhs.clear();
        return result;
    }

    public void invalidateAll() {
        java.util.Arrays.fill(commonRhs, null);
    }

    public BitSet validRhsSnapshot(int lhs, BitSet eligible) {
        if (lhs < 0 || lhs >= totalColumns || eligible.length() > totalColumns)
            throw new IllegalArgumentException("Column outside dataset");
        BitSet result = (BitSet) eligible.clone();
        result.clear(lhs);
        if (result.isEmpty())
            return result;
        if (commonRhs[lhs] == null) {
            intersections++;
            BitSet common = new BitSet(totalColumns);
            common.set(0, totalColumns);
            common.clear(lhs);
            for (BitSet signature : counts.keySet()) {
                signatureVisits++;
                if (signature.get(lhs)) {
                    common.and(signature);
                    if (common.isEmpty())
                        break;
                }
            }
            commonRhs[lhs] = common;
        }
        result.and(commonRhs[lhs]);
        return result;
    }

    public List<long[]> activeSignaturesSnapshot() {
        return counts.keySet().stream().map(BitSet::toLongArray).toList();
    }

    public long intersections() {
        return intersections;
    }

    public long signatureVisits() {
        return signatureVisits;
    }
}
