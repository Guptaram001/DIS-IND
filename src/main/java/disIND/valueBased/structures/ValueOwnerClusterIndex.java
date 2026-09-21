package disIND.valueBased.structures;

import java.util.BitSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ValueOwnerClusterIndex {
    private final int totalColumns;
    private final ValueOwnerMembershipStore store;
    private final int bucketId;
    private final BitSet[] commonRhs;
    private final BitSet affectedLhs = new BitSet();
    private long intersections;
    private long signatureVisits;

    public ValueOwnerClusterIndex(int bucketId, int totalColumns, ValueOwnerMembershipStore store) {
        if (bucketId < 0 || totalColumns <= 0)
            throw new IllegalArgumentException("Invalid cluster dimensions");
        this.store = Objects.requireNonNull(store, "Cluster modes require the membership store");
        this.bucketId = bucketId;
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
            int previous = store.clusterCount(bucketId, before);
            if (previous <= 0)
                throw new IllegalStateException("Missing old cluster");
            if (previous == 1) {
                store.stageClusterCount(bucketId, before, 0);
                affectedLhs.or(before);
                for (int lhs = before.nextSetBit(0); lhs >= 0; lhs = before.nextSetBit(lhs + 1))
                    commonRhs[lhs] = null; // Removal may restore previously rejected RHSs.
            } else
                store.stageClusterCount(bucketId, before, previous - 1);
        }
        if (!after.isEmpty()) {
            int previous = store.clusterCount(bucketId, after);
            if (previous == 0) {
                BitSet signature = (BitSet) after.clone();
                store.stageClusterCount(bucketId, signature, 1);
                affectedLhs.or(signature);
                for (int lhs = signature.nextSetBit(0); lhs >= 0; lhs = signature.nextSetBit(lhs + 1))
                    if (commonRhs[lhs] != null)
                        commonRhs[lhs].and(signature);
            } else
                store.stageClusterCount(bucketId, after, Math.incrementExact(previous));
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
            store.visitSignaturesContaining(bucketId, lhs, signature -> {
                signatureVisits++;
                if (signature.get(lhs))
                    common.and(signature);
                return !common.isEmpty();
            });
            commonRhs[lhs] = common;
        }
        result.and(commonRhs[lhs]);
        return result;
    }

    public List<long[]> activeSignaturesSnapshot() {
        List<long[]> signatures = new ArrayList<>();
        store.visitClusterSignatures(bucketId, signature -> {
            signatures.add(signature.toLongArray());
            return true;
        });
        return signatures;
    }

    public long intersections() {
        return intersections;
    }

    public long signatureVisits() {
        return signatureVisits;
    }
}
