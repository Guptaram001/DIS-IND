package disIND.valueBased.structures;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;

public final class ValueOwnerClusterIndex {
    private final int bucketId;
    private final int totalColumns;
    private final Map<BitSet, Integer> valueCountsBySignature = new HashMap<>();
    private ResolutionMetrics lastResolutionMetrics = ResolutionMetrics.empty();

    public record ResolutionMetrics(int candidates,int lhsGroups,long signatureVisits,int violations) {

        private static ResolutionMetrics empty() {
            return new ResolutionMetrics(0, 0, 0L, 0);
        }
    }

    public ValueOwnerClusterIndex(int bucketId, int totalColumns) {
        if (bucketId < 0)
            throw new IllegalArgumentException("bucketId must not be negative");
        if (totalColumns <= 0)
            throw new IllegalArgumentException("totalColumns must be positive");
        this.bucketId = bucketId;
        this.totalColumns = totalColumns;
    }

    public void moveMembership(BitSet oldSignature, BitSet newSignature) {
        Objects.requireNonNull(oldSignature, "oldSignature");
        Objects.requireNonNull(newSignature, "newSignature");
        validateSignature(oldSignature);
        validateSignature(newSignature);

        if (oldSignature.equals(newSignature))
            return;

        if (!oldSignature.isEmpty())
            decrement(oldSignature);
        if (!newSignature.isEmpty())
            increment(newSignature);
    }

    public Set<CandidateKey> findViolations(Set<CandidateKey> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        if (candidates.isEmpty()) {
            lastResolutionMetrics = ResolutionMetrics.empty();
            return Set.of();
        }

        Map<Integer, List<CandidateKey>> candidatesByLhs = new HashMap<>();
        for (CandidateKey candidate : candidates) {
            validateCandidate(candidate);
            candidatesByLhs.computeIfAbsent(candidate.lhsCol(), ignored -> new ArrayList<>())
                    .add(candidate);
        }

        Set<CandidateKey> violations = new HashSet<>();
        for (Map.Entry<Integer, List<CandidateKey>> entry : candidatesByLhs.entrySet()) {
            BitSet commonRhsColumns = null;
            for (BitSet signature : valueCountsBySignature.keySet()) {
                if (!signature.get(entry.getKey()))
                    continue;
                if (commonRhsColumns == null)
                    commonRhsColumns = (BitSet) signature.clone();
                else
                    commonRhsColumns.and(signature);
            }

            if (commonRhsColumns == null)
                continue;

            for (CandidateKey candidate : entry.getValue()) {
                if (!commonRhsColumns.get(candidate.rhsCol()))
                    violations.add(candidate);
            }
        }
        return Set.copyOf(violations);
    }

    public boolean isViolated(int lhsCol, int rhsCol) {
        validateColumn(lhsCol);
        validateColumn(rhsCol);
        for (BitSet signature : valueCountsBySignature.keySet()) {
            if (signature.get(lhsCol) && !signature.get(rhsCol))
                return true;
        }
        return false;
    }

    public int activeClusterCount() {
        return valueCountsBySignature.size();
    }

    public ResolutionMetrics lastResolutionMetrics() {
        return lastResolutionMetrics;
    }

    int valueCount(BitSet signature) {
        return valueCountsBySignature.getOrDefault(signature, 0);
    }

    private void increment(BitSet signature) {
        Integer previous = valueCountsBySignature.get(signature);
        if (previous == null) {
            valueCountsBySignature.put((BitSet) signature.clone(), 1);
        } else {
            valueCountsBySignature.put(signature, Math.addExact(previous, 1));
        }
    }

    private void decrement(BitSet signature) {
        Integer previous = valueCountsBySignature.get(signature);
        if (previous == null || previous <= 0)
            throw new IllegalStateException("VO " + bucketId + " has no active cluster " + signature);

        if (previous == 1) {
            valueCountsBySignature.remove(signature);
        } else {
            valueCountsBySignature.put(signature, previous - 1);
        }
    }

    private void validateCandidate(CandidateKey candidate) {
        if (candidate.bucketId() != bucketId)
            throw new IllegalArgumentException("Candidate belongs to another bucket: " + candidate);
        validateColumn(candidate.lhsCol());
        validateColumn(candidate.rhsCol());
    }

    private void validateSignature(BitSet signature) {
        if (signature.length() > totalColumns)
            throw new IllegalArgumentException("Membership signature contains a column outside the dataset");
    }

    private void validateColumn(int columnId) {
        if (columnId < 0 || columnId >= totalColumns)
            throw new IllegalArgumentException("Invalid column ID: " + columnId);
    }
}
