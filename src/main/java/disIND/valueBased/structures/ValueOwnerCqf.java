package disIND.valueBased.structures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public final class ValueOwnerCqf {
    private static final int MIN_CAPACITY = 16;
    private static final double MAX_LOAD_FACTOR = 0.70d;

    private final int bucketId;
    private final IntArrayList[] valueIdsByColumn;
    private int[] fingerprints;
    private int[] counts;
    private int distinctFingerprints;
    private int resizeThreshold;
    private long membershipInsertions;
    private long queryCalls;
    private long queryCandidates;
    private long lhsValueVisits;
    private long membershipProbes;
    private long tableSlotInspections;
    private long proposedViolations;
    private long resizeCount;

    public ValueOwnerCqf(int bucketId, int totalColumns) {
        if (bucketId < 0)
            throw new IllegalArgumentException("bucketId must not be negative");
        if (totalColumns <= 0)
            throw new IllegalArgumentException("totalColumns must be positive");
        this.bucketId = bucketId;
        this.valueIdsByColumn = new IntArrayList[totalColumns];
        allocate(tableCapacityFor(MIN_CAPACITY));

    }

    public ValueOwnerCqf(int bucketId, int totalColumns, Map<Integer, Map<Integer, Integer>> membershipSnapshot) {
        this(bucketId, totalColumns);
        Objects.requireNonNull(membershipSnapshot, "membershipSnapshot")
                .forEach((valueId, membership) -> membership.keySet()
                        .forEach(columnId -> addMembership(columnId, valueId)));
    }

    public void addMembership(int columnId, int valueId) {
        validateColumn(columnId);
        if (valueId < 0)
            throw new IllegalArgumentException("valueId must not be negative");

        IntArrayList values = valueIdsByColumn[columnId];
        if (values == null) {
            values = new IntArrayList();
            valueIdsByColumn[columnId] = values;
        }
        values.add(valueId);
        membershipInsertions = Math.addExact(membershipInsertions, 1L);

        insert(fingerprint(columnId, valueId), 1);
    }

    public Map<CandidateKey, Integer> proposeWitnesses(Set<CandidateKey> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        if (candidates.isEmpty()) {
            return Map.of();
        }

        queryCalls = Math.addExact(queryCalls, 1L);
        queryCandidates = Math.addExact(queryCandidates, candidates.size());

        Map<Integer, List<CandidateKey>> candidatesByLhs = new HashMap<>();
        for (CandidateKey candidate : candidates) {
            if (candidate.bucketId() != bucketId)
                throw new IllegalArgumentException("Candidate belongs to another bucket: " + candidate);
            validateColumn(candidate.lhsCol());
            validateColumn(candidate.rhsCol());
            candidatesByLhs.computeIfAbsent(candidate.lhsCol(), ignored -> new ArrayList<>())
                    .add(candidate);
        }

        Set<CandidateKey> unresolved = new HashSet<>(candidates);
        Map<CandidateKey, Integer> proposals = new HashMap<>();

        for (Map.Entry<Integer, List<CandidateKey>> entry : candidatesByLhs.entrySet()) {
            int lhsCol = entry.getKey();
            List<CandidateKey> lhsCandidates = entry.getValue();
            IntArrayList lhsValues = valueIdsByColumn[lhsCol];
            if (lhsValues == null)
                continue;

            int unresolvedForLhs = lhsCandidates.size();
            for (int index = 0; index < lhsValues.size(); index++) {
                lhsValueVisits = Math.addExact(lhsValueVisits, 1L);
                int valueId = lhsValues.getInt(index);
                for (CandidateKey candidate : lhsCandidates) {
                    if (!unresolved.contains(candidate))
                        continue;
                    if (!mightContain(candidate.rhsCol(), valueId)) {
                        proposals.put(candidate, valueId);
                        unresolved.remove(candidate);
                        unresolvedForLhs--;
                    }
                }
                if (unresolvedForLhs == 0)
                    break;
            }
        }
        proposedViolations = Math.addExact(proposedViolations, proposals.size());
        return Map.copyOf(proposals);
    }

    public void removeMembership(int columnId, int valueId) {

        validateColumn(columnId);
        if (valueId < 0)
            throw new IllegalArgumentException("valueId must not be negative");

        IntArrayList values = valueIdsByColumn[columnId];
        if (values == null)
            throw new IllegalStateException("CQF membership does not exist:" + " columnId=" + columnId
                    + ", valueId=" + valueId);

        int valueIndex = values.indexOf(valueId);
        if (valueIndex < 0)
            throw new IllegalStateException(
                    "CQF membership does not exist:" + " columnId=" + columnId + ", valueId=" + valueId);

        values.removeInt(valueIndex);
        if (values.isEmpty())
            valueIdsByColumn[columnId] = null;

        removeFingerprint(fingerprint(columnId, valueId));
    }

    private void removeFingerprint(int fingerprint) {

        int slot = quotient(fingerprint);
        while (counts[slot] != 0) {
            if (fingerprints[slot] == fingerprint) {
                if (counts[slot] > 1) {
                    counts[slot]--;
                    return;
                }
                removeSlotAndRepairCluster(slot);
                return;
            }
            slot = (slot + 1) & (fingerprints.length - 1);
        }
        throw new IllegalStateException("CQF fingerprint does not exist");
    }

    private void removeSlotAndRepairCluster(int removedSlot) {

        fingerprints[removedSlot] = 0;
        counts[removedSlot] = 0;
        distinctFingerprints--;
        int slot = (removedSlot + 1) & (fingerprints.length - 1);

        while (counts[slot] != 0) {
            int fingerprintToMove = fingerprints[slot];
            int countToMove = counts[slot];
            fingerprints[slot] = 0;
            counts[slot] = 0;
            distinctFingerprints--;
            insertWithoutResize(fingerprintToMove, countToMove);
            slot = (slot + 1) & (fingerprints.length - 1);
        }
    }

    private void insertWithoutResize(int fingerprint, int count) {

        int slot = quotient(fingerprint);
        while (counts[slot] != 0) {
            if (fingerprints[slot] == fingerprint) {
                counts[slot] = Math.addExact(counts[slot], count);
                return;
            }
            slot = (slot + 1) & (fingerprints.length - 1);
        }
        fingerprints[slot] = fingerprint;
        counts[slot] = count;
        distinctFingerprints++;
    }

    public boolean mightContain(int columnId, int valueId) {
        validateColumn(columnId);
        membershipProbes = Math.addExact(membershipProbes, 1L);
        int fingerprint = fingerprint(columnId, valueId);
        int slot = quotient(fingerprint);
        while (true) {
            tableSlotInspections = Math.addExact(tableSlotInspections, 1L);
            if (counts[slot] == 0)
                return false;
            if (fingerprints[slot] == fingerprint)
                return true;
            slot = (slot + 1) & (fingerprints.length - 1);
        }
    }

    int distinctFingerprints() {
        return distinctFingerprints;
    }

    int capacity() {
        return fingerprints.length;
    }

    private void insert(int fingerprint, int count) {
        if (distinctFingerprints + 1 > resizeThreshold)
            resize(fingerprints.length << 1);
        int slot = quotient(fingerprint);
        while (counts[slot] != 0) {
            if (fingerprints[slot] == fingerprint) {
                counts[slot] = Math.addExact(counts[slot], count);
                return;
            }
            slot = (slot + 1) & (fingerprints.length - 1);
        }
        fingerprints[slot] = fingerprint;
        counts[slot] = count;
        distinctFingerprints++;
    }

    private void resize(int requestedCapacity) {
        resizeCount = Math.addExact(resizeCount, 1L);
        int[] previousFingerprints = fingerprints;
        int[] previousCounts = counts;
        allocate(tableCapacityFor(requestedCapacity));

        for (int slot = 0; slot < previousCounts.length; slot++) {
            if (previousCounts[slot] != 0)
                insert(previousFingerprints[slot], previousCounts[slot]);
        }
    }

    private void allocate(int capacity) {
        fingerprints = new int[capacity];
        counts = new int[capacity];
        distinctFingerprints = 0;
        resizeThreshold = Math.max(1, (int) (capacity * MAX_LOAD_FACTOR));
    }

    private int quotient(int fingerprint) {
        return fingerprint & (fingerprints.length - 1);
    }

    private static int fingerprint(int columnId, int valueId) {
        long key = ((long) columnId << Integer.SIZE) | (valueId & 0xffffffffL);
        long hash = mix64(key);
        int fingerprint = (int) (hash ^ (hash >>> Integer.SIZE));
        return fingerprint == 0 ? 1 : fingerprint;
    }

    private static long mix64(long value) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdl;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53l;
        return value ^ (value >>> 33);
    }

    private static int tableCapacityFor(int requested) {
        if (requested <= MIN_CAPACITY)
            return MIN_CAPACITY;
        int highest = Integer.highestOneBit(requested - 1);
        if (highest > (1 << 29))
            throw new IllegalStateException("VO CQF capacity exhausted: " + requested);
        return highest << 1;
    }

    private void validateColumn(int columnId) {
        if (columnId < 0 || columnId >= valueIdsByColumn.length)
            throw new IllegalArgumentException("Invalid column ID: " + columnId);
    }
}
