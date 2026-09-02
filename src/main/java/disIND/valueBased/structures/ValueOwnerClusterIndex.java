package disIND.valueBased.structures;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;

public final class ValueOwnerClusterIndex {
    private final int bucketId;
    private final int totalColumns;
    private final ClusterValidationStrategy validationStrategy;
    private final Object2IntOpenHashMap<BitSet> valueCountsBySignature = new Object2IntOpenHashMap<>();

    private final BitSet[] commonRhsByLhs;
    private final BitSet dirtyLhs;
    private long cacheHits;
    private long cacheRebuilds;
    private long cacheInvalidations;
    private long cacheRebuildSignatureVisits;

    private int peakActiveClusters;
    private long activeValues;
    private long membershipMoves;
    private long clusterCreations;
    private long clusterRemovals;
    private long scanCalls;
    private long candidatesScanned;
    private long lhsGroupsScanned;
    private long totalSignatureVisits;
    private long violationsFound;
    private long totalScanNanos;
    private long maxScanNanos;

    public record ClusterMetrics(int bucketId, int activeClusters, int peakActiveClusters, long activeValues,
            long membershipMoves, long clusterCreations, long clusterRemovals, long scanCalls, long candidatesScanned,
            long lhsGroupsScanned, long signatureVisits, long violationsFound, long totalScanNanos, long maxScanNanos) {

        public double maxScanMillis() {
            return maxScanNanos / 1_000_000.0;
        }
    }

    public ValueOwnerClusterIndex(int bucketId, int totalColumns) {
        this(bucketId, totalColumns, ClusterValidationStrategy.SCAN);
    }

    public ValueOwnerClusterIndex(int bucketId, int totalColumns, ClusterValidationStrategy validationStrategy) {
        if (bucketId < 0)
            throw new IllegalArgumentException("bucketId must not be negative");
        if (totalColumns <= 0)
            throw new IllegalArgumentException("totalColumns must be positive");
        this.bucketId = bucketId;
        this.totalColumns = totalColumns;
        this.validationStrategy = Objects.requireNonNull(validationStrategy, "validationStrategy");
        valueCountsBySignature.defaultReturnValue(0);
        if (validationStrategy == ClusterValidationStrategy.LHS_CACHE) {
            commonRhsByLhs = new BitSet[totalColumns];
            dirtyLhs = new BitSet(totalColumns);
            dirtyLhs.set(0, totalColumns);
        } else {
            commonRhsByLhs = null;
            dirtyLhs = null;
        }
    }

    public record CacheMetrics(ClusterValidationStrategy strategy, long hits, long rebuilds, long invalidations,
            long rebuildSignatureVisits) {
    }

    public CacheMetrics cacheMetrics() {
        return new CacheMetrics(validationStrategy, cacheHits, cacheRebuilds, cacheInvalidations,
                cacheRebuildSignatureVisits);
    }

    public void moveMembership(BitSet oldSignature, BitSet newSignature) {
        Objects.requireNonNull(oldSignature, "oldSignature");
        Objects.requireNonNull(newSignature, "newSignature");
        validateSignature(oldSignature);
        validateSignature(newSignature);

        if (oldSignature.equals(newSignature))
            return;
        membershipMoves = Math.incrementExact(membershipMoves);

        if (!oldSignature.isEmpty())
            decrement(oldSignature);
        else
            activeValues = Math.incrementExact(activeValues);

        if (!newSignature.isEmpty())
            increment(newSignature);
        else
            activeValues = Math.decrementExact(activeValues);
        peakActiveClusters = Math.max(peakActiveClusters, valueCountsBySignature.size());
    }

    public List<long[]> activeSignaturesSnapshot() {
        List<long[]> signatures = new ArrayList<>(valueCountsBySignature.size());
        for (BitSet signature : valueCountsBySignature.keySet())
            signatures.add(signature.toLongArray());
        return List.copyOf(signatures);
    }

    private void increment(BitSet signature) {
        int previous = valueCountsBySignature.getInt(signature);
        if (previous == 0) {
            BitSet storedSignature = (BitSet) signature.clone();
            valueCountsBySignature.put(storedSignature, 1);
            clusterCreations = Math.incrementExact(clusterCreations);
            signatureAdded(storedSignature);
        } else {
            valueCountsBySignature.put(signature, Math.addExact(previous, 1));
        }
    }

    private void decrement(BitSet signature) {
        int previous = valueCountsBySignature.getInt(signature);
        if (previous <= 0)
            throw new IllegalStateException("VO " + bucketId + " has no active cluster " + signature);

        if (previous == 1) {
            valueCountsBySignature.removeInt(signature);
            clusterRemovals = Math.incrementExact(clusterRemovals);
            signatureRemoved(signature);
        } else {
            valueCountsBySignature.put(signature, previous - 1);
        }
    }

    private void signatureAdded(BitSet signature) {
        if (validationStrategy != ClusterValidationStrategy.LHS_CACHE)
            return;

        for (int lhs = signature.nextSetBit(0); lhs >= 0; lhs = signature.nextSetBit(lhs + 1)) {
            if (dirtyLhs.get(lhs))
                continue;

            BitSet cached = commonRhsByLhs[lhs];
            if (cached == null)
                commonRhsByLhs[lhs] = (BitSet) signature.clone();
            else
                cached.and(signature);
        }
    }

    private void signatureRemoved(BitSet signature) {
        if (validationStrategy != ClusterValidationStrategy.LHS_CACHE)
            return;

        for (int lhs = signature.nextSetBit(0); lhs >= 0; lhs = signature.nextSetBit(lhs + 1)) {
            if (!dirtyLhs.get(lhs)) {
                dirtyLhs.set(lhs);
                cacheInvalidations = Math.incrementExact(cacheInvalidations);
            }
        }
    }

    private void validateSignature(BitSet signature) {
        if (signature.length() > totalColumns)
            throw new IllegalArgumentException("Membership signature contains a column outside the dataset");
    }

    private void validateColumn(int columnId) {
        if (columnId < 0 || columnId >= totalColumns)
            throw new IllegalArgumentException("Invalid column ID: " + columnId);
    }

    public LongSet findViolationKeys(LongSet candidates) {

        Objects.requireNonNull(candidates, "candidates");

        if (candidates.isEmpty()) {
            // lastResolutionMetrics = ResolutionMetrics.empty();
            return new LongOpenHashSet();
        }

        long scanStarted = System.nanoTime();
        BitSet[] candidateRhsByLhs = new BitSet[totalColumns];
        int lhsGroups = 0;
        LongIterator candidateIterator = candidates.iterator();
        while (candidateIterator.hasNext()) {
            long candidateKey = candidateIterator.nextLong();
            int lhs = lhsColumn(candidateKey);
            int rhs = rhsColumn(candidateKey);
            validateColumn(lhs);
            validateColumn(rhs);

            BitSet rhsCandidates = candidateRhsByLhs[lhs];
            if (rhsCandidates == null) {
                rhsCandidates = new BitSet(totalColumns);
                candidateRhsByLhs[lhs] = rhsCandidates;
                lhsGroups++;
            }
            rhsCandidates.set(rhs);
        }

        LongOpenHashSet violations = new LongOpenHashSet();
        if (validationStrategy == ClusterValidationStrategy.LHS_CACHE) {
            long signatureVisits = findViolationKeysCached(candidateRhsByLhs, violations);
            long elapsedNanos = System.nanoTime() - scanStarted;
            recordScan(candidates.size(), lhsGroups, signatureVisits, violations.size(), elapsedNanos);
            return violations;
        }

        long signatureVisits = 0L;
        for (int lhs = 0; lhs < totalColumns; lhs++) {
            BitSet rhsCandidates = candidateRhsByLhs[lhs];
            if (rhsCandidates == null)
                continue;

            BitSet survivingRhs = (BitSet) rhsCandidates.clone();
            boolean lhsSeen = false;
            for (BitSet signature : valueCountsBySignature.keySet()) {
                signatureVisits = Math.incrementExact(signatureVisits);
                if (!signature.get(lhs))
                    continue;

                lhsSeen = true;
                survivingRhs.and(signature);
                if (survivingRhs.isEmpty())
                    break;

            }
            if (!lhsSeen)
                continue;

            BitSet invalidRhs = (BitSet) rhsCandidates.clone();
            invalidRhs.andNot(survivingRhs);
            for (int rhs = invalidRhs.nextSetBit(0); rhs >= 0; rhs = invalidRhs.nextSetBit(rhs + 1)) {
                violations.add(candidateKey(lhs, rhs));
            }
        }

        long elapsedNanos = System.nanoTime() - scanStarted;
        recordScan(candidates.size(), lhsGroups, signatureVisits, violations.size(), elapsedNanos);

        return violations;
    }

    private long findViolationKeysCached(BitSet[] candidateRhsByLhs, LongOpenHashSet violations) {
        long visitsBefore = cacheRebuildSignatureVisits;
        for (int lhs = 0; lhs < totalColumns; lhs++) {
            BitSet rhsCandidates = candidateRhsByLhs[lhs];
            if (rhsCandidates == null)
                continue;

            BitSet cached = commonRhsFor(lhs);
            if (cached == null)
                continue;

            BitSet invalidRhs = (BitSet) rhsCandidates.clone();
            invalidRhs.andNot(cached);
            for (int rhs = invalidRhs.nextSetBit(0); rhs >= 0; rhs = invalidRhs.nextSetBit(rhs + 1))
                violations.add(candidateKey(lhs, rhs));
        }
        return cacheRebuildSignatureVisits - visitsBefore;
    }

    private BitSet commonRhsFor(int lhs) {
        if (!dirtyLhs.get(lhs)) {
            cacheHits = Math.incrementExact(cacheHits);
            return commonRhsByLhs[lhs];
        }

        BitSet intersection = null;
        long visits = 0L;
        for (BitSet signature : valueCountsBySignature.keySet()) {
            visits = Math.incrementExact(visits);
            if (!signature.get(lhs))
                continue;
            if (intersection == null)
                intersection = (BitSet) signature.clone();
            else
                intersection.and(signature);
        }

        commonRhsByLhs[lhs] = intersection;
        dirtyLhs.clear(lhs);
        cacheRebuilds = Math.incrementExact(cacheRebuilds);
        cacheRebuildSignatureVisits = Math.addExact(cacheRebuildSignatureVisits, visits);
        return intersection;
    }

    private static long candidateKey(int lhs, int rhs) {
        return ((long) lhs << Integer.SIZE) | (rhs & 0xffffffffL);
    }

    private static int lhsColumn(long key) {
        return (int) (key >>> Integer.SIZE);
    }

    private static int rhsColumn(long key) {
        return (int) key;
    }

    private void recordScan(long candidateCount, long lhsGroups, long signatureVisits, long violationCount,
            long elapsedNanos) {

        scanCalls = Math.incrementExact(scanCalls);
        candidatesScanned = Math.addExact(candidatesScanned, candidateCount);
        lhsGroupsScanned = Math.addExact(lhsGroupsScanned, lhsGroups);
        totalSignatureVisits = Math.addExact(totalSignatureVisits, signatureVisits);
        violationsFound = Math.addExact(violationsFound, violationCount);
        totalScanNanos = Math.addExact(totalScanNanos, elapsedNanos);
        maxScanNanos = Math.max(maxScanNanos, elapsedNanos);
    }
}
