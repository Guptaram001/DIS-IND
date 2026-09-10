package disIND.valueBased.tracking;

import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.membership.ColumnSet;
import disIND.valueBased.model.ClusterOptions;
import disIND.valueBased.model.ClusterOptions.IndCalculation;
import disIND.valueBased.model.SharedModel.*;
import disIND.valueBased.structures.*;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

public final class ClusterContext implements ModeSpecificContext {
    private final int bucketId, columns;
    private final boolean prune;
    private final ClusterOptions options;
    private final CandidateDomain domain;
    private final ValueOwnerClusterIndex clusters;
    private final PruneMetricsCollector metrics;
    private final int[] distinct;
    private final PartitionCountHierarchy partitions;
    private final ValueOwnerCqf cqf;
    private final TransitiveValidityIndex transitive;
    private final BitSet[] noExcludedEdges;
    private final BitSet[] results;
    private final BitSet addedColumns = new BitSet(), removedColumns = new BitSet();
    private final BitSet before = new BitSet(), after = new BitSet();
    private boolean finalStarted;
    private long dirtyLhsCount, transitionsCount;

    public ClusterContext(CandidateTrackingMode mode, int bucketId, int columns,
            CandidateDomain domain, ClusterOptions options) {
        if (mode != CandidateTrackingMode.EXACT && mode != CandidateTrackingMode.PRUNE)
            throw new IllegalArgumentException("Cluster context requires exact or prune");
        this.bucketId = bucketId;
        this.columns = columns;
        this.prune = mode == CandidateTrackingMode.PRUNE;
        this.domain = domain;
        this.options = Objects.requireNonNull(options);
        clusters = new ValueOwnerClusterIndex(bucketId, columns);
        metrics = new PruneMetricsCollector(columns);
        results = new BitSet[columns];
        distinct = prune ? new int[columns] : null;
        partitions = prune && options.partitionCounts()
                ? new PartitionCountHierarchy(columns, options.partitions(), options.partitionHierarchy())
                : null;
        cqf = prune && options.cqf() ? new ValueOwnerCqf(bucketId, columns) : null;
        transitive = prune && options.transitive() ? new TransitiveValidityIndex(columns) : null;
        noExcludedEdges = transitive == null ? null : new BitSet[columns];
    }

    @Override
    public boolean clusterBased() {
        return true;
    }

    @Override
    public boolean derivesAtDrain() {
        return options.calculation() == IndCalculation.FINAL;
    }

    @Override
    public boolean usesAuxiliaryFilters() {
        return prune;
    }

    @Override
    public CandidateTracker tracker() {
        throw new UnsupportedOperationException("Cluster mode has no candidate-event tracker");
    }

    @Override
    public void membershipAdded(int column, int value) {
        if (!derivesAtDrain())
            addedColumns.set(column);
        if (prune)
            distinct[column] = Math.incrementExact(distinct[column]);
        if (partitions != null)
            partitions.add(column, value);
        if (cqf != null)
            cqf.addMembership(column, value);
    }

    @Override
    public void membershipRemoved(int column, int value) {
        if (!derivesAtDrain())
            removedColumns.set(column);
        if (prune) {
            if (distinct[column] <= 0)
                throw new IllegalStateException("Removing absent membership");
            distinct[column]--;
        }
        if (partitions != null)
            partitions.remove(column, value);
        if (cqf != null)
            cqf.removeMembership(column, value);
    }

    @Override
    public void membershipChanged(Int2IntMap membership, ColumnSet added, ColumnSet removed) {
        if (finalStarted)
            throw new IllegalStateException("Cannot update after final derivation");
        after.clear();
        for (int column : membership.keySet())
            after.set(column);
        before.clear();
        before.or(after);
        if (added != null)
            for (int column = added.nextSetBit(0); column >= 0; column = added.nextSetBit(column + 1))
                before.clear(column);
        if (removed != null)
            for (int column = removed.nextSetBit(0); column >= 0; column = removed.nextSetBit(column + 1))
                before.set(column);
        clusters.moveMembership(before, after);
    }

    private BitSet eligible(int lhs) {
        if (domain != null)
            return domain.compatibleRhsSnapshot(lhs);
        BitSet rhs = new BitSet(columns);
        rhs.set(0, columns);
        rhs.clear(lhs);
        return rhs;
    }

    @Override
    public TrackingResult finishBatch() {
        if (derivesAtDrain())
            throw new IllegalStateException("Final mode must not derive during ingestion");
        BitSet dirty = clusters.takeAffectedLhs();
        dirtyLhsCount += dirty.cardinality();
        if (!options.changeDetection()) {
            dirty.set(0, columns);
            clusters.invalidateAll();
        }
        if (transitive != null)
            for (int lhs = dirty.nextSetBit(0); lhs >= 0; lhs = dirty.nextSetBit(lhs + 1))
                transitive.initializeValid(lhs, new BitSet());
        var transitions = new Int2ObjectOpenHashMap<List<CandidateLocalStatus>>();
        for (int lhs = dirty.nextSetBit(0); lhs >= 0; lhs = dirty.nextSetBit(lhs + 1)) {
            BitSet previous = results[lhs] == null ? eligible(lhs) : results[lhs];
            BitSet next = derive(lhs, previous);
            BitSet changed = (BitSet) previous.clone();
            changed.xor(next);
            if (!changed.isEmpty()) {
                List<CandidateLocalStatus> row = new ArrayList<>();
                for (int rhs = changed.nextSetBit(0); rhs >= 0; rhs = changed.nextSetBit(rhs + 1))
                    row.add(new CandidateLocalStatus(rhs, next.get(rhs)));
                transitions.put(lhs, row);
                transitionsCount += row.size();
            }
            results[lhs] = next;
        }
        addedColumns.clear();
        removedColumns.clear();
        return new TrackingResult(Map.of(), transitions);
    }

    private BitSet derive(int lhs, BitSet previous) {
        BitSet unresolved = eligible(lhs);
        BitSet valid = new BitSet(columns);
        if (prune) {
            for (int rhs = unresolved.nextSetBit(0); rhs >= 0; rhs = unresolved.nextSetBit(rhs + 1)) {

                if (previous != null && previous.get(rhs) && !addedColumns.get(lhs) && !removedColumns.get(rhs)) {
                    valid.set(rhs);
                    unresolved.clear(rhs);
                    if (removedColumns.get(lhs))
                        metrics.lhsDeletionValidSkipped(lhs);
                    else if (addedColumns.get(rhs))
                        metrics.rhsInsertionValidSkipped(lhs);
                    continue;
                }
                if (previous != null && !previous.get(rhs) && !removedColumns.get(lhs) && !addedColumns.get(rhs)) {
                    unresolved.clear(rhs);
                    if (addedColumns.get(lhs))
                        metrics.lhsInsertionInvalidSkipped(lhs);
                    else if (removedColumns.get(rhs))
                        metrics.rhsDeletionInvalidSkipped(lhs);
                    continue;
                }
                if (distinct[lhs] > distinct[rhs]) {
                    metrics.wholeCountPruned(lhs);
                    unresolved.clear(rhs);
                    continue;
                }
                if (partitions != null) {
                    var check = partitions.check(lhs, rhs);
                    metrics.partitionComparisons(lhs, check.comparisons4(), check.comparisons16(),
                            check.comparisonsFine());
                    if (check.rejected()) {
                        metrics.partitionCountPruned(lhs, check.rejectionLevel());
                        unresolved.clear(rhs);
                    }
                }
            }
            if (cqf != null && !unresolved.isEmpty()) {
                Set<CandidateKey> keys = new HashSet<>();
                for (int rhs = unresolved.nextSetBit(0); rhs >= 0; rhs = unresolved.nextSetBit(rhs + 1))
                    keys.add(new CandidateKey(bucketId, lhs, rhs));
                for (CandidateKey key : cqf.proposeWitnesses(keys).keySet()) {
                    unresolved.clear(key.rhsCol());
                    metrics.cqfPruned(lhs);
                }
            }
            if (transitive != null) {
                transitive.initializeValid(lhs, valid);
                BitSet proven = transitive.reachableFrom(lhs, noExcludedEdges);
                proven.and(unresolved);
                for (int rhs = proven.nextSetBit(0); rhs >= 0; rhs = proven.nextSetBit(rhs + 1))
                    metrics.transitivelyValidated(lhs);
                valid.or(proven);
                unresolved.andNot(proven);
            }
        }
        int tested = unresolved.cardinality();
        BitSet checked = clusters.validRhsSnapshot(lhs, unresolved);
        metrics.exactResults(lhs, tested, checked.cardinality());
        valid.or(checked);
        if (transitive != null)
            transitive.initializeValid(lhs, valid);
        return valid;
    }

    @Override
    public RoaringBitmap validRhsSnapshot(int lhs) {
        if (derivesAtDrain()) {
            if (!finalStarted) {
                finalStarted = true;
                dirtyLhsCount += clusters.takeAffectedLhs().cardinality();
                if (!options.changeDetection())
                    clusters.invalidateAll();
            }
            if (results[lhs] == null)
                results[lhs] = derive(lhs, null);
        }
        BitSet row = results[lhs] == null ? eligible(lhs) : results[lhs];
        RoaringBitmap snapshot = new RoaringBitmap();
        for (int rhs = row.nextSetBit(0); rhs >= 0; rhs = row.nextSetBit(rhs + 1))
            snapshot.add(rhs);
        return snapshot;
    }

    @Override
    public PruneMetrics metricsFor(int lhs) {
        return metrics.snapshot(lhs);
    }

    @Override
    public List<long[]> activeClusterSignatures() {
        return clusters.activeSignaturesSnapshot();
    }

    @Override
    public long[] derivationMetrics() {
        return new long[] { dirtyLhsCount, clusters.intersections(), clusters.signatureVisits(), transitionsCount };
    }
}
