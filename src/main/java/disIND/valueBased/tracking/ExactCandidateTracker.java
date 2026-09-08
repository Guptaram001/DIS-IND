package disIND.valueBased.tracking;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import disIND.valueBased.membership.CandidateIndex;
import disIND.valueBased.membership.CandidateSet;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.structures.ValueOwnerClusterIndex;
import disIND.valueBased.structures.PruneMetricsCollector;
import disIND.valueBased.structures.ValueOwnerMembershipStore;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public final class ExactCandidateTracker implements CandidateTracker {

    private final ValueOwnerClusterIndex clusters;
    private final CandidateIndex candidateIndex;
    private final CandidateSet locallyRejectedCandidates;
    private final boolean directViolationEnabled;
    private final PruneMetricsCollector metrics;

    public ExactCandidateTracker(ValueOwnerClusterIndex clusters, CandidateIndex candidateIndex,
            CandidateSet locallyRejectedCandidates) {
        this(clusters, candidateIndex, locallyRejectedCandidates, false);
    }

    public ExactCandidateTracker(ValueOwnerClusterIndex clusters, CandidateIndex candidateIndex,
            CandidateSet locallyRejectedCandidates, boolean directViolationEnabled) {
        this(clusters, candidateIndex, locallyRejectedCandidates, directViolationEnabled, null);
    }

    public ExactCandidateTracker(ValueOwnerClusterIndex clusters, CandidateIndex candidateIndex,
            CandidateSet locallyRejectedCandidates, boolean directViolationEnabled, PruneMetricsCollector metrics) {
        this.metrics = metrics;
        this.clusters = Objects.requireNonNull(clusters, "clusters");
        this.candidateIndex = Objects.requireNonNull(candidateIndex, "candidateIndex");
        this.locallyRejectedCandidates = Objects.requireNonNull(locallyRejectedCandidates, "locallyRejectedCandidates");
        this.directViolationEnabled = directViolationEnabled;
    }

    private static final class ExactViolationHandler implements ViolationHandler {

        private final int bucketId;
        private final LongOpenHashSet createdViolations = new LongOpenHashSet();
        private final LongOpenHashSet repairedViolations = new LongOpenHashSet();

        private ExactViolationHandler(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(int lhsCol, int rhsCol, int valueId) {
            createdViolations.add(CandidateEvaluator.candidateKey(lhsCol, rhsCol));
        }

        @Override
        public void violationRepaired(int lhsCol, int rhsCol, int valueId) {
            repairedViolations.add(CandidateEvaluator.candidateKey(lhsCol, rhsCol));
        }
    }

    @Override
    public ViolationHandler createViolationHandler(int bucketId) {
        return new ExactViolationHandler(bucketId);
    }

    @Override
    public boolean persistsCandidateState() {
        return false;
    }

    @Override
    public TrackingResult apply(ViolationHandler violationHandler, Int2ObjectMap<Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store) {

        Objects.requireNonNull(updatedMembership, "updatedMembership");
        Objects.requireNonNull(store, "store");

        if (!(violationHandler instanceof ExactViolationHandler exactViolationHandler)) {
            throw new IllegalArgumentException("Exact tracker received incompatible changes");
        }

        if (exactViolationHandler.createdViolations.isEmpty() && exactViolationHandler.repairedViolations.isEmpty()) {
            return new TrackingResult(Map.of(), new Int2ObjectOpenHashMap<>());
        }

        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs = new Int2ObjectOpenHashMap<>();

        LongOpenHashSet candidatesToValidate = new LongOpenHashSet(exactViolationHandler.repairedViolations);
        if (directViolationEnabled) {
            LongIterator created = exactViolationHandler.createdViolations.iterator();
            while (created.hasNext())
                recordStatus(created.nextLong(), true, transitionsByLhs);
            candidatesToValidate.removeAll(exactViolationHandler.createdViolations);
        } else {
            candidatesToValidate.addAll(exactViolationHandler.createdViolations);
        }

        LongSet violationKeys = clusters.findViolationKeys(candidatesToValidate);
        LongIterator iterator = candidatesToValidate.iterator();
        while (iterator.hasNext()) {
            long compactKey = iterator.nextLong();
            boolean rejectedAfter = violationKeys.contains(compactKey);
            // Count each deduplicated candidate actually submitted to cluster validation.
            // Direct rejections above do not perform a cluster check.
            if (metrics != null) {
                int lhsCol = lhsColumn(compactKey);
                metrics.exactTested(lhsCol);
                if (rejectedAfter)
                    metrics.exactRejected(lhsCol);
                else
                    metrics.exactValidated(lhsCol);
            }
            recordStatus(compactKey, rejectedAfter, transitionsByLhs);
        }

        return new TrackingResult(Map.of(), transitionsByLhs);
    }

    private void recordStatus(long compactKey, boolean rejectedAfter,
            Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs) {
        int lhsCol = lhsColumn(compactKey);
        int rhsCol = rhsColumn(compactKey);
        int index = candidateIndex.index(lhsCol, rhsCol);
        boolean rejectedBefore = locallyRejectedCandidates.contains(index);

        if (rejectedAfter)
            locallyRejectedCandidates.add(index);
        else
            locallyRejectedCandidates.remove(index);

        if (rejectedBefore == rejectedAfter)
            return;

        List<CandidateLocalStatus> transitions = transitionsByLhs.get(lhsCol);
        if (transitions == null) {
            transitions = new ArrayList<>();
            transitionsByLhs.put(lhsCol, transitions);
        }
        transitions.add(new CandidateLocalStatus(rhsCol, !rejectedAfter));
    }

    private static int lhsColumn(long key) {
        return (int) (key >>> Integer.SIZE);
    }

    private static int rhsColumn(long key) {
        return (int) key;
    }
}
