package disIND.valueBased.tracking;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import disIND.valueBased.membership.CandidateIndex;
import disIND.valueBased.membership.CandidateSet;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.structures.ValueOwnerClusterIndex;
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

    public ExactCandidateTracker(ValueOwnerClusterIndex clusters, CandidateIndex candidateIndex,
            CandidateSet locallyRejectedCandidates) {
        this.clusters = Objects.requireNonNull(clusters, "clusters");
        this.candidateIndex = Objects.requireNonNull(candidateIndex, "candidateIndex");
        this.locallyRejectedCandidates = Objects.requireNonNull(locallyRejectedCandidates, "locallyRejectedCandidates");
    }

    private static final class ExactChanges implements CandidateViolationAfterApplyingUpdates {

        private final int bucketId;
        private final LongOpenHashSet candidates = new LongOpenHashSet();

        private ExactChanges(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(int lhsCol, int rhsCol, int valueId) {
            candidates.add(CandidateEvaluator.candidateKey(lhsCol, rhsCol));
        }

        @Override
        public void violationRepaired(int lhsCol, int rhsCol, int valueId) {
            candidates.add(CandidateEvaluator.candidateKey(lhsCol, rhsCol));
        }
    }

    @Override
    public CandidateViolationAfterApplyingUpdates newChanges(int bucketId) {
        return new ExactChanges(bucketId);
    }

    @Override
    public boolean persistsCandidateState() {
        return false;
    }

    @Override
    public TrackingResult apply(CandidateViolationAfterApplyingUpdates changes,
            Int2ObjectMap<Int2IntMap> updatedMembership, ValueOwnerMembershipStore store) {

        Objects.requireNonNull(updatedMembership, "updatedMembership");
        Objects.requireNonNull(store, "store");

        if (!(changes instanceof ExactChanges exactChanges)) {
            throw new IllegalArgumentException("Exact tracker received incompatible changes");
        }

        if (exactChanges.candidates.isEmpty()) {
            return new TrackingResult(Map.of(), new Int2ObjectOpenHashMap<>());
        }

        LongSet violationKeys = clusters.findViolationKeys(exactChanges.candidates);
        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs = new Int2ObjectOpenHashMap<>();
        LongIterator iterator = exactChanges.candidates.iterator();
        while (iterator.hasNext()) {
            long compactKey = iterator.nextLong();

            int lhsCol = lhsColumn(compactKey);
            int rhsCol = rhsColumn(compactKey);

            int index = candidateIndex.index(lhsCol, rhsCol);
            boolean rejectedBefore = locallyRejectedCandidates.contains(index);
            boolean rejectedAfter = violationKeys.contains(compactKey);
            if (rejectedAfter) {
                locallyRejectedCandidates.add(index);
            } else {
                locallyRejectedCandidates.remove(index);
            }

            if (rejectedBefore == rejectedAfter) {
                continue;
            }
            List<CandidateLocalStatus> transitions = transitionsByLhs.get(lhsCol);
            if (transitions == null) {
                transitions = new ArrayList<>();
                transitionsByLhs.put(lhsCol, transitions);
            }

            transitions.add(new CandidateLocalStatus(rhsCol, !rejectedAfter));
        }

        return new TrackingResult(Map.of(), transitionsByLhs);
    }

    private static int lhsColumn(long key) {
        return (int) (key >>> Integer.SIZE);
    }

    private static int rhsColumn(long key) {
        return (int) key;
    }
}