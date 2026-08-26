package disIND.valueBased.tracking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.ValueOwnerClusterIndex;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.ExactState;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public final class ExactCandidateTracker implements CandidateTracker {

    private final ValueOwnerClusterIndex clusters;

    public ExactCandidateTracker(ValueOwnerClusterIndex clusters) {
        this.clusters = Objects.requireNonNull(clusters, "clusters");
    }

    private static final class ExactChanges
            implements CandidateViolationAfterApplyingUpdates {

        private final int bucketId;
        private final LongOpenHashSet candidates = new LongOpenHashSet();

        private ExactChanges(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(
                int lhsCol,
                int rhsCol,
                int valueId) {

            candidates.add(
                    CandidateEvaluator.candidateKey(lhsCol, rhsCol));
        }

        @Override
        public void violationRepaired(
                int lhsCol,
                int rhsCol,
                int valueId) {

            candidates.add(
                    CandidateEvaluator.candidateKey(lhsCol, rhsCol));
        }
    }

    @Override
    public CandidateViolationAfterApplyingUpdates newChanges(
            int bucketId) {

        return new ExactChanges(bucketId);
    }

    @Override
    public TrackingResult apply(
            CandidateViolationAfterApplyingUpdates changes,
            Int2ObjectMap<Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store) {

        Objects.requireNonNull(updatedMembership, "updatedMembership");
        Objects.requireNonNull(store, "store");

        if (!(changes instanceof ExactChanges exactChanges)) {
            throw new IllegalArgumentException(
                    "Exact tracker received incompatible changes");
        }

        Set<CandidateKey> keys = new HashSet<>(exactChanges.candidates.size());

        LongIterator iterator = exactChanges.candidates.iterator();

        while (iterator.hasNext()) {
            long compactKey = iterator.nextLong();

            keys.add(new CandidateKey(
                    exactChanges.bucketId,
                    lhsColumn(compactKey),
                    rhsColumn(compactKey)));
        }

        if (keys.isEmpty()) {
            return new TrackingResult(
                    Map.of(),
                    new Int2ObjectOpenHashMap<>());
        }

        /*
         * This is the unconditional exact scan.
         *
         * There is no cardinality check, partition check, CQF check,
         * witness lookup, or violation-count calculation before it.
         */
        LongSet violationKeys = clusters.findViolationKeys(
                exactChanges.candidates);

        Map<CandidateKey, CandidateState> previousStates = store.loadCandidates(
                keys,
                CandidateTrackingMode.EXACT);

        Map<CandidateKey, CandidateState> changedStates = new HashMap<>(keys.size());

        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs = new Int2ObjectOpenHashMap<>();

        for (CandidateKey key : keys) {
            ExactState before = (ExactState) previousStates.get(key);

            if (before == null) {
                throw new IllegalStateException(
                        "No previous exact state for " + key);
            }

            long compactKey = CandidateEvaluator.candidateKey(
                    key.lhsCol(),
                    key.rhsCol());
            boolean rejected = violationKeys.contains(compactKey);

            ExactState after = rejected
                    ? ExactState.rejectedState()
                    : ExactState.valid();

            if (after.equals(before)) {
                continue;
            }

            changedStates.put(key, after);

            List<CandidateLocalStatus> transitions = transitionsByLhs.get(key.lhsCol());

            if (transitions == null) {
                transitions = new ArrayList<>();
                transitionsByLhs.put(
                        key.lhsCol(),
                        transitions);
            }

            transitions.add(
                    new CandidateLocalStatus(
                            key.rhsCol(),
                            !rejected));
        }

        return new TrackingResult(
                changedStates,
                transitionsByLhs);
    }

    private static int lhsColumn(long key) {
        return (int) (key >>> Integer.SIZE);
    }

    private static int rhsColumn(long key) {
        return (int) key;
    }
}