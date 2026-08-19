package disIND.valueBased.structures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import disIND.valueBased.actors.ValueOwnerActor.CandidateChanges;
import disIND.valueBased.actors.ValueOwnerActor.CandidateTracker;
import disIND.valueBased.actors.ValueOwnerActor.TrackingResult;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CountState;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

public final class CountCandidateTracker implements CandidateTracker {

    private static final class CountChanges implements CandidateChanges {
        private final int bucketId;
        private final Map<CandidateKey, Integer> deltas = new HashMap<>();

        private CountChanges(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(int lhsCol, int rhsCol, int valueId) {
            merge(lhsCol, rhsCol, 1);
        }

        @Override
        public void violationRepaired(int lhsCol, int rhsCol, int valueId) {
            merge(lhsCol, rhsCol, -1);
        }

        private void merge(int lhsCol, int rhsCol, int delta) {
            CandidateKey key = new CandidateKey(bucketId, lhsCol, rhsCol);
            deltas.merge(key, delta, Math::addExact);
            if (deltas.get(key) == 0)
                deltas.remove(key);
        }
    }

    @Override
    public CandidateChanges newChanges(int bucketId) {
        return new CountChanges(bucketId);
    }

    @Override
    public TrackingResult apply(CandidateChanges changes,Map<Integer, Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store) {
        if (!(changes instanceof CountChanges countChanges))
            throw new IllegalArgumentException("Count tracker received incompatible changes");

        Set<CandidateKey> keys = countChanges.deltas.keySet();
        Map<CandidateKey, CandidateState> previousStates =store.loadCandidates(keys, CandidateTrackingMode.COUNT);
        Map<CandidateKey, CandidateState> changedStates = new HashMap<>();
        Map<Integer, List<CandidateLocalStatus>> transitionsByLhs = new HashMap<>();

        countChanges.deltas.forEach((key, delta) -> {
            CountState before = (CountState) previousStates.get(key);
            int nextCount = Math.addExact(before.violationCount(), delta);
            if (nextCount < 0) {
                throw new IllegalStateException("Negative violation count for candidate " + key
                                + ": previous=" + before.violationCount()+ ", delta=" + delta);
            }

            CountState after = new CountState(nextCount);
            if (!after.equals(before))
                changedStates.put(key, after);
            if (before.rejected() != after.rejected()) {
                transitionsByLhs.computeIfAbsent(key.lhsCol(), ignored -> new ArrayList<>())
                        .add(new CandidateLocalStatus(key.rhsCol(), !after.rejected()));
            }
        });

        return new TrackingResult(changedStates, transitionsByLhs);
    }
}
