package disIND.valueBased.tracking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CountState;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntMaps;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public final class CountCandidateTracker implements CandidateTracker {

    private static final class CountChanges implements CandidateViolationAfterApplyingUpdates {
        private final int bucketId;
        private final Long2IntOpenHashMap deltas = new Long2IntOpenHashMap();

        private CountChanges(int bucketId) {
            this.bucketId = bucketId;
            deltas.defaultReturnValue(0);
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
            long key = CandidateEvaluator.candidateKey(lhsCol, rhsCol);
            int previous = deltas.get(key);
            int next = Math.addExact(previous, delta);
            if (next == 0)
                deltas.remove(key);
            else
                deltas.put(key, next);
        }
    }

    @Override
    public CandidateViolationAfterApplyingUpdates newChanges(int bucketId) {
        return new CountChanges(bucketId);
    }

    @Override
    public TrackingResult apply(CandidateViolationAfterApplyingUpdates changes,
            Int2ObjectMap<Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store) {
        if (!(changes instanceof CountChanges countChanges))
            throw new IllegalArgumentException("Count tracker received incompatible changes");

        Set<CandidateKey> keys = new HashSet<>(countChanges.deltas.size());
        ObjectIterator<Long2IntMap.Entry> iterator = Long2IntMaps.fastIterator(countChanges.deltas);

        while (iterator.hasNext()) {
            long compactKey = iterator.next().getLongKey();
            int lhsCol = lhsColumn(compactKey);
            int rhsCol = rhsColumn(compactKey);
            keys.add(new CandidateKey(countChanges.bucketId, lhsCol, rhsCol));
        }

        if (keys.isEmpty()) {
            return new TrackingResult(Map.of(), new Int2ObjectOpenHashMap<>());
        }

        Map<CandidateKey, CandidateState> previousStates = store.loadCandidates(keys, CandidateTrackingMode.COUNT);
        Map<CandidateKey, CandidateState> changedStates = new HashMap<>(keys.size());
        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs = new Int2ObjectOpenHashMap<>();

        for (CandidateKey key : keys) {
            long compactKey = CandidateEvaluator.candidateKey(key.lhsCol(), key.rhsCol());
            int delta = countChanges.deltas.get(compactKey);
            CountState before = (CountState) previousStates.get(key);
            if (before == null)
                throw new IllegalStateException("No previous count state for candidate " + key);

            int previousCount = before.violationCount();
            int nextCount = Math.addExact(previousCount, delta);

            if (nextCount < 0) {
                throw new IllegalStateException("Negative violation count ");
            }
            CountState after = new CountState(nextCount);

            changedStates.put(key, after);
            boolean wasRejected = previousCount > 0;
            boolean nowRejected = nextCount > 0;
            if (wasRejected != nowRejected) {
                int lhsCol = key.lhsCol();
                List<CandidateLocalStatus> transitions = transitionsByLhs.get(lhsCol);
                if (transitions == null) {
                    transitions = new ArrayList<>();
                    transitionsByLhs.put(lhsCol, transitions);
                }

                transitions.add(new CandidateLocalStatus(key.rhsCol(), !nowRejected));
            }
        }
        return new TrackingResult(changedStates, transitionsByLhs);
    }

    private static int lhsColumn(long key) {
        return (int) (key >>> Integer.SIZE);
    }

    private static int rhsColumn(long key) {
        return (int) key;
    }
}
