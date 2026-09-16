package disIND.valueBased.tracking;

import java.util.*;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.WitnessState;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;

public final class WitnessCandidateTracker implements CandidateTracker {
    private final int witnessLimit;

    public WitnessCandidateTracker(int witnessLimit) {
        if (witnessLimit <= 0 || witnessLimit > ValueOwnerMembershipStore.MAX_WITNESSES)
            throw new IllegalArgumentException("Invalid witness limit: " + witnessLimit);
        this.witnessLimit = witnessLimit;
    }

    private static final class WitnessViolationHandler implements ViolationHandler {
        private java.util.function.IntConsumer comparisons = ignored -> {
        };

        @Override
        public void comparisonRecorder(java.util.function.IntConsumer recorder) {
            comparisons = recorder;
        }

        private final int bucketId;
        private final LongSet candidates = new LongOpenHashSet();

        WitnessViolationHandler(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public boolean deferCandidate(int lhs, int rhs) {
            candidates.add(candidateKey(lhs, rhs));
            return true;
        }

        @Override
        public void violationCreated(int lhs, int rhs, int value) {
            deferCandidate(lhs, rhs);
        }

        @Override
        public void violationRepaired(int lhs, int rhs, int value) {
            deferCandidate(lhs, rhs);
        }
    }

    @Override
    public ViolationHandler createViolationHandler(int bucketId) {
        return new WitnessViolationHandler(bucketId);
    }

    @Override
    public TrackingResult apply(ViolationHandler changes,
            Int2ObjectMap<Int2IntMap> updatedMembership, ValueOwnerMembershipStore store) {
        if (!(changes instanceof WitnessViolationHandler handler))
            throw new IllegalArgumentException("Witness tracker received incompatible changes");
        if (handler.candidates.isEmpty())
            return new TrackingResult(Map.of(), new Int2ObjectOpenHashMap<>());

        Set<CandidateKey> keys = new HashSet<>();
        for (long pair : handler.candidates)
            keys.add(new CandidateKey(handler.bucketId, lhsColumn(pair), rhsColumn(pair)));
        Map<CandidateKey, CandidateState> previous = store.loadCandidates(keys, CandidateTrackingMode.WITNESS);
        Map<CandidateKey, CandidateState> changedStates = new HashMap<>();
        LongSet needsRecovery = new LongOpenHashSet();

        for (CandidateKey key : keys) {
            WitnessState before = (WitnessState) previous.get(key);
            IntArrayList retained = new IntArrayList(witnessLimit);
            for (int value : before.witnesses()) {
                Int2IntMap record = updatedMembership.get(value);
                if (record != null)
                    handler.comparisons.accept(key.lhsCol());
                if (record == null || violates(record, key))
                    retained.add(value);
            }

            if (retained.isEmpty()) {
                for (var entry : updatedMembership.int2ObjectEntrySet()) {
                    handler.comparisons.accept(key.lhsCol());
                    if (violates(entry.getValue(), key))
                        retained.add(entry.getIntKey());
                    if (retained.size() == witnessLimit)
                        break;
                }

                if (retained.isEmpty() && before.rejected())
                    needsRecovery.add(candidateKey(key.lhsCol(), key.rhsCol()));
            }
            WitnessState after = new WitnessState(retained.toIntArray());
            if (!after.equals(before))
                changedStates.put(key, after);
        }

        Long2ObjectMap<int[]> recovered = store.findWitnessesBatch(
                handler.bucketId, needsRecovery, witnessLimit, updatedMembership);
        for (long pair : needsRecovery)
            changedStates.put(new CandidateKey(handler.bucketId, lhsColumn(pair), rhsColumn(pair)),
                    new WitnessState(recovered.get(pair)));

        Int2ObjectMap<List<CandidateLocalStatus>> transitions = new Int2ObjectOpenHashMap<>();
        var iterator = changedStates.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            CandidateKey key = entry.getKey();
            WitnessState before = (WitnessState) previous.get(key);
            WitnessState after = (WitnessState) entry.getValue();
            if (after.equals(before)) {
                iterator.remove();
                continue;
            }
            if (before.rejected() != after.rejected())
                transitions.computeIfAbsent(key.lhsCol(), ignored -> new ArrayList<>())
                        .add(new CandidateLocalStatus(key.rhsCol(), !after.rejected()));
        }
        return new TrackingResult(changedStates, transitions);
    }

    private static boolean violates(Int2IntMap record, CandidateKey key) {
        return record.containsKey(key.lhsCol()) && !record.containsKey(key.rhsCol());
    }

    public static long candidateKey(int lhs, int rhs) {
        return ((long) lhs << Integer.SIZE) | (rhs & 0xffffffffL);
    }

    private static int lhsColumn(long pair) {
        return (int) (pair >>> Integer.SIZE);
    }

    private static int rhsColumn(long pair) {
        return (int) pair;
    }
}
