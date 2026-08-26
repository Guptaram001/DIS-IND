package disIND.valueBased.tracking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.WitnessState;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

public final class WitnessCandidateTracker implements CandidateTracker {
    private final int witnessLimit;

    public WitnessCandidateTracker(int witnessLimit) {
        if (witnessLimit <= 0 || witnessLimit > ValueOwnerMembershipStore.MAX_WITNESSES) {
            throw new IllegalArgumentException(
                    "Witness limit must be between 1 and " + ValueOwnerMembershipStore.MAX_WITNESSES);
        }
        this.witnessLimit = witnessLimit;
    }

    private static final class WitnessDelta {
        private final IntLinkedOpenHashSet created = new IntLinkedOpenHashSet();
        private final IntOpenHashSet repaired = new IntOpenHashSet();
    }

    private final class WitnessChanges implements CandidateViolationAfterApplyingUpdates {
        private final int bucketId;
        private final Long2ObjectOpenHashMap<WitnessDelta> deltas = new Long2ObjectOpenHashMap<>();

        private WitnessChanges(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(int lhsCol, int rhsCol, int valueId) {
            WitnessDelta delta = delta(lhsCol, rhsCol);
            if (delta.created.size() < witnessLimit)
                delta.created.add(valueId);
        }

        @Override
        public void violationRepaired(int lhsCol, int rhsCol, int valueId) {
            delta(lhsCol, rhsCol).repaired.add(valueId);
        }

        private WitnessDelta delta(int lhsCol, int rhsCol) {
            long compactKey = CandidateEvaluator.candidateKey(lhsCol, rhsCol);
            WitnessDelta delta = deltas.get(compactKey);
            if (delta == null) {
                delta = new WitnessDelta();
                deltas.put(compactKey, delta);
            }
            return delta;
        }

    }

    @Override
    public CandidateViolationAfterApplyingUpdates newChanges(int bucketId) {
        return new WitnessChanges(bucketId);
    }

    @Override
    public TrackingResult apply(CandidateViolationAfterApplyingUpdates changes,
            Int2ObjectMap<Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store) {
        if (!(changes instanceof WitnessChanges witnessChanges))
            throw new IllegalArgumentException("Witness tracker received incompatible changes");

        if (witnessChanges.deltas.isEmpty())
            return new TrackingResult(Map.of(), new Int2ObjectOpenHashMap<>());

        Set<CandidateKey> keys = new ObjectOpenHashSet<>(witnessChanges.deltas.size());
        ObjectIterator<Long2ObjectMap.Entry<WitnessDelta>> iterator = Long2ObjectMaps
                .fastIterator(witnessChanges.deltas);
        while (iterator.hasNext()) {
            long compactKey = iterator.next().getLongKey();
            keys.add(new CandidateKey(witnessChanges.bucketId, lhsColumn(compactKey), rhsColumn(compactKey)));
        }
        Map<CandidateKey, CandidateState> previousStates = store.loadCandidates(keys, CandidateTrackingMode.WITNESS);
        Map<CandidateKey, CandidateState> changedStates = new HashMap<>(keys.size());

        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs = new Int2ObjectOpenHashMap<>();
        iterator = Long2ObjectMaps.fastIterator(witnessChanges.deltas);

        while (iterator.hasNext()) {
            Long2ObjectMap.Entry<WitnessDelta> entry = iterator.next();
            long compactKey = entry.getLongKey();
            CandidateKey key = new CandidateKey(witnessChanges.bucketId, lhsColumn(compactKey), rhsColumn(compactKey));
            WitnessState before = (WitnessState) previousStates.get(key);
            if (before == null)
                throw new IllegalStateException("No previous witness state for " + key);

            WitnessState after = updateState(key, before, entry.getValue(), updatedMembership, store);

            if (!after.equals(before))
                changedStates.put(key, after);

            if (before.rejected() != after.rejected()) {
                int lhsCol = key.lhsCol();
                List<CandidateLocalStatus> transitions = transitionsByLhs.get(lhsCol);
                if (transitions == null) {
                    transitions = new ArrayList<>();
                    transitionsByLhs.put(lhsCol, transitions);
                }
                transitions.add(new CandidateLocalStatus(key.rhsCol(), !after.rejected()));
            }
        }
        return new TrackingResult(changedStates, transitionsByLhs);
    }

    private WitnessState updateState(CandidateKey key, WitnessState before, WitnessDelta delta,
            Int2ObjectMap<Int2IntMap> updatedMembership, ValueOwnerMembershipStore store) {

        int[] beforeWitnesses = before.witnesses();
        boolean removesStoredWitness = false;
        for (int valueId : before.witnesses()) {
            if (delta.repaired.contains(valueId)) {
                removesStoredWitness = true;
                break;
            }
        }
        boolean hasRoomForCreatedWitness = before.witnesses().length < witnessLimit && !delta.created.isEmpty();

        if (!removesStoredWitness && !hasRoomForCreatedWitness)
            return before;

        IntLinkedOpenHashSet witnesses = new IntLinkedOpenHashSet(witnessLimit);
        for (int valueId : before.witnesses()) {
            if (!delta.repaired.contains(valueId))
                witnesses.add(valueId);
        }

        IntIterator createdIterator = delta.created.iterator();
        while (createdIterator.hasNext() && witnesses.size() < witnessLimit) {
            witnesses.add(createdIterator.nextInt());
        }

        if (witnesses.isEmpty() && before.rejected())
            return new WitnessState(store.findWitnesses(key.bucketId(), key.lhsCol(), key.rhsCol(),
                    witnessLimit, updatedMembership));

        return new WitnessState(witnesses.toIntArray());
    }

    public static long candidateKey(int lhsCol, int rhsCol) {
        return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
    }

    private static int lhsColumn(long compactKey) {
        return (int) (compactKey >>> Integer.SIZE);
    }

    private static int rhsColumn(long compactKey) {
        return (int) compactKey;
    }
}
