package disIND.valueBased.structures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import disIND.valueBased.actors.ValueOwnerActor;
import disIND.valueBased.actors.ValueOwnerActor.CandidateChanges;
import disIND.valueBased.actors.ValueOwnerActor.CandidateTracker;
import disIND.valueBased.actors.ValueOwnerActor.TrackingResult;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.WitnessState;

public final class WitnessCandidateTracker implements CandidateTracker {
    private final int witnessLimit;

    public WitnessCandidateTracker(int witnessLimit) {
        if (witnessLimit <= 0 || witnessLimit > ValueOwnerMembershipStore.MAX_WITNESSES) {
            throw new IllegalArgumentException( "Witness limit must be between 1 and "+ ValueOwnerMembershipStore.MAX_WITNESSES);
        }
        this.witnessLimit = witnessLimit;
    }

    private static final class WitnessDelta {
        private final LinkedHashSet<Integer> created = new LinkedHashSet<>();
        private final Set<Integer> repaired = new HashSet<>();
    }

    private final class WitnessChanges implements CandidateChanges {
        private final int bucketId;
        private final Map<CandidateKey, WitnessDelta> deltas = new HashMap<>();

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
            return deltas.computeIfAbsent(new CandidateKey(bucketId, lhsCol, rhsCol),ignored -> new WitnessDelta());
        }
    }

    @Override
    public CandidateChanges newChanges(int bucketId) {
        return new WitnessChanges(bucketId);
    }

    @Override
    public TrackingResult apply(CandidateChanges changes,Map<Integer, Map<Integer, Integer>> updatedMembership,
            ValueOwnerMembershipStore store) {
        if (!(changes instanceof WitnessChanges witnessChanges))
            throw new IllegalArgumentException("Witness tracker received incompatible changes");

        Set<CandidateKey> keys = witnessChanges.deltas.keySet();
        Map<CandidateKey, CandidateState> previousStates =store.loadCandidates(keys, CandidateTrackingMode.WITNESS);
        Map<CandidateKey, CandidateState> changedStates = new HashMap<>();
        Map<Integer, List<CandidateLocalStatus>> transitionsByLhs = new HashMap<>();

        witnessChanges.deltas.forEach((key, delta) -> {
            WitnessState before = (WitnessState) previousStates.get(key);
            WitnessState after = updateState(key, before, delta, updatedMembership, store);

            if (!after.equals(before))
                changedStates.put(key, after);
            if (before.rejected() != after.rejected()) {
                transitionsByLhs
                        .computeIfAbsent(key.lhsCol(), ignored -> new ArrayList<>())
                        .add(new CandidateLocalStatus(key.rhsCol(), !after.rejected()));
            }
        });

        return new TrackingResult(changedStates, transitionsByLhs);
    }

    private WitnessState updateState(CandidateKey key,WitnessState before,WitnessDelta delta,Map<Integer, Map<Integer, Integer>> updatedMembership,
            ValueOwnerMembershipStore store) {
        boolean removesStoredWitness = before.witnesses().stream().anyMatch(delta.repaired::contains);
        boolean hasRoomForCreatedWitness = before.witnesses().size() < witnessLimit && !delta.created.isEmpty();

        // Most updates should not touch one of this candidate's stored witnesses.
        if (!removesStoredWitness && !hasRoomForCreatedWitness)
            return before;

        LinkedHashSet<Integer> witnesses =new LinkedHashSet<>(before.witnesses());
        witnesses.removeAll(delta.repaired);
        for (int valueId : delta.created) {
            if (witnesses.size() == witnessLimit)
                break;
            witnesses.add(valueId);
        }

        if (witnesses.isEmpty() && before.rejected()) {
            return new WitnessState(store.findWitnesses(key.bucketId(),key.lhsCol(),key.rhsCol(),witnessLimit,updatedMembership));
        }
        return new WitnessState(List.copyOf(witnesses));
    }
}
