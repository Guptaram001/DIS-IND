package disIND.valueBased.structures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


import disIND.valueBased.actors.ValueOwnerActor.CandidateChanges;
import disIND.valueBased.actors.ValueOwnerActor.CandidateTracker;
import disIND.valueBased.actors.ValueOwnerActor.TrackingResult;
import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import disIND.valueBased.structures.ValueOwnerMembershipStore.PruneState;

public final class PruneCandidateTracker implements CandidateTracker {
    private final ValueOwnerCqf cqf;
    private final int[] localDistinctCounts;

    public PruneCandidateTracker(ValueOwnerCqf cqf,int[] localDistinctCounts) {
        this.cqf = Objects.requireNonNull(cqf, "cqf");
        this.localDistinctCounts = Objects.requireNonNull(localDistinctCounts,"localDistinctCounts");
    }

    private static final class PruneDelta {

        private int createdWitness =PruneState.NO_WITNESS;
        private final Set<Integer> repairedValues =new HashSet<>();

        private boolean hasCreatedWitness() {
            return createdWitness != PruneState.NO_WITNESS;
        }
    }

    private static final class PruneChanges implements CandidateChanges {
        private final int bucketId;
        private final Map<CandidateKey, PruneDelta> deltas =new HashMap<>();

        private PruneChanges(int bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void violationCreated(int lhsCol,int rhsCol,int valueId) {
            PruneDelta delta =delta(lhsCol, rhsCol);

            if (!delta.hasCreatedWitness()) {
                delta.createdWitness = valueId;
            }
        }

        @Override
        public void violationRepaired(int lhsCol,int rhsCol,int valueId) {
            delta(lhsCol, rhsCol).repairedValues.add(valueId);
        }

        private PruneDelta delta(int lhsCol,int rhsCol) {
            CandidateKey key =new CandidateKey(bucketId,lhsCol,rhsCol);
            return deltas.computeIfAbsent(key,ignored -> new PruneDelta());
        }
    }

    @Override
    public CandidateChanges newChanges(int bucketId) {
        return new PruneChanges(bucketId);
    }

    @Override
    public TrackingResult apply(CandidateChanges changes,Map<Integer, Map<Integer, Integer>>
                    updatedMembership,ValueOwnerMembershipStore store) {

        if (!(changes instanceof PruneChanges pruneChanges)) 
            throw new IllegalArgumentException("Prune tracker received incompatible changes");

        Set<CandidateKey> keys =pruneChanges.deltas.keySet();
        if (keys.isEmpty()) 
            return new TrackingResult(Map.of(),Map.of());
        

        Map<CandidateKey, CandidateState> previousStates =store.loadCandidates(keys,CandidateTrackingMode.PRUNE);
        Map<CandidateKey, PruneState> nextStates =new HashMap<>();

        Set<CandidateKey> unresolved =new LinkedHashSet<>();
        Set<CandidateKey> cardinalityPruned =new LinkedHashSet<>();

        pruneChanges.deltas.forEach((key, delta) -> {
            PruneState before = (PruneState) previousStates.get(key);

            if (delta.hasCreatedWitness()) {
                nextStates.put(key,PruneState.rejected(delta.createdWitness));
                return;
            }

            if (!before.rejected()) {
                nextStates.put(key, before);
                return;
            }

            if (before.hasWitness()&& !delta.repairedValues.contains(before.witnessValueId())) {
                nextStates.put(key, before);
                return;
            }

            int lhsDistinct = distinctCount(key.lhsCol());
            int rhsDistinct = distinctCount(key.rhsCol());
            if (lhsDistinct > rhsDistinct) {
                nextStates.put(key,PruneState.rejectedByCardinality());
                cardinalityPruned.add(key);
                return;
            }

            unresolved.add(key);
        });

        Map<CandidateKey, Integer> cqfProposals = cqf.proposeWitnesses(unresolved);
        Map<CandidateKey, Integer> cqfWitnesses = store.verifyCandidateWitnesses(pruneChanges.bucketId, cqfProposals, updatedMembership);

        Set<CandidateKey> exactScanCandidates = new LinkedHashSet<>(unresolved);
        exactScanCandidates.removeAll(cqfWitnesses.keySet());

        Map<CandidateKey, Integer> scannedWitnesses = new HashMap<>(cqfWitnesses);
        scannedWitnesses.putAll(store.findOneWitnessPerCandidate(pruneChanges.bucketId,exactScanCandidates,
                updatedMembership));

        for (CandidateKey key : unresolved) {
            Integer witness =scannedWitnesses.get(key);
            PruneState after =witness == null? PruneState.valid(): PruneState.rejected(witness);
            nextStates.put(key, after);
        }

        Map<CandidateKey, CandidateState> changedStates =new HashMap<>();

        Map<Integer, List<CandidateLocalStatus>>transitionsByLhs = new HashMap<>();

        for (CandidateKey key : keys) {
            PruneState before =(PruneState) previousStates.get(key);
            PruneState after =nextStates.get(key);
            if (after == null) 
                throw new IllegalStateException("No prune result for candidate "+ key);
            
            if (!after.equals(before)) 
                changedStates.put(key, after);

            if (before.rejected() != after.rejected()) {
                transitionsByLhs.computeIfAbsent(key.lhsCol(),ignored -> new ArrayList<>())
                        .add(new CandidateLocalStatus(key.rhsCol(),!after.rejected()));
            }
        }

        return new TrackingResult(changedStates,transitionsByLhs);
    }

    private int distinctCount(int columnId) {
        if (columnId < 0 || columnId >= localDistinctCounts.length) {
            throw new IllegalArgumentException("Column ID outside local distinct-count array: "+ columnId);
        }
        return localDistinctCounts[columnId];
    }
}
