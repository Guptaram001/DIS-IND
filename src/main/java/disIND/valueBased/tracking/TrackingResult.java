package disIND.valueBased.tracking;

import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.List;
import java.util.Map;

public record TrackingResult(Map<CandidateKey, CandidateState> changedStates,
        Int2ObjectMap<List<CandidateLocalStatus>> transitionsByLhs) {
}
