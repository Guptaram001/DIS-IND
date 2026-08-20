package disIND.valueBased.tracking;

import disIND.valueBased.model.SharedModel.CandidateLocalStatus;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateKey;
import disIND.valueBased.structures.ValueOwnerMembershipStore.CandidateState;

import java.util.List;
import java.util.Map;

public record TrackingResult(Map<CandidateKey, CandidateState> changedStates,
                Map<Integer, List<CandidateLocalStatus>> transitionsByLhs) {
}
