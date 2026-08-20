package disIND.valueBased.tracking;

import disIND.valueBased.structures.ValueOwnerMembershipStore;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.util.Map;

public interface CandidateTracker {
    CandidateViolationAfterApplyingUpdates newChanges(int bucketId);

    TrackingResult apply(CandidateViolationAfterApplyingUpdates candidateViolationAfterApplyingUpdates,
            Map<Integer, Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store);
}
