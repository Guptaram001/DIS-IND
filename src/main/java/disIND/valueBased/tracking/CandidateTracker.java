package disIND.valueBased.tracking;

import disIND.valueBased.structures.ValueOwnerMembershipStore;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public interface CandidateTracker {
    CandidateViolationAfterApplyingUpdates newChanges(int bucketId);

    TrackingResult apply(CandidateViolationAfterApplyingUpdates candidateViolationAfterApplyingUpdates,
            Int2ObjectMap<Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store);
}
