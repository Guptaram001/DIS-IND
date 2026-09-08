package disIND.valueBased.tracking;

import disIND.valueBased.structures.ValueOwnerMembershipStore;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public interface CandidateTracker {
    ViolationHandler createViolationHandler(int bucketId);

    TrackingResult apply(ViolationHandler changedViolation, Int2ObjectMap<Int2IntMap> updatedMembership,
            ValueOwnerMembershipStore store);

    default boolean persistsCandidateState() {
        return true;
    }
}
