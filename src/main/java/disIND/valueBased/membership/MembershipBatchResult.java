package disIND.valueBased.membership;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public record MembershipBatchResult(
                Int2ObjectMap<Int2IntMap> previousRecordsByValue,
                Int2ObjectMap<Int2IntMap> updatedRecordsByValue,
                Int2ObjectMap<ColumnSet> newlyAddedColumnsByValue,
                Int2ObjectMap<ColumnSet> newlyRemovedColumnsByValue) {
}
