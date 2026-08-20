package disIND.valueBased.membership;

import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.util.Map;

public record MembershipBatchResult(
        Map<Integer, Int2IntMap> updatedRecordsByValue,
        Map<Integer, ColumnSet> newlyAddedColumnsByValue) {
}
