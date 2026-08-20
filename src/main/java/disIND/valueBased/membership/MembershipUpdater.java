package disIND.valueBased.membership;

import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.tracking.ModeSpecificContext;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.util.HashMap;
import java.util.Map;

public final class MembershipUpdater {
    private final int bucketId;
    private final ValueOwnerMembershipStore membershipStore;
    private final ColumnSetFactory columnSets;
    private final ModeSpecificContext modeSpecificContext;

    public MembershipUpdater(int bucketId, ValueOwnerMembershipStore membershipStore,
            ColumnSetFactory columnSets, ModeSpecificContext modeSpecificContext) {
        this.bucketId = bucketId;
        this.membershipStore = membershipStore;
        this.columnSets = columnSets;
        this.modeSpecificContext = modeSpecificContext;
    }

    // Return complete records after merging with updates that later write to Db.
    public MembershipBatchResult apply(Map<Integer, Int2IntMap> updatesByValue) {
        // Loads the membership from the RocksDB store or caches if any
        Map<Integer, Int2IntMap> records = membershipStore.loadBatch(bucketId, updatesByValue.keySet());
        Map<Integer, ColumnSet> addedColumnsByValue = new HashMap<>();

        updatesByValue.forEach((valueId, columnUpdates) -> {
            Int2IntMap record = records.get(valueId);
            if (record == null)
                throw new IllegalStateException("No membership record loaded for value " + valueId);

            ColumnSet addedColumns = columnSets.create();
            columnUpdates.forEach((columnId, count) -> {
                int primitiveColumnId = columnId.intValue();
                int primitiveCount = count.intValue();
                int previousCount = record.getOrDefault(primitiveColumnId, 0);
                boolean wasPresent = record.containsKey(primitiveColumnId);
                record.put(primitiveColumnId, Math.addExact(previousCount, primitiveCount));
                if (!wasPresent) {
                    addedColumns.add(primitiveColumnId);
                    modeSpecificContext.membershipAdded(primitiveColumnId, valueId);
                }
            });

            if (!addedColumns.isEmpty()) {
                modeSpecificContext.membershipChanged(record, addedColumns);
                addedColumnsByValue.put(valueId, addedColumns);
            }
        });

        return new MembershipBatchResult(records, addedColumnsByValue);
    }
}
