package disIND.valueBased.membership;

import disIND.valueBased.monitor.WorkerPhaseMetrics;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.tracking.ModeSpecificContext;
import disIND.valueBased.monitor.WorkerPhaseMetrics.Phase;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Objects;

public final class MembershipUpdater {
    private final int bucketId;
    private final ValueOwnerMembershipStore membershipStore;
    private final ColumnSetFactory columnSets;
    private final ModeSpecificContext modeSpecificContext;
    private final WorkerPhaseMetrics phaseMetrics;

    public MembershipUpdater(int bucketId, ValueOwnerMembershipStore membershipStore,
            ColumnSetFactory columnSets, ModeSpecificContext modeSpecificContext, WorkerPhaseMetrics phaseMetrics) {
        this.bucketId = bucketId;
        this.membershipStore = membershipStore;
        this.columnSets = columnSets;
        this.modeSpecificContext = modeSpecificContext;
        this.phaseMetrics = Objects.requireNonNull(phaseMetrics);
    }

    // Return complete records after merging with updates that later write to Db.
    public MembershipBatchResult apply(Int2ObjectMap<Int2IntMap> updatesByValue) {
        // Loads the membership from the RocksDB store or caches if any
        long started = System.nanoTime();
        Int2ObjectMap<Int2IntMap> records = membershipStore.loadBatch(bucketId, updatesByValue.keySet());
        phaseMetrics.record(Phase.MEMBERSHIP_LOAD, System.nanoTime() - started);
        started = System.nanoTime();
        Int2ObjectMap<ColumnSet> addedColumnsByValue = new Int2ObjectOpenHashMap<>();

        for (Int2ObjectMap.Entry<Int2IntMap> valueEntry : updatesByValue.int2ObjectEntrySet()) {

            int valueId = valueEntry.getIntKey();
            Int2IntMap columnUpdates = valueEntry.getValue();
            Int2IntMap record = records.get(valueId);

            if (record == null)
                throw new IllegalStateException("No membership record loaded for value " + valueId);

            // Allocate only if a new membership is discovered.
            ColumnSet addedColumns = null;
            for (Int2IntMap.Entry columnEntry : columnUpdates.int2IntEntrySet()) {
                int columnId = columnEntry.getIntKey();
                int count = columnEntry.getIntValue();
                boolean wasPresent = record.containsKey(columnId);
                int previousCount = record.getOrDefault(columnId, 0);
                record.put(columnId, Math.addExact(previousCount, count));

                if (!wasPresent) {
                    if (addedColumns == null)
                        addedColumns = columnSets.create();
                    addedColumns.add(columnId);
                    modeSpecificContext.membershipAdded(columnId, valueId);
                }
            }

            if (addedColumns != null) {
                modeSpecificContext.membershipChanged(record, addedColumns);
                addedColumnsByValue.put(valueId, addedColumns);
            }
        }
        phaseMetrics.record(Phase.MEMBERSHIP_UPDATE, System.nanoTime() - started);

        return new MembershipBatchResult(records, addedColumnsByValue);
    }
}
