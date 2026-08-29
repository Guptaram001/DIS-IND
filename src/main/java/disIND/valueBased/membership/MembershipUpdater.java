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
        Int2ObjectMap<ColumnSet> removedColumnsByValue = new Int2ObjectOpenHashMap<>();
        Int2ObjectMap<Int2IntMap> previousRecordsByValue = new Int2ObjectOpenHashMap<>();

        for (Int2ObjectMap.Entry<Int2IntMap> valueEntry : updatesByValue.int2ObjectEntrySet()) {

            int valueId = valueEntry.getIntKey();
            Int2IntMap columnUpdates = valueEntry.getValue();
            Int2IntMap record = records.get(valueId);
            if (record == null)
                throw new IllegalStateException("No membership record loaded for value ");
            previousRecordsByValue.put(valueId, new AdaptiveColumnCounts(record));

            // Allocate only if a new membership is discovered.
            ColumnSet addedColumns = null;
            ColumnSet removedColumns = null;
            for (Int2IntMap.Entry columnEntry : columnUpdates.int2IntEntrySet()) {
                int columnId = columnEntry.getIntKey();
                int delta = columnEntry.getIntValue();

                if (delta == 0)
                    continue;

                int previousCount = record.getOrDefault(columnId, 0);
                MembershipCountUpdate.Result countResult = MembershipCountUpdate.apply(valueId, columnId, previousCount,
                        delta);
                int updatedCount = countResult.updatedCount();

                switch (countResult.transition()) {
                    case ADDED:
                        record.put(columnId, updatedCount);
                        if (addedColumns == null)
                            addedColumns = columnSets.create();
                        addedColumns.add(columnId);
                        break;

                    case REMOVED:
                        record.remove(columnId);
                        if (removedColumns == null)
                            removedColumns = columnSets.create();
                        removedColumns.add(columnId);
                        break;

                    case NONE:
                        if (updatedCount == 0)
                            record.remove(columnId);
                        else
                            record.put(columnId, updatedCount);

                        break;
                }
            }
            if (addedColumns != null) {
                addedColumnsByValue.put(valueId, addedColumns);
                for (int columnId = addedColumns.nextSetBit(0); columnId >= 0; columnId = addedColumns
                        .nextSetBit(columnId + 1)) {
                    modeSpecificContext.membershipAdded(columnId, valueId);
                }
                modeSpecificContext.membershipChanged(record, addedColumns);
            }

            if (removedColumns != null)
                removedColumnsByValue.put(valueId, removedColumns);
        }
        phaseMetrics.record(Phase.MEMBERSHIP_UPDATE, System.nanoTime() - started);

        return new MembershipBatchResult(previousRecordsByValue, records, addedColumnsByValue, removedColumnsByValue);
    }
}
