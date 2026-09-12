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
        long started = System.nanoTime();
        // Loads the membership from the RocksDB store or caches if any
        Int2ObjectMap<Int2IntMap> records = membershipStore.loadBatch(bucketId, updatesByValue.keySet());
        phaseMetrics.record(Phase.MEMBERSHIP_LOAD, System.nanoTime() - started);
        started = System.nanoTime();
        Int2ObjectMap<ColumnSet> addedColumnsByValue = new Int2ObjectOpenHashMap<>(); // ValueId to columns added.
        Int2ObjectMap<ColumnSet> removedColumnsByValue = new Int2ObjectOpenHashMap<>(); // ValueId to columns removed.
        long filterUpdateNanos = 0L;
        long clusterUpdateNanos = 0L;
        boolean useFilterUpdates = modeSpecificContext.usesAuxiliaryFilters();

        for (Int2ObjectMap.Entry<Int2IntMap> valueEntry : updatesByValue.int2ObjectEntrySet()) {

            int valueId = valueEntry.getIntKey();
            Int2IntMap columnUpdates = valueEntry.getValue();
            Int2IntMap record = records.get(valueId);
            if (record == null)
                throw new IllegalStateException("No membership record loaded for value ");

            // Allocate only if a new membership is discovered.
            ColumnSet addedColumns = null;
            ColumnSet removedColumns = null;
            for (Int2IntMap.Entry columnEntry : columnUpdates.int2IntEntrySet()) {
                // Update the membership with new updates.
                int columnId = columnEntry.getIntKey();
                int newCount = columnEntry.getIntValue();

                if (newCount == 0)
                    continue;

                int previousCount = record.getOrDefault(columnId, 0);
                int updatedCount = Math.addExact(previousCount, newCount);
                if (updatedCount < 0)
                    throw new IllegalStateException("Membership count became negative: bucketId=" + bucketId);

                // Count transition:
                // 0 -> >0 : membership added
                // >0 -> 0 : membership removed
                // >0 -> >0 : count changed, membership unchanged
                if (previousCount == 0 && updatedCount > 0) {
                    record.put(columnId, updatedCount);
                    if (addedColumns == null)
                        addedColumns = columnSets.create();
                    addedColumns.add(columnId);
                } else if (previousCount > 0 && updatedCount == 0) {
                    record.remove(columnId);
                    if (removedColumns == null)
                        removedColumns = columnSets.create();
                    removedColumns.add(columnId);
                } else {
                    if (updatedCount == 0)
                        record.remove(columnId);
                    else
                        record.put(columnId, updatedCount);
                }
            }
            if (addedColumns != null) addedColumnsByValue.put(valueId, addedColumns);
            if (removedColumns != null) removedColumnsByValue.put(valueId, removedColumns);
            if (addedColumns != null || removedColumns != null) {
                if (modeSpecificContext.clusterBased()) {
                    long clusterStarted = System.nanoTime();
                    if (addedColumns != null)
                        for (int col = addedColumns.nextSetBit(0); col >= 0; col = addedColumns.nextSetBit(col + 1))
                            modeSpecificContext.membershipAdded(col, valueId);
                    if (removedColumns != null)
                        for (int col = removedColumns.nextSetBit(0); col >= 0; col = removedColumns.nextSetBit(col + 1))
                            modeSpecificContext.membershipRemoved(col, valueId);
                    modeSpecificContext.membershipChanged(record, addedColumns, removedColumns);
                    clusterUpdateNanos += System.nanoTime() - clusterStarted;
                }
                if (useFilterUpdates) {
                    long filterStarted = System.nanoTime();
                    if (addedColumns != null)
                        for (int col = addedColumns.nextSetBit(0); col >= 0; col = addedColumns.nextSetBit(col + 1))
                            modeSpecificContext.auxiliaryMembershipAdded(col, valueId);
                    if (removedColumns != null)
                        for (int col = removedColumns.nextSetBit(0); col >= 0; col = removedColumns.nextSetBit(col + 1))
                            modeSpecificContext.auxiliaryMembershipRemoved(col, valueId);
                    filterUpdateNanos += System.nanoTime() - filterStarted;
                }
            }
        }

        long updateNanos = System.nanoTime() - started;
        phaseMetrics.record(Phase.MEMBERSHIP_UPDATE, Math.max(0L, updateNanos - filterUpdateNanos - clusterUpdateNanos));
        if (modeSpecificContext.clusterBased())
            phaseMetrics.record(Phase.CLUSTER_MAINTENANCE, clusterUpdateNanos);
        if (useFilterUpdates)
            phaseMetrics.record(Phase.FILTER_UPDATE, filterUpdateNanos);

        return new MembershipBatchResult(records, addedColumnsByValue, removedColumnsByValue);
    }
}
