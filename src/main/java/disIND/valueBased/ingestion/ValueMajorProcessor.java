package disIND.valueBased.ingestion;

import disIND.valueBased.model.SharedModel.MembershipUpdates;
import disIND.valueBased.model.SharedModel.ValueUpdates;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnRows;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueData;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueMajorBatch;
import disIND.valueBased.structures.WorkerValueIdStore;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import disIND.valueBased.membership.AdaptiveColumnCounts;

import java.util.List;

public class ValueMajorProcessor implements BatchProcessor {
    /*
     * value → column → row IDs to
     * valueId → columnId → occurrence count
     * Ignores the rowIds here, might be used changed lateer to nary.
     */

    @Override
    public MembershipUpdates process(int bucketId, BatchBody body, WorkerValueIdStore valueIds) {
        if (!(body instanceof ValueMajorBatch))
            throw new IllegalArgumentException("ValueMajorProcessor expected ValueMajorBatch, received ");

        ValueMajorBatch batch = (ValueMajorBatch) body;
        List<ValueData> batchValues = batch.values();

        Object2IntMap<String> idsByValue = valueIds.resolveBatch(bucketId, batchValues);
        Int2ObjectMap<Int2IntMap> updatesByValue = new Int2ObjectOpenHashMap<>();
        for (ValueData valueData : batch.values()) {
            int valueId = idsByValue.getInt(valueData.value());

            if (valueId == WorkerValueIdStore.UNRESOLVED)
                throw new IllegalStateException("No ID resolved for value: " + valueData.value());
            Int2IntMap columnUpdates = updatesByValue.get(valueId);

            if (columnUpdates == null) {
                columnUpdates = new AdaptiveColumnCounts(valueData.columns().size());
                updatesByValue.put(valueId, columnUpdates);
            }

            for (ColumnRows column : valueData.columns()) {
                int count = column.count();
                int columnId = column.columnId();
                int previous = columnUpdates.get(columnId);
                int updated = Math.addExact(previous, count);
                columnUpdates.put(columnId, updated);
            }
        }

        return new ValueUpdates(updatesByValue);
    }

}
