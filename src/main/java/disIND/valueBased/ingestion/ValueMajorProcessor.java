package disIND.valueBased.ingestion;

import disIND.valueBased.model.SharedModel.MembershipUpdates;
import disIND.valueBased.model.SharedModel.ValueUpdates;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnRows;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueData;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueMajorBatch;
import disIND.valueBased.structures.WorkerValueIdStore;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Map;
import java.util.List;
import java.util.LinkedHashMap;

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

        List<String> distinctValues = batch.values().stream()
                .map(ValueData::value)
                .distinct()
                .toList();

        Map<String, Integer> idsByValue = valueIds.resolveBatch(bucketId, distinctValues);
        Map<Integer, Int2IntMap> updatesByValue = new LinkedHashMap<>();
        for (ValueData valueData : batch.values()) {
            Integer valueId = idsByValue.get(valueData.value());

            if (valueId == null)
                throw new IllegalStateException("No ID resolved for value: " + valueData.value());

            Map<Integer, Integer> columnUpdates = updatesByValue.computeIfAbsent(valueId,
                    ignored -> new Int2IntOpenHashMap());

            for (ColumnRows column : valueData.columns()) {
                int count = column.count();
                columnUpdates.merge(column.columnId(), count, Math::addExact);
            }
        }

        return new ValueUpdates(updatesByValue);
    }

}
