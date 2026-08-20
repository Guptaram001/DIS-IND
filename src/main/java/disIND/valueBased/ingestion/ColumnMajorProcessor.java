package disIND.valueBased.ingestion;

import disIND.valueBased.model.SharedModel.MembershipUpdates;
import disIND.valueBased.model.SharedModel.ValueUpdates;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnMajorBatch;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnValues;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueRows;
import disIND.valueBased.structures.WorkerValueIdStore;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class ColumnMajorProcessor implements BatchProcessor {
    /*
     * column → value → row IDs to
     * valueId → columnId → occurrence count
     * Ignores the rowIds here, might be used changed lateer to nary.
     * Does not show significant improvement when used column based storage.
     */

    @Override
    public MembershipUpdates process(int bucketId, BatchBody body, WorkerValueIdStore valueIds) {
        if (!(body instanceof ColumnMajorBatch)) {
            throw new IllegalArgumentException("ColumnMajorProcessor expected ColumnMajorBatch ");
        }

        ColumnMajorBatch batch = (ColumnMajorBatch) body;
        Set<String> distinctValues = new LinkedHashSet<>();

        for (ColumnValues column : batch.columns())
            for (ValueRows valueRows : column.values())
                distinctValues.add(valueRows.value());

        Map<String, Integer> idsByValue = valueIds.resolveBatch(bucketId, distinctValues);
        Map<Integer, Int2IntMap> updatesByValue = new LinkedHashMap<>();

        for (ColumnValues column : batch.columns()) {
            for (ValueRows valueRows : column.values()) {
                Integer valueId = idsByValue.get(valueRows.value());
                if (valueId == null)
                    throw new IllegalStateException("No ID resolved for value: ");

                updatesByValue.computeIfAbsent(valueId, ignored -> new Int2IntOpenHashMap())
                        .merge(column.colId(), valueRows.rowIds().length, Math::addExact);
            }
        }

        return new ValueUpdates(updatesByValue);
    }
}
