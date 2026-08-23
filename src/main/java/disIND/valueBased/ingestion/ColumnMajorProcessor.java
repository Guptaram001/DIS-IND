package disIND.valueBased.ingestion;

import disIND.valueBased.model.SharedModel.MembershipUpdates;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnMajorBatch;
import disIND.valueBased.structures.WorkerValueIdStore;

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

        // ColumnMajorBatch batch = (ColumnMajorBatch) body;
        // Set<String> distinctValues = new LinkedHashSet<>();

        // for (ColumnValues column : batch.columns())
        // for (ValueRows valueRows : column.values())
        // distinctValues.add(valueRows.value());

        // Object2IntMap<String> idsByValue = valueIds.resolveBatch(bucketId,
        // distinctValues);
        // Int2IntMap updatesByValue = new Int2IntOpenHashMap();

        // for (ColumnValues column : batch.columns()) {
        // for (ValueRows valueRows : column.values()) {
        // Integer valueId = idsByValue.get(valueRows.value());
        // if (valueId == null)
        // throw new IllegalStateException("No ID resolved for value: ");

        // updatesByValue.computeIfAbsent(valueId, ignored -> new Int2IntOpenHashMap())
        // .merge(column.colId(), valueRows.rowIds().length, Math::addExact);
        // }
        // }

        return null;
    }
}
