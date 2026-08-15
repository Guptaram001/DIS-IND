package disIND.valueBased.structures;

import disIND.valueBased.actors.ValueOwnerActor.BatchBody;
import disIND.valueBased.actors.ValueOwnerActor.BatchProcessor;
import disIND.valueBased.actors.ValueOwnerActor.ColumnMajorBatch;
import disIND.valueBased.actors.ValueOwnerActor.ColumnValues;
import disIND.valueBased.actors.ValueOwnerActor.ValueRows;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class ColumnMajorProcessor
        implements BatchProcessor {

    @Override
    public Map<Integer, Map<Integer, Long>> process(int bucketId,BatchBody body,WorkerValueIdStore valueIds) {
        if (!(body instanceof ColumnMajorBatch batch)) {
            throw new IllegalArgumentException("ColumnMajorProcessor expected ColumnMajorBatch, received ");
        }

        Set<String> distinctValues = new LinkedHashSet<>();

        for (ColumnValues column : batch.columns()) 
            for (ValueRows valueRows : column.values()) 
                distinctValues.add(valueRows.value());

        Map<String, Integer> idsByValue =valueIds.resolveBatch(bucketId,distinctValues);
        Map<Integer, Map<Integer, Long>> updatesByValue = new LinkedHashMap<>();

        for (ColumnValues column : batch.columns()) {
            for (ValueRows valueRows : column.values()) {
                Integer valueId =idsByValue.get(valueRows.value());
                if (valueId == null) 
                    throw new IllegalStateException("No ID resolved for value: ");
                

                updatesByValue.computeIfAbsent(valueId,ignored -> new LinkedHashMap<>())
                        .merge(column.colId(),(long) valueRows.rowIds().length,Math::addExact);
            }
        }

        return updatesByValue;
    }
}