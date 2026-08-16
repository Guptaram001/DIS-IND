package disIND.valueBased.structures;

import disIND.valueBased.actors.ValueOwnerActor.BatchBody;
import disIND.valueBased.actors.ValueOwnerActor.BatchProcessor;
import disIND.valueBased.actors.ValueOwnerActor.ColumnRows;
import disIND.valueBased.actors.ValueOwnerActor.ValueData;
import disIND.valueBased.actors.ValueOwnerActor.ValueMajorBatch;

import java.util.Map;
import java.util.List;
import java.util.LinkedHashMap;

public class ValueMajorProcessor implements BatchProcessor{
    /* 
    value → column → row IDs to
    valueId → columnId → occurrence count
    Ignores the rowIds here, might be used changed lateer to nary.
    */

    @Override
    public Map<Integer, Map<Integer, Integer>> process(int bucketId, BatchBody body, WorkerValueIdStore valueIds) {
         if (!(body instanceof ValueMajorBatch))
            throw new IllegalArgumentException("ValueMajorProcessor expected ValueMajorBatch, received ");

        ValueMajorBatch batch=(ValueMajorBatch) body;
        
        List<String> distinctValues =batch.values().stream()
                        .map(ValueData::value)
                        .distinct()
                        .toList();

        Map<String, Integer> idsByValue =valueIds.resolveBatch(bucketId,distinctValues);
        Map<Integer, Map<Integer, Integer>> updatesByValue =new LinkedHashMap<>();
        for (ValueData valueData : batch.values()) {
            Integer valueId =idsByValue.get(valueData.value());

            if (valueId == null) 
                throw new IllegalStateException("No ID resolved for value: "+ valueData.value());
            
            Map<Integer, Integer> columnUpdates =updatesByValue.computeIfAbsent(valueId,ignored -> new LinkedHashMap<>());

            for (ColumnRows column : valueData.columns()) {
                int count = column.rowIds().length;
                columnUpdates.merge(column.columnId(),count,Math::addExact);
            }
        }

        return updatesByValue;
    }
    
}
