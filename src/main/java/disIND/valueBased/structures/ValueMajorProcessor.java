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

    @Override
    public Map<Integer, Map<Integer, Long>> process(int bucketId, BatchBody body, WorkerValueIdStore valueIds) {
         if (!(body instanceof ValueMajorBatch batch))
            throw new IllegalArgumentException("ValueMajorProcessor expected ValueMajorBatch, received ");
        
        List<String> distinctValues =batch.values().stream()
                        .map(ValueData::value)
                        .distinct()
                        .toList();

        Map<String, Integer> idsByValue =valueIds.resolveBatch(bucketId,distinctValues);
        Map<Integer, Map<Integer, Long>> updatesByValue =new LinkedHashMap<>();
        for (ValueData valueData : batch.values()) {
            Integer valueId =idsByValue.get(valueData.value());

            if (valueId == null) 
                throw new IllegalStateException("No ID resolved for value: "+ valueData.value());
            
            Map<Integer, Long> columnUpdates =updatesByValue.computeIfAbsent(valueId,ignored -> new LinkedHashMap<>());

            for (ColumnRows column : valueData.columns()) {
                long count = column.rowIds().length;
                columnUpdates.merge(column.columnId(),count,Math::addExact);
            }
        }

        return updatesByValue;
    }
    
}
