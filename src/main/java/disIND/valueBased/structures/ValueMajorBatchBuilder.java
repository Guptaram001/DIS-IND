package disIND.valueBased.structures;

import java.util.ArrayList;

import disIND.valueBased.actors.ValueOwnerActor.BatchBody;
import disIND.valueBased.actors.ValueOwnerActor.ColumnRows;
import disIND.valueBased.actors.ValueOwnerActor.ValueData;
import disIND.valueBased.actors.ValueOwnerActor.ValueMajorBatch;
import disIND.valueBased.dataset.DataLoader.OwnerBatchBuilder;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ValueMajorBatchBuilder implements OwnerBatchBuilder{
    // value -> columnId -> row IDs

     private final Map<String, Map<Integer, LongArrayList>>
            rowsByValue = new LinkedHashMap<>();

    private boolean built;

    @Override
     public void add(int columnId, String value, long rowId) {
        if (built) 
            throw new IllegalStateException("Cannot add data after the batch was built");
        
        if (columnId < 0) 
            throw new IllegalArgumentException("columnId must not be negative");
        
        if (rowId < 0) 
            throw new IllegalArgumentException("rowId must not be negative");
        
        Objects.requireNonNull(value, "value");

        rowsByValue.computeIfAbsent(value,ignored -> new LinkedHashMap<>())
                .computeIfAbsent(columnId,ignored -> new LongArrayList())
                .add(rowId);
    }

    @Override
    public BatchBody build() {
        if (built) 
            throw new IllegalStateException("The batch was already built");
        
        built = true;
        List<ValueData> values =new ArrayList<>(rowsByValue.size());

        rowsByValue.forEach((value, rowsByColumn) -> {List<ColumnRows> columns =
                    new ArrayList<>(rowsByColumn.size());

            rowsByColumn.forEach((columnId, rowIds) ->
                    columns.add(new ColumnRows(columnId,rowIds.toLongArray())));

            values.add(new ValueData(value,columns));
        });

        return new ValueMajorBatch(values);
    }
    
}