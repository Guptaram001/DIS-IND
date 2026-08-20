package disIND.valueBased.ingestion;

import java.util.ArrayList;

import disIND.valueBased.dataset.DataLoader.OrientetationBatchBuilder;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnRows;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueData;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueMajorBatch;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ValueMajorBatchBuilder implements OrientetationBatchBuilder {
    // value -> columnId -> row IDs modified to
    // value -> columnId -> count for unary only

    // private final Map<String, Int2ObjectMap<IntArrayList>> rowsByValue =new
    // HashMap<>();
    private final Map<String, Int2IntOpenHashMap> countByValue = new HashMap<>();

    private boolean built;

    @Override
    public void add(int columnId, String value, int rowId) {
        if (built)
            throw new IllegalStateException("Cannot add data after the batch was built");

        if (columnId < 0)
            throw new IllegalArgumentException("columnId must not be negative");

        if (rowId < 0)
            throw new IllegalArgumentException("rowId must not be negative");

        Objects.requireNonNull(value, "value");

        countByValue.computeIfAbsent(value, ignored -> new Int2IntOpenHashMap()).addTo(columnId, 1);
    }

    @Override
    public BatchBody build() {
        if (built)
            throw new IllegalStateException("The batch was already built");

        built = true;
        List<ValueData> values = new ArrayList<>(countByValue.size());

        countByValue.forEach((value, countByColumn) -> {
            List<ColumnRows> columns = new ArrayList<>(countByColumn.size());
            countByColumn.forEach((columnId, count) -> columns.add(new ColumnRows(columnId, count)));
            values.add(new ValueData(value, columns));
        });

        return new ValueMajorBatch(values);
    }

}
