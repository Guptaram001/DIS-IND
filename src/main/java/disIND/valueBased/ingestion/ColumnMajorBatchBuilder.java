package disIND.valueBased.ingestion;

import java.util.ArrayList;

import disIND.valueBased.dataset.DataLoader.OrientetationBatchBuilder;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnMajorBatch;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnValues;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueRows;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.Objects;

public final class ColumnMajorBatchBuilder implements OrientetationBatchBuilder {
    // column -> value -> row IDs
    private final Map<Integer, Map<String, LongArrayList>> rowsByColumn = new LinkedHashMap<>();
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

        rowsByColumn.computeIfAbsent(columnId, ignored -> new LinkedHashMap<>())
                .computeIfAbsent(value, ignored -> new LongArrayList())
                .add(rowId);
    }

    @Override
    public BatchBody build() {
        if (built)
            throw new IllegalStateException("The batch was already built");

        built = true;
        List<ColumnValues> columns = new ArrayList<>(rowsByColumn.size());

        rowsByColumn.forEach((columnId, rowsByValue) -> {
            List<ValueRows> values = new ArrayList<>(rowsByValue.size());

            rowsByValue.forEach((value, rowIds) -> values.add(new ValueRows(value, rowIds.toLongArray())));

            columns.add(new ColumnValues(columnId, values));
        });

        return new ColumnMajorBatch(columns);
    }
}
