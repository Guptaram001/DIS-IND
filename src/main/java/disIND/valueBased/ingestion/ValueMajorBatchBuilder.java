package disIND.valueBased.ingestion;

import java.util.ArrayList;

import disIND.valueBased.dataset.DataLoader.OrientetationBatchBuilder;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnRows;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueData;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueMajorBatch;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ValueMajorBatchBuilder implements OrientetationBatchBuilder {
    // value -> columnId -> row IDs modified to
    // value -> columnId -> count for unary only

    // private final Map<String, Int2ObjectMap<IntArrayList>> rowsByValue =new
    // HashMap<>();
    private final Map<String, SmallColumnCounts> countByValue = new HashMap<>();

    private boolean built;

    private static final class SmallColumnCounts {

        private static final int INLINE_CAPACITY = 4;
        private int size;
        private int column0, column1, column2, column3;
        private int count0, count1, count2, count3;
        private Int2IntOpenHashMap more;

        void addTo(int column, int delta) {
            if (more != null) {
                int previous = more.get(column);
                int updated = Math.addExact(previous, delta);
                more.put(column, updated);
                return;
            }

            for (int i = 0; i < size; i++) {
                if (columnAt(i) == column) {
                    setCount(i, Math.addExact(countAt(i), delta));
                    return;
                }
            }

            if (size < INLINE_CAPACITY) {
                setPair(size, column, delta);
                size++;
                return;
            }

            promoteToHashMap();
            more.addTo(column, delta);
        }

        int size() {
            return more == null ? size : more.size();
        }

        private void promoteToHashMap() {
            more = new Int2IntOpenHashMap(INLINE_CAPACITY + 1);
            for (int i = 0; i < size; i++)
                more.put(columnAt(i), countAt(i));

        }

        private int columnAt(int index) {
            return switch (index) {
                case 0 -> column0;
                case 1 -> column1;
                case 2 -> column2;
                case 3 -> column3;
                default -> throw new IndexOutOfBoundsException(index);
            };
        }

        private int countAt(int index) {
            return switch (index) {
                case 0 -> count0;
                case 1 -> count1;
                case 2 -> count2;
                case 3 -> count3;
                default -> throw new IndexOutOfBoundsException(index);
            };
        }

        private void setPair(int index, int column, int count) {
            switch (index) {
                case 0 -> {
                    column0 = column;
                    count0 = count;
                }
                case 1 -> {
                    column1 = column;
                    count1 = count;
                }
                case 2 -> {
                    column2 = column;
                    count2 = count;
                }
                case 3 -> {
                    column3 = column;
                    count3 = count;
                }
                default -> throw new IndexOutOfBoundsException(index);
            }
        }

        private void appendTo(List<ColumnRows> destination) {
            if (more != null) {

                ObjectIterator<Int2IntMap.Entry> iterator = Int2IntMaps.fastIterator(more);
                while (iterator.hasNext()) {
                    Int2IntMap.Entry entry = iterator.next();
                    int count = entry.getIntValue();
                    if (count != 0)
                        destination.add(new ColumnRows(entry.getIntKey(), count));
                }
                return;
            }

            for (int i = 0; i < size; i++) {
                int count = countAt(i);
                if (count != 0)
                    destination.add(new ColumnRows(columnAt(i), count));

            }
        }

        private void setCount(int index, int count) {
            switch (index) {
                case 0 -> count0 = count;
                case 1 -> count1 = count;
                case 2 -> count2 = count;
                case 3 -> count3 = count;
                default -> throw new IndexOutOfBoundsException(index);
            }
        }
    }

    @Override
    public void add(int columnId, String value, int rowId, int delta) {
        if (built)
            throw new IllegalStateException("Cannot add data after the batch was built");

        if (columnId < 0)
            throw new IllegalArgumentException("columnId must not be negative");

        if (rowId < 0)
            throw new IllegalArgumentException("rowId must not be negative");

        if (delta == 0)
            throw new IllegalArgumentException("delta must not be zero");

        Objects.requireNonNull(value, "value");

        SmallColumnCounts counts = countByValue.get(value);
        if (counts == null) {
            counts = new SmallColumnCounts();
            countByValue.put(value, counts);
        }
        counts.addTo(columnId, delta);
    }

    @Override
    public BatchBody build() {
        if (built)
            throw new IllegalStateException("The batch was already built");

        built = true;
        List<ValueData> values = new ArrayList<>(countByValue.size());

        for (Map.Entry<String, SmallColumnCounts> entry : countByValue.entrySet()) {
            String value = entry.getKey();
            SmallColumnCounts counts = entry.getValue();
            List<ColumnRows> columns = new ArrayList<>(counts.size());
            counts.appendTo(columns);
            if (!columns.isEmpty())
                values.add(new ValueData(value, columns));
        }

        return new ValueMajorBatch(values);
    }

}
