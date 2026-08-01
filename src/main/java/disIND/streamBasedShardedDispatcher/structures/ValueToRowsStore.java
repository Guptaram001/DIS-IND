package disIND.streamBasedShardedDispatcher.structures;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/** In-memory value-to-rows changes accumulated since the latest checkpoint. */
public final class ValueToRowsStore {
    private final Int2ObjectOpenHashMap<IntArrayList> valueToRows =new Int2ObjectOpenHashMap<>();

    public void add(int valueId, int rowId) {
        valueToRows.computeIfAbsent(valueId, ignored -> new IntArrayList()).add(rowId);
    }

    public int[] rowsForValue(int valueId) {
        IntArrayList rows = valueToRows.get(valueId);
        return rows == null ? new int[0] : rows.toIntArray();
    }

    public void forEach(BiConsumer<Integer, IntArrayList> consumer) {
        for (Int2ObjectMap.Entry<IntArrayList> entry : valueToRows.int2ObjectEntrySet()) {
            consumer.accept(entry.getIntKey(), entry.getValue());
        }
    }

    public Map<Integer, int[]> snapshot() {
        Map<Integer, int[]> copy = new HashMap<>(valueToRows.size());
        forEach((valueId, rows) -> copy.put(valueId, rows.toIntArray()));
        return Collections.unmodifiableMap(copy);
    }

    public boolean isEmpty() {
        return valueToRows.isEmpty();
    }

    public void clear() {
        valueToRows.clear();
    }
}
