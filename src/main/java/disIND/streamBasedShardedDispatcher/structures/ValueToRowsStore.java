package disIND.streamBasedShardedDispatcher.structures;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.roaringbitmap.RoaringBitmap;

public final class ValueToRowsStore {

    private final Int2ObjectOpenHashMap<RoaringBitmap> valueToRows = new Int2ObjectOpenHashMap<>();

    public void add(int valueId, long rowId) {
        valueToRows.computeIfAbsent(valueId, k -> new RoaringBitmap()).add((int) rowId);
    }

    public boolean containsValue(int valueId) {
        return valueToRows.containsKey(valueId);
    }

    public ValueToRowsStore deepCopy() {
        ValueToRowsStore copy = new ValueToRowsStore();
        valueToRows.forEach((valueId, rows) -> copy.valueToRows.put(valueId, rows.clone()));
        return copy;
    }
}