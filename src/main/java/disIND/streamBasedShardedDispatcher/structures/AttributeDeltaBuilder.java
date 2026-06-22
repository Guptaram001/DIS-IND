package disIND.streamBasedShardedDispatcher.structures;

import disIND.streamBasedShardedDispatcher.model.SharedModel;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class AttributeDeltaBuilder {

    private final long epoch;
    private final Int2ObjectMap<IntArrayList> insertedRowsByValue = new Int2ObjectOpenHashMap<>();

    public AttributeDeltaBuilder(long epoch) {
        this.epoch = epoch;
    }

    public void addInsert(int valueId, int row) {
        insertedRowsByValue.computeIfAbsent(valueId, k -> new IntArrayList()).add(row);
    }

    public SharedModel.AttributeDelta build() {
        return new SharedModel.AttributeDelta(epoch, insertedRowsByValue);
    }
}