package disIND.streamBasedShardedDispatcher.structures;

import disIND.streamBasedShardedDispatcher.model.SharedModel;
import org.roaringbitmap.RoaringBitmap;

public class AttributeDeltaBuilder {

    private final long epoch;
    private final RoaringBitmap distinctValues = new RoaringBitmap();

    public AttributeDeltaBuilder(long epoch) {
        this.epoch = epoch;
    }

    public void addInsert(int valueId, int row) {
        distinctValues.add(valueId);
    }

    public SharedModel.AttributeDelta build() {
        return new SharedModel.AttributeDelta(epoch, distinctValues);
    }
}
