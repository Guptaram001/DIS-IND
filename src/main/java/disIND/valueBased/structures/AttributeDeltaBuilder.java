package disIND.valueBased.structures;

import disIND.valueBased.model.SharedModel;
import org.roaringbitmap.RoaringBitmap;

public class AttributeDeltaBuilder {

    private final int round;
    private final RoaringBitmap distinctValues = new RoaringBitmap();

    public AttributeDeltaBuilder(int round) {
        this.round = round;
    }

    public void addInsert(int valueId, int row) {
        distinctValues.add(valueId);
    }

    public SharedModel.AttributeDelta build() {
        return new SharedModel.AttributeDelta(round, distinctValues);
    }
}
