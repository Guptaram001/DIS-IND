package disIND.valueBased.membership;

import org.roaringbitmap.RoaringBitmap;

public class RoaringCandidateSet implements CandidateSet {

    private final RoaringBitmap bits = new RoaringBitmap();

    @Override
    public boolean add(int index) {
        return bits.checkedAdd(index);
    }

    @Override
    public boolean contains(int index) {
        return bits.contains(index);
    }

    @Override
    public boolean remove(int index) {
        return bits.checkedRemove(index);
    }

    @Override
    public void clear() {
        bits.clear();
    }

    @Override
    public int size() {
        return bits.getCardinality();
    }

}
