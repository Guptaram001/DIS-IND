package disIND.streamBasedShardedDispatcher.structures;

import disIND.streamBasedShardedDispatcher.model.SharedModel;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import java.util.ArrayList;
import java.util.List;

public final class BitmapStore {

    private final RoaringBitmap bitmapStore = new RoaringBitmap();
    public void insertIds(int[] ids) {
        for (int id : ids)
            bitmapStore.add(id);
    }

    public RoaringBitmap getBitmap() {
        return bitmapStore.clone();
    }

    public boolean contains(int id) {
        return bitmapStore.contains(id);
    }

    public BitmapStore deepCopy() {
        BitmapStore copy = new BitmapStore();
        copy.bitmapStore.or(this.bitmapStore);
        return copy;
    }

    public SharedModel.ScanResult compareAgainst(RoaringBitmap lhs, SharedModel.UnaryPair pair, int round) {
        RoaringBitmap violating = lhs;
        violating.andNot(bitmapStore);
        int count = violating.getCardinality();
        IntIterator it = violating.getIntIterator();
        List<Integer> witnesses = new ArrayList<>(100);
        while (it.hasNext() && witnesses.size() < 2) {
            witnesses.add(it.next());
        }
        return new SharedModel.ScanResult(pair,count,witnesses,violating,round);
    }
}
