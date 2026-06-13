package disIND.streamBasedShardedDispatcher.structures;

import org.roaringbitmap.RoaringBitmap;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class BitmapSnapshotStore {

    private static final int MAX_SNAPSHOTS = 5;

    private final TreeMap<Long, RoaringBitmap> snapshots = new TreeMap<>();
    private final RoaringBitmap head = new RoaringBitmap();

    private long lastSnapshotCardinality = -1L;

    public void insertIds(int[] ids) {
        for (int id : ids) head.add(id);
    }

    public boolean containsId(int id) {
        return head.contains(id);
    }

    public void retractId(int id) {
        head.remove(id);
        // Reset cardinality cache so the next commitEpoch() doesn't skip the clone.
        lastSnapshotCardinality = -1L;
    }

    public void commitEpoch(long epoch) {
        long card = head.getCardinality();
        if (card == lastSnapshotCardinality && !snapshots.isEmpty()) {
            return;   // no new distinct values — skip the clone
        }
        RoaringBitmap snap = head.clone();
        snap.runOptimize();
        snapshots.put(epoch, snap);
        lastSnapshotCardinality = card;

        // Hard cap: drop oldest entries beyond MAX_SNAPSHOTS.
        // Always keep the latest snapshot (lastKey) so GetSketch/GetBitmap
        // at any future epoch returns the most recent data.
        while (snapshots.size() > MAX_SNAPSHOTS) {
            snapshots.pollFirstEntry();   // removes lowest epoch
        }
    }

    public RoaringBitmap snapshotAt(long epoch) {
        Map.Entry<Long, RoaringBitmap> e = snapshots.floorEntry(epoch);
        return e != null ? e.getValue() : null;
    }

    public RoaringBitmap current() {
        return head.clone();
    }

    public int evictBelow(long watermark) {
        if (snapshots.size() <= 1) return 0;
        Long keepFrom = snapshots.lastKey();
        NavigableMap<Long, RoaringBitmap> stale = snapshots.headMap(
                Math.min(watermark, keepFrom), false);
        int count = stale.size();
        stale.clear();
        return count;
    }

    public int snapshotCount()  { return snapshots.size(); }
    public long cardinality()   { return head.getCardinality(); }
}