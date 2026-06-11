package disIND.streamBasedShardedDispatcher.structures;

import org.roaringbitmap.RoaringBitmap;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Per-AttributeActor COW bitmap snapshot store.
 *
 * Hard snapshot cap (MAX_SNAPSHOTS)
 * ──────────────────────────────────
 * The watermark-based eviction path was not working: the binary watermark
 * stays near 0 during early ingestion (CM pair resumeEpoch initialised to 0),
 * so evictBelow(0) evicts nothing.  For lineitem.partkey (200k distinct values,
 * new values in every batch), 1733 snapshots × ~25 KB each = 43 MB per column
 * accumulated with no release path.
 *
 * Fix: commitEpoch now enforces a hard cap of MAX_SNAPSHOTS=5.  After storing
 * the new snapshot it immediately drops any entries beyond the oldest 5,
 * regardless of any watermark.  This bounds the store to ~125 KB per column
 * even under worst-case (all new distinct values every batch).
 *
 * Why 5 is safe:
 *   - DeltaScan needs snapshotAt(sinceEpoch) and snapshotAt(untilEpoch).
 *   - GetBitmap needs snapshotAt(epoch).
 *   - All three are issued for the current or very-recently-committed epoch.
 *   - 5 snapshots gives ample headroom for any in-flight RA/Appraisal request.
 *
 * containsId / cardinality-skip optimisation (unchanged from previous version).
 * evictBelow is retained for watermark-driven cleanup when it does work.
 */
public final class BitmapSnapshotStore {

    /** Hard cap: keep only this many snapshots regardless of watermark. */
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

    /**
     * Commit head as snapshot for this epoch, then enforce the hard cap.
     * Skips cloning if cardinality is unchanged (no new distinct values).
     */
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

    /** Largest epoch ≤ requested. Returns null if none exists yet. */
    public RoaringBitmap snapshotAt(long epoch) {
        Map.Entry<Long, RoaringBitmap> e = snapshots.floorEntry(epoch);
        return e != null ? e.getValue() : null;
    }

    public RoaringBitmap current() {
        return head.clone();
    }

    /** Watermark-driven eviction (secondary path; cap above is primary). */
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