package disIND.streamBasedShardedDispatcher.structures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Per-AttributeActor segmented row index.
 *
 * Changes vs original
 * ───────────────────
 * capSegments(int max) — evict the oldest sealed segments so at most `max`
 * segments remain live (sealed + current combined).  Called by AttributeActor
 * on every batch regardless of the n-ary watermark.  This caps row-index memory
 * at max × 256 KB per column even when no n-ary work has started and the
 * watermark is still 0.  At MAX_ROW_SEGMENTS=4 that is 1 MB per column,
 * 61 MB total — safe on any normal heap.
 *
 * NRA uses valueToRows (the inverted vid→rowBitmap index), not the row index,
 * so dropping old segments does not affect n-ary correctness.  The row index is
 * only used by sliceToArray, which is not called in the current pipeline.
 */
public final class SegmentedRowIndex {

    public static final int SEGMENT_SIZE = 65536;

    public static final class Segment {
        public final long startRow;
        public final long epochStart;
        public long epochEnd;
        public final int[] data;

        Segment(long startRow, long epochStart) {
            this.startRow   = startRow;
            this.epochStart = epochStart;
            this.epochEnd   = epochStart;
            this.data       = new int[SEGMENT_SIZE];
        }
    }

    private final List<Segment> sealed = new ArrayList<>();
    private Segment current = new Segment(0L, 0L);
    private long totalRows = 0L;

    public void appendBatch(long[] rows, int[] valueIds, long epoch) {
        for (int i = 0; i < rows.length; i++) {
            long row  = rows[i];
            int  vid  = valueIds[i];
            int  slot = (int)(row - current.startRow);
            if (slot >= SEGMENT_SIZE) {
                current.epochEnd = epoch - 1;
                sealed.add(current);
                current = new Segment(row, epoch);
                slot = 0;
            }
            current.data[slot] = vid;
            current.epochEnd   = epoch;
            if (row + 1 > totalRows) totalRows = row + 1;
        }
    }

    public int valueAt(long row) {
        if (row >= current.startRow) {
            return current.data[(int)(row - current.startRow)];
        }
        for (int i = sealed.size() - 1; i >= 0; i--) {
            Segment s = sealed.get(i);
            if (s.startRow <= row) {
                return s.data[(int)(row - s.startRow)];
            }
        }
        throw new IllegalArgumentException("Row " + row + " not found");
    }

    public int[] sliceToArray(long fromRow, long toRow) {
        int size = (int)(toRow - fromRow);
        int[] result = new int[size];
        for (int i = 0; i < size; i++) result[i] = valueAt(fromRow + i);
        return result;
    }

    /** Evict sealed segments whose epochEnd is below the watermark. */
    public int evictBelow(long naryWm) {
        int evicted = 0;
        Iterator<Segment> it = sealed.iterator();
        while (it.hasNext()) {
            if (it.next().epochEnd < naryWm) { it.remove(); evicted++; }
            else break;
        }
        return evicted;
    }

    /**
     * Evict oldest sealed segments so that (sealed.size() + 1) ≤ max.
     * The +1 accounts for the always-live current segment.
     * Returns the number of segments evicted.
     *
     * Safe to call regardless of the n-ary watermark: the row index is not
     * used by NRA (NRA uses the valueToRows inverted index).  Old segments
     * that have been fully filled and sealed will never be needed again by
     * any current code path.
     */
    public int capSegments(int max) {
        int target = Math.max(0, max - 1); // how many sealed segments to keep
        if (sealed.size() <= target) return 0;
        int toRemove = sealed.size() - target;
        sealed.subList(0, toRemove).clear();
        return toRemove;
    }

    public long totalRowCount() { return totalRows; }
}