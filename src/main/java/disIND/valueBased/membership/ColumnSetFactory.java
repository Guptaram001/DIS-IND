package disIND.valueBased.membership;

import org.roaringbitmap.RoaringBitmap;

public final class ColumnSetFactory {
    private final int totalColumns;

    public ColumnSetFactory(int totalColumns) {
        this.totalColumns = totalColumns;
    }

    public ColumnSet create() {
        if (totalColumns <= Long.SIZE)
            return new LongColumnSet();
        return new RoaringColumnSet();
    }

    private static final class LongColumnSet implements ColumnSet {
        private long bits;

        private LongColumnSet() {
        }

        private LongColumnSet(long bits) {
            this.bits = bits;
        }

        @Override
        public void add(int column) {
            bits |= 1L << column;
        }

        @Override
        public boolean contains(int column) {
            return (bits & (1L << column)) != 0;
        }

        @Override
        public boolean isEmpty() {
            return bits == 0;
        }

        @Override
        public int cardinality() {
            return Long.bitCount(bits);
        }

        @Override
        public int nextSetBit(int fromColumn) {
            if (fromColumn < 0 || fromColumn >= Long.SIZE)
                return -1;
            long remaining = bits & (-1L << fromColumn);
            return remaining == 0 ? -1 : Long.numberOfTrailingZeros(remaining);
        }

        @Override
        public ColumnSet copy() {
            return new LongColumnSet(bits);
        }

        @Override
        public void andNot(ColumnSet other) {
            bits &= ~((LongColumnSet) other).bits;
        }

        @Override
        public void clear() {
            bits = 0L;
        }
    }

    private static final class RoaringColumnSet implements ColumnSet {
        private final RoaringBitmap bits;

        private RoaringColumnSet() {
            this.bits = new RoaringBitmap();
        }

        private RoaringColumnSet(RoaringBitmap bits) {
            this.bits = bits;
        }

        @Override
        public void add(int column) {
            bits.add(column);
        }

        @Override
        public boolean contains(int column) {
            return bits.contains(column);
        }

        @Override
        public boolean isEmpty() {
            return bits.isEmpty();
        }

        @Override
        public int cardinality() {
            return bits.getCardinality();
        }

        @Override
        public int nextSetBit(int fromColumn) {
            return (int) bits.nextValue(fromColumn);
        }

        @Override
        public ColumnSet copy() {
            return new RoaringColumnSet(bits.clone());
        }

        @Override
        public void andNot(ColumnSet other) {
            bits.andNot(((RoaringColumnSet) other).bits);
        }

        @Override
        public void clear() {
            bits.clear();
        }
    }
}
