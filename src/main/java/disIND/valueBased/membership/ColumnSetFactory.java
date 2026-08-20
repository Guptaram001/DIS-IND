package disIND.valueBased.membership;

import org.roaringbitmap.RoaringBitmap;

import java.util.function.IntConsumer;

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
        public void forEach(IntConsumer action) {
            long remaining = bits;
            while (remaining != 0) {
                int column = Long.numberOfTrailingZeros(remaining);
                action.accept(column);
                remaining &= remaining - 1;
            }
        }

        @Override
        public ColumnSet copy() {
            return new LongColumnSet(bits);
        }

        @Override
        public void andNot(ColumnSet other) {
            bits &= ~((LongColumnSet) other).bits;
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
        public void forEach(IntConsumer action) {
            bits.forEach((int column) -> action.accept(column));
        }

        @Override
        public ColumnSet copy() {
            return new RoaringColumnSet(bits.clone());
        }

        @Override
        public void andNot(ColumnSet other) {
            bits.andNot(((RoaringColumnSet) other).bits);
        }
    }
}
