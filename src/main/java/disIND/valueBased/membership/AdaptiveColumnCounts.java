package disIND.valueBased.membership;

import it.unimi.dsi.fastutil.ints.AbstractInt2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

public final class AdaptiveColumnCounts extends AbstractInt2IntMap {
    private static final int ARRAY_MAP_MAX_SIZE = 4;
    private Int2IntMap delegate;

    public AdaptiveColumnCounts() {
        this(0);
    }

    public AdaptiveColumnCounts(int expectedSize) {
        if (expectedSize < 0)
            throw new IllegalArgumentException("expectedSize must not be negative");

        if (expectedSize <= ARRAY_MAP_MAX_SIZE)
            delegate = new Int2IntArrayMap(expectedSize);
        else
            delegate = new Int2IntOpenHashMap(expectedSize);
    }

    public AdaptiveColumnCounts(Int2IntMap source) {
        this(source.size());
        defaultReturnValue(source.defaultReturnValue());
        putAll(source);
    }

    @Override
    public int put(int column, int count) {
        promoteIfNecessary(column);
        return delegate.put(column, count);
    }

    @Override
    public int get(int column) {
        return delegate.get(column);
    }

    @Override
    public boolean containsKey(int column) {
        return delegate.containsKey(column);
    }

    @Override
    public int remove(int column) {
        return delegate.remove(column);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public void defaultReturnValue(int value) {
        delegate.defaultReturnValue(value);
    }

    @Override
    public int defaultReturnValue() {
        return delegate.defaultReturnValue();
    }

    @Override
    public ObjectSet<Int2IntMap.Entry> int2IntEntrySet() {
        return delegate.int2IntEntrySet();
    }

    private void promoteIfNecessary(int column) {
        if (delegate instanceof Int2IntOpenHashMap)
            return;

        if (delegate.containsKey(column))
            return;

        if (delegate.size() < ARRAY_MAP_MAX_SIZE)
            return;

        int missingValue = delegate.defaultReturnValue();
        Int2IntOpenHashMap promoted = new Int2IntOpenHashMap(ARRAY_MAP_MAX_SIZE + 1);
        promoted.defaultReturnValue(missingValue);
        promoted.putAll(delegate);
        delegate = promoted;
    }
}