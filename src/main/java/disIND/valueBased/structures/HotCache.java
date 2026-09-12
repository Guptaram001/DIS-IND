package disIND.valueBased.structures;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.LinkedHashMap;
import java.util.function.ToIntFunction;

final class HotCache<K, V> {
    private final Cache<K, V> caffeine;
    private final LinkedHashMap<K, V> lru;
    private final ToIntFunction<V> weigh;
    private final Runnable eviction;
    private long maximum, weight;

    HotCache(CacheMode policy, long maximum, ToIntFunction<V> weigh, Runnable eviction) {
        if (maximum < 0)
            throw new IllegalArgumentException("Negative cache budget");
        this.maximum = maximum;
        this.weigh = weigh;
        this.eviction = eviction;
        lru = policy == CacheMode.LRU ? new LinkedHashMap<>(16, 0.75f, true) : null;
        caffeine = policy == CacheMode.CAFFEINE ? Caffeine.newBuilder()
                .maximumWeight(maximum)
                .weigher((K key, V value) -> weigh.applyAsInt(value))
                .evictionListener(
                        (K key, V value, com.github.benmanes.caffeine.cache.RemovalCause cause) -> eviction.run())
                .build() : null;
    }

    V get(K key) {
        return caffeine != null ? caffeine.getIfPresent(key) : lru.get(key);
    }

    V remove(K key) {
        if (caffeine != null)
            return caffeine.asMap().remove(key);
        V previous = lru.remove(key);
        if (previous != null)
            weight -= weigh.applyAsInt(previous);
        return previous;
    }

    void put(K key, V value) {
        if (caffeine != null) {
            caffeine.put(key, value);
            return;
        }
        V previous = lru.put(key, value);
        if (previous != null)
            weight -= weigh.applyAsInt(previous);
        weight += weigh.applyAsInt(value);
        trim();
    }

    void maximum(long value) {
        if (value == maximum)
            return;
        maximum = value;
        if (caffeine != null)
            caffeine.policy().eviction().orElseThrow().setMaximum(value);
        else
            trim();
    }

    private void trim() {
        var iterator = lru.entrySet().iterator();
        while (weight > maximum && iterator.hasNext()) {
            weight -= weigh.applyAsInt(iterator.next().getValue());
            iterator.remove();
            eviction.run();
        }
    }

    long size() {
        return caffeine != null ? caffeine.estimatedSize() : lru.size();
    }

    long weight() {
        return caffeine != null ? caffeine.policy().eviction().orElseThrow().weightedSize().orElse(0) : weight;
    }

    void clear() {
        if (caffeine != null) {
            caffeine.invalidateAll();
            caffeine.cleanUp();
        } else {
            lru.clear();
            weight = 0;
        }
    }
}
