package disIND.valueBased.monitor;

import java.util.concurrent.atomic.LongAdder;

public final class WorkerValueIdMetrics {

    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();
    private final LongAdder cacheEvictions = new LongAdder();
    private final LongAdder rocksReadBatches = new LongAdder();
    private final LongAdder rocksReadKeys = new LongAdder();
    private final LongAdder rocksReadNanos = new LongAdder();

    public void cacheHit() {
        cacheHits.increment();
    }

    public void cacheMiss() {
        cacheMisses.increment();
    }

    public void cacheEviction() {
        cacheEvictions.increment();
    }

    public void rocksRead(long keys, long elapsedNanos) {

        if (keys < 0L)
            throw new IllegalArgumentException("keys cannot be negative");

        if (elapsedNanos < 0L)
            throw new IllegalArgumentException("elapsedNanos cannot be negative");

        rocksReadBatches.increment();
        rocksReadKeys.add(keys);
        rocksReadNanos.add(elapsedNanos);
    }

    public Snapshot snapshot(long currentEntries, long maximumEntries) {

        return new Snapshot(cacheHits.sum(), cacheMisses.sum(), cacheEvictions.sum(), currentEntries, maximumEntries,
                rocksReadBatches.sum(), rocksReadKeys.sum(), rocksReadNanos.sum());
    }

    public record Snapshot(long hits, long misses, long evictions, long currentEntries, long maximumEntries,
            long rocksReadBatches, long rocksReadKeys, long rocksReadNanos) {

        public long requests() {
            return hits + misses;
        }

        public double hitRate() {
            long requests = requests();
            return requests == 0L ? 0.0 : (double) hits / requests;
        }

        public double rocksReadMillis() {
            return rocksReadNanos / 1_000_000.0;
        }

        public double averageRocksReadMicrosPerKey() {
            return rocksReadKeys == 0L ? 0.0 : (rocksReadNanos / 1_000.0) / rocksReadKeys;
        }
    }
}