package disIND.valueBased.monitor;

import java.util.concurrent.atomic.LongAdder;

public final class WorkerMembershipMetrics {

        private final LongAdder cacheHits = new LongAdder();
        private final LongAdder cacheMisses = new LongAdder();
        private final LongAdder cacheEvictions = new LongAdder();
        private final LongAdder rocksReadCalls = new LongAdder();
        private final LongAdder rocksReadKeys = new LongAdder();
        private final LongAdder rocksReadNanos = new LongAdder();
        private final LongAdder rocksWriteCalls = new LongAdder();
        private final LongAdder rocksWriteNanos = new LongAdder();
        private final LongAdder logicalBytesWritten = new LongAdder();
        private final LongAdder membershipRecordsWritten = new LongAdder();
        private final LongAdder candidateRecordsWritten = new LongAdder();
        private final LongAdder candidateRecordsDeleted = new LongAdder();

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

                requireNonNegative(keys, "keys");
                requireNonNegative(elapsedNanos, "elapsedNanos");
                rocksReadCalls.increment();
                rocksReadKeys.add(keys);
                rocksReadNanos.add(elapsedNanos);
        }

        public void rocksWrite(long elapsedNanos, long logicalBytes, long membershipRecords,
                        long candidateRecords, long candidateDeletes) {

                requireNonNegative(elapsedNanos, "elapsedNanos");
                requireNonNegative(logicalBytes, "logicalBytes");
                requireNonNegative(membershipRecords, "membershipRecords");
                requireNonNegative(candidateRecords, "candidateRecords");
                requireNonNegative(candidateDeletes, "candidateDeletes");

                rocksWriteCalls.increment();
                rocksWriteNanos.add(elapsedNanos);
                logicalBytesWritten.add(logicalBytes);
                membershipRecordsWritten.add(membershipRecords);
                candidateRecordsWritten.add(candidateRecords);
                candidateRecordsDeleted.add(candidateDeletes);
        }

        public Snapshot snapshot(long currentEntries, long currentEstimatedBytes,
                        long maximumEstimatedBytes, long activeBuckets) {

                return new Snapshot(cacheHits.sum(), cacheMisses.sum(), cacheEvictions.sum(),
                                currentEntries, maximumEstimatedBytes,
                                activeBuckets, rocksReadCalls.sum(), rocksReadKeys.sum(),
                                rocksReadNanos.sum(), rocksWriteCalls.sum(), rocksWriteNanos.sum(),
                                logicalBytesWritten.sum(), membershipRecordsWritten.sum(),
                                candidateRecordsWritten.sum(), candidateRecordsDeleted.sum());
        }

        private static void requireNonNegative(long value, String name) {
                if (value < 0L)
                        throw new IllegalArgumentException(name + " cannot be negative");

        }

        public record Snapshot(long cacheHits, long cacheMisses, long cacheEvictions, long currentEntries,
                        long maximumEstimatedBytes, long activeBuckets, long rocksReadCalls,
                        long rocksReadKeys, long rocksReadNanos, long rocksWriteCalls, long rocksWriteNanos,
                        long logicalBytesWritten, long membershipRecordsWritten, long candidateRecordsWritten,
                        long candidateRecordsDeleted) {

                public long cacheRequests() {
                        return cacheHits + cacheMisses;
                }

                public double cacheHitRate() {
                        long requests = cacheRequests();
                        return requests == 0L ? 0.0 : (double) cacheHits / requests;
                }

                public double rocksReadSecs() {
                        return rocksReadNanos / 1_000_000_000.0;
                }

                public double averageReadMicrosPerKey() {
                        return rocksReadKeys == 0L ? 0.0 : (rocksReadNanos / 1_000.0) / rocksReadKeys;
                }

                public double rocksWriteSecs() {
                        return rocksWriteNanos / 1_000_000_000.0;
                }

                public double averageWriteSecs() {
                        return rocksWriteCalls == 0L ? 0.0 : rocksWriteSecs() / rocksWriteCalls;
                }

                public double averageBytesPerWrite() {
                        return rocksWriteCalls == 0L ? 0.0 : (double) logicalBytesWritten / rocksWriteCalls;
                }
        }

}