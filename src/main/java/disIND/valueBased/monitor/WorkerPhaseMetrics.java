package disIND.valueBased.monitor;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public final class WorkerPhaseMetrics {

    public enum Phase {
        BATCH_PREPARATION, MEMBERSHIP_LOAD, MEMBERSHIP_UPDATE, CANDIDATE_EVALUATION,
        TRACKER_RESOLUTION, ROCKSDB_WRITE
    }

    public record PhaseSnapshot(long calls, long totalNanos, long maxNanos) {

        public double totalMillis() {
            return totalNanos / 1_000_000.0;
        }

        public double averageMillis() {
            return calls == 0L ? 0.0 : totalMillis() / calls;
        }

        public double maxMillis() {
            return maxNanos / 1_000_000.0;
        }
    }

    private static final class PhaseCounter {
        private final LongAdder calls = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final LongAccumulator maxNanos = new LongAccumulator(Long::max, 0L);

        void record(long elapsedNanos) {
            if (elapsedNanos < 0L)
                throw new IllegalArgumentException("elapsedNanos cannot be negative");
            calls.increment();
            totalNanos.add(elapsedNanos);
            maxNanos.accumulate(elapsedNanos);
        }

        PhaseSnapshot snapshot() {
            return new PhaseSnapshot(calls.sum(), totalNanos.sum(), maxNanos.get());
        }
    }

    private final PhaseCounter[] counters;

    public WorkerPhaseMetrics() {
        Phase[] phases = Phase.values();
        counters = new PhaseCounter[phases.length];
        for (int index = 0; index < counters.length; index++)
            counters[index] = new PhaseCounter();

    }

    public void record(Phase phase, long elapsedNanos) {
        counters[phase.ordinal()].record(elapsedNanos);
    }

    public PhaseSnapshot snapshot(Phase phase) {
        return counters[phase.ordinal()].snapshot();
    }
}