package disIND.valueBased.monitor;

import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.WorkerValueIdStore;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerMetricsFlusher {

    private final AtomicBoolean written = new AtomicBoolean();

    private final WorkerMetricsWriter writer;
    private final WorkerValueIdStore valueIdStore;
    private final ValueOwnerMembershipStore membershipStore;
    private final WorkerPhaseMetrics phaseMetrics;

    public WorkerMetricsFlusher(WorkerMetricsWriter writer, WorkerValueIdStore valueIdStore,
            ValueOwnerMembershipStore membershipStore, WorkerPhaseMetrics phaseMetrics) {

        this.writer = Objects.requireNonNull(writer);
        this.valueIdStore = Objects.requireNonNull(valueIdStore);
        this.membershipStore = Objects.requireNonNull(membershipStore);
        this.phaseMetrics = Objects.requireNonNull(phaseMetrics);
    }

    public boolean flushOnce() {
        if (!written.compareAndSet(false, true))
            return false;

        writer.writeAll(valueIdStore.metricsSnapshot(), membershipStore.metricsSnapshot(), phaseMetrics);
        return true;
    }

    public boolean isWritten() {
        return written.get();
    }
}