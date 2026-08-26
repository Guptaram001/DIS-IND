package disIND.valueBased.monitor;

import disIND.valueBased.monitor.WorkerPhaseMetrics.Phase;
import disIND.valueBased.monitor.WorkerPhaseMetrics.PhaseSnapshot;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class WorkerMetricsWriter {

        private final Path workerDirectory;
        private final Logger log;

        public WorkerMetricsWriter(String nodeId, Logger log) {
                Objects.requireNonNull(nodeId, "nodeId");
                this.log = Objects.requireNonNull(log, "log");
                Path diagnosticsDirectory = Path
                                .of(System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR", "diagnostics"));
                this.workerDirectory = diagnosticsDirectory.resolve("workers").resolve(nodeId);
        }

        public void writeAll(WorkerValueIdMetrics.Snapshot valueId, WorkerMembershipMetrics.Snapshot membership,
                        WorkerPhaseMetrics phaseMetrics) {

                Objects.requireNonNull(valueId, "valueId");
                Objects.requireNonNull(membership, "membership");
                Objects.requireNonNull(phaseMetrics, "phaseMetrics");

                writeValueIdMetrics(valueId);
                writeMembershipMetrics(membership);
                writeBatchPhaseMetrics(phaseMetrics);
        }

        private void writeValueIdMetrics(WorkerValueIdMetrics.Snapshot metrics) {

                String contents = "metric\tvalue\tunit\n"
                                + "cache_hits\t"
                                + metrics.hits()
                                + "\trequests\n"

                                + "cache_misses\t"
                                + metrics.misses()
                                + "\trequests\n"

                                + "cache_requests\t"
                                + metrics.requests()
                                + "\trequests\n"

                                + "cache_hit_rate\t"
                                + metrics.hitRate()
                                + "\tratio\n"

                                + "cache_evictions\t"
                                + metrics.evictions()
                                + "\tentries\n"

                                + "cache_current_entries\t"
                                + metrics.currentEntries()
                                + "\tentries\n"

                                + "cache_maximum_entries\t"
                                + metrics.maximumEntries()
                                + "\tentries\n"

                                + "rocksdb_read_batches\t"
                                + metrics.rocksReadBatches()
                                + "\toperations\n"

                                + "rocksdb_read_keys\t"
                                + metrics.rocksReadKeys()
                                + "\tkeys\n"

                                + "rocksdb_read_time_ns\t"
                                + metrics.rocksReadNanos()
                                + "\tnanoseconds\n"

                                + "rocksdb_read_time_ms\t"
                                + metrics.rocksReadMillis()
                                + "\tmilliseconds\n";

                write("value-id-cache-metrics.tsv", contents, "VALUE-ID-CACHE-METRICS");
        }

        private void writeMembershipMetrics(WorkerMembershipMetrics.Snapshot metrics) {

                String contents = "metric\tvalue\tunit\n"

                                + "cache_hits\t"
                                + metrics.cacheHits()
                                + "\trequests\n"

                                + "cache_misses\t"
                                + metrics.cacheMisses()
                                + "\trequests\n"

                                + "cache_requests\t"
                                + metrics.cacheRequests()
                                + "\trequests\n"

                                + "cache_hit_rate\t"
                                + metrics.cacheHitRate()
                                + "\tratio\n"

                                + "cache_evictions\t"
                                + metrics.cacheEvictions()
                                + "\tentries\n"

                                + "cache_current_entries\t"
                                + metrics.currentEntries()
                                + "\tentries\n"

                                + "cache_current_estimated_bytes\t"
                                + metrics.currentEstimatedBytes()
                                + "\tbytes\n"

                                + "cache_maximum_estimated_bytes\t"
                                + metrics.maximumEstimatedBytes()
                                + "\tbytes\n"

                                + "cache_active_buckets\t"
                                + metrics.activeBuckets()
                                + "\tbuckets\n"

                                + "rocksdb_read_calls\t"
                                + metrics.rocksReadCalls()
                                + "\toperations\n"

                                + "rocksdb_read_keys\t"
                                + metrics.rocksReadKeys()
                                + "\tkeys\n"

                                + "rocksdb_read_time_ns\t"
                                + metrics.rocksReadNanos()
                                + "\tnanoseconds\n"

                                + "rocksdb_read_time_ms\t"
                                + metrics.rocksReadMillis()
                                + "\tmilliseconds\n"

                                + "rocksdb_average_read_us_per_key\t"
                                + metrics.averageReadMicrosPerKey()
                                + "\tmicroseconds_per_key\n"

                                + "rocksdb_write_calls\t"
                                + metrics.rocksWriteCalls()
                                + "\toperations\n"

                                + "rocksdb_write_time_ns\t"
                                + metrics.rocksWriteNanos()
                                + "\tnanoseconds\n"

                                + "rocksdb_write_time_ms\t"
                                + metrics.rocksWriteMillis()
                                + "\tmilliseconds\n"

                                + "rocksdb_average_write_time_ms\t"
                                + metrics.averageWriteMillis()
                                + "\tmilliseconds\n"

                                + "rocksdb_logical_bytes_written\t"
                                + metrics.logicalBytesWritten()
                                + "\tbytes\n"

                                + "rocksdb_average_bytes_per_write\t"
                                + metrics.averageBytesPerWrite()
                                + "\tbytes_per_operation\n"

                                + "rocksdb_membership_records_written\t"
                                + metrics.membershipRecordsWritten()
                                + "\trecords\n"

                                + "rocksdb_candidate_records_written\t"
                                + metrics.candidateRecordsWritten()
                                + "\trecords\n"

                                + "rocksdb_candidate_records_deleted\t"
                                + metrics.candidateRecordsDeleted()
                                + "\trecords\n";

                write("membership-cache-metrics.tsv", contents, "MEMBERSHIP-CACHE-METRICS");
        }

        private void writeBatchPhaseMetrics(WorkerPhaseMetrics metrics) {

                StringBuilder contents = new StringBuilder("phase\tcalls\ttotal_ms\t" + "average_ms\tmax_ms\n");
                for (Phase phase : Phase.values()) {
                        PhaseSnapshot snapshot = metrics.snapshot(phase);
                        contents.append(phase.name().toLowerCase())
                                        .append('\t')
                                        .append(snapshot.calls())
                                        .append('\t')
                                        .append(snapshot.totalMillis())
                                        .append('\t')
                                        .append(snapshot.averageMillis())
                                        .append('\t')
                                        .append(snapshot.maxMillis())
                                        .append('\n');
                }

                write("batch-phase-metrics.tsv", contents.toString(), "BATCH-PHASE-METRICS");
        }

        private void write(String filename, String contents, String metricType) {

                Path file = workerDirectory.resolve(filename);
                try {
                        Files.createDirectories(workerDirectory);
                        Files.writeString(file, contents, StandardCharsets.UTF_8);
                        log.info("[{}] writtenTo={}", metricType, file.toAbsolutePath());
                } catch (IOException exception) {
                        log.error("Unable to write {} to {}", metricType, file, exception);
                }
        }

}