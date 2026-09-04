package disIND.valueBased.monitor;

import disIND.valueBased.monitor.WorkerPhaseMetrics.Phase;
import disIND.valueBased.monitor.WorkerPhaseMetrics.PhaseSnapshot;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.structures.WorkerValueIdStore;

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

        public void writeAuxiliaryStorage(WorkerValueIdStore.DBSnapshot valueIds,
                        ValueOwnerMembershipStore.DBSnapshot membership) {
                long logicalTotal = Math.addExact(valueIds.logicalBytes(), membership.logicalBytes());
                long physicalTotal = Math.addExact(valueIds.physicalDiskBytes(), membership.physicalDiskBytes());
                String contents = "metric\tvalue\tunit\n"
                                + "value_id_records\t" + valueIds.valueRecords() + "\trecords\n"
                                + "value_id_key_bytes\t" + valueIds.valueKeyBytes() + "\tbytes\n"
                                + "value_id_value_bytes\t" + valueIds.valueBytes() + "\tbytes\n"
                                + "value_id_metadata_records\t" + valueIds.metadataRecords() + "\trecords\n"
                                + "value_id_metadata_bytes\t"
                                + (valueIds.metadataKeyBytes() + valueIds.metadataValueBytes()) + "\tbytes\n"
                                + "membership_records\t" + membership.membershipRecords() + "\trecords\n"
                                + "membership_key_bytes\t" + membership.membershipKeyBytes() + "\tbytes\n"
                                + "membership_value_bytes\t" + membership.membershipValueBytes() + "\tbytes\n"
                                + "candidate_records\t" + membership.candidateRecords() + "\trecords\n"
                                + "candidate_key_bytes\t" + membership.candidateKeyBytes() + "\tbytes\n"
                                + "candidate_value_bytes\t" + membership.candidateValueBytes() + "\tbytes\n"
                                + "logical_auxiliary_bytes\t" + logicalTotal + "\tbytes\n"
                                + "value_id_physical_disk_bytes\t" + valueIds.physicalDiskBytes() + "\tbytes\n"
                                + "membership_physical_disk_bytes\t" + membership.physicalDiskBytes() + "\tbytes\n"
                                + "physical_disk_bytes\t" + physicalTotal + "\tbytes\n";
                write("auxiliary-storage.tsv", contents, "AUXILIARY-STORAGE");
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

                                + "cache_maximum_entries_per_owner\t"
                                + metrics.maximumEntries()
                                + "\tentries\n"

                                // In multiget a batch may have multi keys. So count of batches. 1 read call may
                                // have muletiple keys.
                                + "rocksdb_read_batches\t"
                                + metrics.rocksReadBatches()
                                + "\toperations\n"

                                // Total keys requested.
                                + "rocksdb_read_keys\t"
                                + metrics.rocksReadKeys()
                                + "\tkeys\n"

                                + "rocksdb_read_time_sec\t"
                                + metrics.rocksReadSec()
                                + "\tseconds\n"

                                + "rocksdb_average_read_in_sec_per_key\t"
                                + metrics.averageReadInSecsPerKey()
                                + "\tmicroseconds_per_key\n"

                                // total number of batch write operation , can contain 100 records
                                + "rocksdb_write_calls\t"
                                + metrics.rocksWriteCalls()
                                + "\toperations\n"

                                + "rocksdb_write_records\t"
                                + metrics.rocksWriteRecords()
                                + "\trecords\n"

                                + "rocksdb_write_time_sec\t"
                                + metrics.rocksWriteSecs()
                                + "\tseconds\n"

                                + "rocksdb_average_write_time_sec\t"
                                + metrics.averageWriteInSec()
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

                                + "cache_maximum_estimated_bytes\t"
                                + metrics.maximumEstimatedBytes()
                                + "\tbytes\n"

                                + "cache_active_buckets\t"
                                + metrics.activeBuckets()
                                + "\tbuckets\n"

                                // No. of multi read operations.
                                + "rocksdb_read_calls\t"
                                + metrics.rocksReadCalls()
                                + "\toperations\n"

                                // Total membership keys asked
                                + "rocksdb_read_keys\t"
                                + metrics.rocksReadKeys()
                                + "\tkeys\n"

                                + "rocksdb_read_time_sec\t"
                                + metrics.rocksReadSecs()
                                + "\tseconds\n"

                                + "rocksdb_average_read_in_sec_per_key\t"
                                + metrics.averageReadMicrosPerKey()
                                + "\tmicroseconds_per_key\n"

                                + "rocksdb_write_calls\t"
                                + metrics.rocksWriteCalls()
                                + "\toperations\n"

                                + "rocksdb_write_time_sec\t"
                                + metrics.rocksWriteSecs()
                                + "\tseconds\n"

                                + "rocksdb_average_write_time_sec\t"
                                + metrics.averageWriteSecs()
                                + "\tseconds\n"

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
