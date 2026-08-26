package disIND.valueBased.monitor;

import disIND.valueBased.model.SharedModel.PruneMetrics;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class ResultMetricsWriter {

        private final Path diagnosticsDirectory;

        public ResultMetricsWriter(Logger log) {
                this.diagnosticsDirectory = Path.of(System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR",
                                "diagnostics"));
        }

        public void writeAll(long candidateEvaluationsWithoutPruning, long exactValueProbesWithoutPruning,
                        int finalRound, PruneMetrics pruneMetrics, long activeClusterEntriesAcrossBuckets,
                        long distinctActiveClusterSignatures) {
                Objects.requireNonNull(pruneMetrics, "pruneMetrics");
                writeComparisonMetrics(candidateEvaluationsWithoutPruning,
                                exactValueProbesWithoutPruning, finalRound);
                writePruneMetrics(pruneMetrics);
                writeClusterMetrics(activeClusterEntriesAcrossBuckets, distinctActiveClusterSignatures);
        }

        private void writeComparisonMetrics(long candidateEvaluations, long exactValueProbes,
                        int finalRound) {

                String contents = "metric\tcount\tunit\n"

                                + "candidate_evaluations_without_pruning\t"
                                + candidateEvaluations
                                + "\tcandidates\n"

                                + "exact_value_probes_without_pruning\t"
                                + exactValueProbes
                                + "\tvalue_probes\n"

                                + "final_round\t"
                                + finalRound
                                + "\tround\n";

                write("comparisons-without-pruning.tsv", contents, "NO-PRUNING-METRICS");
        }

        private void writePruneMetrics(PruneMetrics metrics) {

                long filterPruned = Math.addExact(metrics.wholeCountPruned(),
                                Math.addExact(metrics.partitionCountPruned(), metrics.cqfPruned()));

                String contents = "metric\tcount\tunit\n"

                                + "invalid_lhs_skips\t"
                                + metrics.invalidLhsSkips()
                                + "\tcandidate_value_checks\n"

                                + "valid_rhs_skips\t"
                                + metrics.validRhsSkips()
                                + "\tcandidate_value_checks\n"

                                + "same_batch_skips\t"
                                + metrics.sameBatchSkips()
                                + "\tcandidate_value_checks\n"

                                + "direct_lhs_rejections\t"
                                + metrics.directLhsRejections()
                                + "\tcandidate_batch_events\n"

                                + "whole_count_pruned\t"
                                + metrics.wholeCountPruned()
                                + "\tcandidates\n"

                                + "partition_count_pruned\t"
                                + metrics.partitionCountPruned()
                                + "\tcandidates\n"

                                + "cqf_pruned\t"
                                + metrics.cqfPruned()
                                + "\tcandidates\n"

                                + "filter_pruned_total\t"
                                + filterPruned
                                + "\tcandidates\n"

                                + "transitively_validated\t"
                                + metrics.transitivelyValidated()
                                + "\tcandidates\n"

                                + "exact_tested\t"
                                + metrics.exactTested()
                                + "\tcandidates\n"

                                + "exact_rejected\t"
                                + metrics.exactRejected()
                                + "\tcandidates\n"

                                + "exact_validated\t"
                                + metrics.exactValidated()
                                + "\tcandidates\n";

                write("prune-metrics.tsv", contents, "PRUNE-METRICS");
        }

        private void writeClusterMetrics(long activeEntries, long distinctSignatures) {

                long duplicateEntries = Math.subtractExact(activeEntries, distinctSignatures);
                String contents = "metric\tcount\tunit\n"

                                + "active_cluster_entries_across_buckets\t"
                                + activeEntries
                                + "\tclusters\n"

                                + "distinct_active_cluster_signatures\t"
                                + distinctSignatures
                                + "\tclusters\n"

                                + "duplicate_cluster_entries_across_buckets\t"
                                + duplicateEntries
                                + "\tclusters\n";

                write("cluster-metrics.tsv", contents, "CLUSTER-METRICS");
        }

        private void write(String filename, String contents, String metricType) {
                Path file = diagnosticsDirectory.resolve(filename);
                try {
                        Files.createDirectories(diagnosticsDirectory);
                        Files.writeString(file, contents, StandardCharsets.UTF_8);
                } catch (IOException exception) {
                }
        }
}