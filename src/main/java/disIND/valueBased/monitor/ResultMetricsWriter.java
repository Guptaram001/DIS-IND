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

        public void writeAll(long exactValueProbesWithoutPruning,
                        int finalRound, PruneMetrics pruneMetrics, long activeClusterEntriesAcrossBuckets,
                        long distinctActiveClusterSignatures) {
                Objects.requireNonNull(pruneMetrics, "pruneMetrics");
                writeComparisonMetrics(exactValueProbesWithoutPruning, finalRound);
                writePruneMetrics(pruneMetrics);
                writeClusterMetrics(activeClusterEntriesAcrossBuckets, distinctActiveClusterSignatures);
        }

        private void writeComparisonMetrics(long exactValueProbes,
                        int finalRound) {

                String contents = "metric\tcount\tunit\n"

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

                                // LHS insertion checks skipped for already rejected candidates.
                                + "lhs_insertion_invalid_skips\t"
                                + metrics.lhsInsertionInvalidSkips()
                                + "\tcandidate_value_checks\n"

                                // Adding on rhs for valid - skip
                                + "rhs_insertion_valid_skips\t"
                                + metrics.rhsInsertionValidSkips()
                                + "\tcandidate_value_checks\n"

                                + "rhs_deletion_invalid_skips\t"
                                + metrics.rhsDeletionInvalidSkips()
                                + "\tcandidate_value_checks\n"

                                + "lhs_deletion_valid_skips\t"
                                + metrics.lhsDeletionValidSkips()
                                + "\tcandidate_value_checks\n"

                                + "mixed_update_skips\t"
                                + metrics.mixedUpdateSkips()
                                + "\tcandidate_value_checks\n"

                                // Skips same batch changes
                                + "same_batch_rejected_candidate_skips\t"
                                + metrics.sameBatchRejectedCandidateSkips()
                                + "\tcandidate_value_checks\n"

                                // Skips further iterations due to some counterexample
                                + "direct_lhs_rejections\t"
                                + metrics.directLhsRejections()
                                + "\tcandidate_batch_events\n"

                                // Whole distinct value ocunt > than other
                                + "whole_count_pruned\t"
                                + metrics.wholeCountPruned()
                                + "\tcandidates\n"

                                // : 4+16+fine pruned
                                + "partition_count_pruned\t"
                                + metrics.partitionCountPruned()
                                + "\tcandidates\n"

                                + "partition_4_pruned\t"
                                + metrics.partition4Pruned()
                                + "\tcandidates\n"

                                + "partition_16_pruned\t"
                                + metrics.partition16Pruned()
                                + "\tcandidates\n"

                                + "partition_fine_pruned\t"
                                + metrics.partitionFinePruned()
                                + "\tcandidates\n"

                                + "partition_4_comparisons\t"
                                + metrics.partition4Comparisons()
                                + "\tpartition_comparisons\n"

                                + "partition_16_comparisons\t"
                                + metrics.partition16Comparisons()
                                + "\tpartition_comparisons\n"

                                + "partition_fine_comparisons\t"
                                + metrics.partitionFineComparisons()
                                + "\tpartition_comparisons\n"

                                + "cqf_pruned\t"
                                + metrics.cqfPruned()
                                + "\tcandidates\n"

                                // whole_count_pruned + partition_count_pruned + cqf_pruned.
                                + "filter_pruned_total\t"
                                + filterPruned
                                + "\tcandidates\n"

                                + "transitively_validated\t"
                                + metrics.transitivelyValidated()
                                + "\tcandidates\n"

                                // Candidates that pass through filters
                                + "exact_tested\t"
                                + metrics.exactTested()
                                + "\tcandidates\n"

                                // Candidates rejected at exact scan or lhs-based operation
                                + "exact_rejected\t"
                                + metrics.exactRejected()
                                + "\tcandidates\n"

                                // Final remaining inds after final validataion
                                + "exact_validated\t"
                                + metrics.exactValidated()
                                + "\tcandidates\n";

                write("prune-metrics.tsv", contents, "PRUNE-METRICS");
        }

        private void writeClusterMetrics(long activeEntries, long distinctSignatures) {

                long duplicateEntries = Math.subtractExact(activeEntries, distinctSignatures);
                String contents = "metric\tcount\tunit\n"

                                // Active clusters total all buckets including duplicates too
                                + "active_cluster_entries_across_buckets\t"
                                + activeEntries
                                + "\tclusters\n"

                                + "distinct_active_cluster_signatures\t"
                                + distinctSignatures
                                + "\tclusters\n"

                                // Duplicated clusters
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
