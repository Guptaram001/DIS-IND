package disIND.valueBased.monitor;

import disIND.valueBased.model.SharedModel.IngestionResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class PipelineMetricsWriter {

        private final Path metricsFile;

        public PipelineMetricsWriter() {

                this.metricsFile = Path.of(
                                System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR", "diagnostics"),
                                "pipeline-metrics.tsv");
        }

        public Path write(IngestionResult ingestion, long ingestionNanos,
                        long finalizationNanos, long totalPipelineNanos, int confirmedUnarys) {

                Objects.requireNonNull(ingestion, "ingestion");
                double ingestionSeconds = seconds(ingestionNanos);
                double finalizationSeconds = seconds(finalizationNanos);
                double totalSeconds = seconds(totalPipelineNanos);
                double ingestionRowsPerSecond = rate(ingestion.totalRows(), ingestionSeconds);
                double pipelineRowsPerSecond = rate(ingestion.totalRows(), totalSeconds);
                String contents = "metric\tvalue\tunit\n"

                                + "total_rows\t"
                                + ingestion.totalRows()
                                + "\trows\n"

                                + "total_batches\t"
                                + ingestion.totalBatches()
                                + "\tbatches\n"

                                + "total_cells\t"
                                + ingestion.totalCells()
                                + "\tcells\n"

                                + "final_round\t"
                                + ingestion.finalRound()
                                + "\tround\n"

                                + "confirmed_unarys\t"
                                + confirmedUnarys
                                + "\tinds\n"

                                + "ingestion_runtime_seconds\t"
                                + ingestionSeconds
                                + "\tseconds\n"

                                + "finalization_runtime_seconds\t"
                                + finalizationSeconds
                                + "\tseconds\n"

                                + "total_pipeline_runtime_seconds\t"
                                + totalSeconds
                                + "\tseconds\n"

                                + "ingestion_rows_per_second\t"
                                + ingestionRowsPerSecond
                                + "\trows_per_second\n"

                                + "pipeline_rows_per_second\t"
                                + pipelineRowsPerSecond
                                + "\trows_per_second\n";

                try {
                        Files.createDirectories(metricsFile.getParent());
                        Files.writeString(metricsFile, contents, StandardCharsets.UTF_8);

                } catch (IOException exception) {

                }
                return metricsFile.toAbsolutePath();

        }

        private static double seconds(long nanos) {
                return nanos / 1_000_000_000.0;
        }

        private static double rate(long count, double seconds) {
                return seconds == 0.0 ? 0.0 : count / seconds;
        }
}
