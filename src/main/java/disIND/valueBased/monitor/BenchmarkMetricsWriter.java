package disIND.valueBased.monitor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Update-phase wall time through batch/CM acknowledgments; excludes final report and full disk flush. */
public final class BenchmarkMetricsWriter {
    private BenchmarkMetricsWriter() {}

    public static void write(String operation, String sampleHash, long rows, long cells, long batches,
            long elapsedNanos, String startedAt, String endedAt) throws IOException {
        Path directory = Path.of(System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR", "diagnostics"));
        Files.createDirectories(directory);
        double seconds = elapsedNanos / 1_000_000_000.0;
        Files.writeString(directory.resolve("benchmark-metrics.tsv"),
                "metric\tvalue\noperation\t" + operation
                + "\nsample_manifest_sha256\t" + sampleHash
                + "\nstarted_at_utc\t" + startedAt + "\nended_at_utc\t" + endedAt
                + "\nupdate_rows\t" + rows + "\nupdate_cells\t" + cells + "\nupdate_batches\t" + batches
                + "\nelapsed_seconds\t" + seconds + "\nrows_per_second\t" + (rows / seconds)
                + "\ncells_per_second\t" + (cells / seconds) + "\n");
    }
}
