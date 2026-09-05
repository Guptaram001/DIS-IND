package disIND.valueBased.monitor;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BatchTimeMonitor implements AutoCloseable {

    private final PrintWriter writer;

    public BatchTimeMonitor() {
        Path directory = Path.of(System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR", "diagnostics"));

        try {
            Files.createDirectories(directory);
            Path file = directory.resolve("batch-time-monitor.tsv");

            writer = new PrintWriter(file.toFile());
            writer.println("epoch" + "\ttable_id" + "\tbatch_id" + "\tround" + "\tbatch_rows" + "\tprocessed_rows"
                    + "\tdispatch_started_seconds" + "\tcompletion_seconds" + "\tbatch_latency_seconds");
            writer.flush();
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot create batch time monitor file", exception);
        }
    }

    public void write(int epoch, int tableId, int batchId, int round, int batchRows, long processedRows,
            double dispatchStartedSeconds, double completionSeconds, double batchLatencySeconds) {

        writer.printf("%d\t%d\t%d\t%d\t%d\t%d\t%.6f\t%.6f\t%.6f%n", epoch, tableId, batchId, round, batchRows,
                processedRows, dispatchStartedSeconds, completionSeconds, batchLatencySeconds);
        writer.flush();
        if (writer.checkError())
            throw new IllegalStateException("Failed to write ingestion checkpoint");
    }

    @Override
    public void close() {
        writer.close();
    }
}
