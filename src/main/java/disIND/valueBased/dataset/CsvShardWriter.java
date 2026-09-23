package disIND.valueBased.dataset;

import disIND.valueBased.utility.UserConfig;
import org.apache.commons.csv.*;
import java.io.IOException;
import java.nio.file.*;
import java.security.*;
import java.util.*;

public final class CsvShardWriter {
    private CsvShardWriter() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "Usage: create-csv-shards.sh INPUT_DIR OUTPUT_DIR DATASET_NAME CHUNK_CELLS");
        }
        UserConfig.setDatasetDetails(args[2]);
        Path manifest = write(Path.of(args[0]), Path.of(args[1]), Integer.parseInt(args[3]));
        System.out.println("Shard manifest: " + manifest);
    }

    static Path write(Path input, Path output, int chunkSize) throws Exception {
        if (chunkSize <= 0)
            throw new IllegalArgumentException("chunk size must be positive");
        Path sourceRoot = input.toRealPath();
        Path destination = output.toAbsolutePath().normalize();
        // Existing destinations are never overwritten; a failed build has no manifest.
        Files.createDirectories(destination.getParent());
        Path resolved = destination.getParent().toRealPath().resolve(destination.getFileName());
        if (resolved.startsWith(sourceRoot))
            throw new IOException("Shard output must be outside the input directory");
        List<String> files = DataLoader.listInputFiles(sourceRoot);
        if (files.isEmpty())
            throw new IOException("No input files");
        Files.createDirectory(destination);
        Properties manifest = new Properties();
        manifest.setProperty("version", "1");
        manifest.setProperty("chunk_size", Integer.toString(chunkSize));
        manifest.setProperty("delimiter", UserConfig.separator);
        manifest.setProperty("source_header", Boolean.toString(UserConfig.inputFileHasHeader));
        manifest.setProperty("tables", Integer.toString(files.size()));
        var outputFormat = CSVFormat.DEFAULT.builder().setDelimiter(UserConfig.separator.charAt(0)).build();
        for (int t = 0; t < files.size(); t++) {
            Path file = Path.of(files.get(t));
            long size = Files.size(file), mtime = Files.getLastModifiedTime(file).toMillis();
            String key = "table." + t + ".";
            manifest.setProperty(key + "name", file.getFileName().toString());
            manifest.setProperty(key + "source_bytes", Long.toString(size));
            manifest.setProperty(key + "source_mtime_ms", Long.toString(mtime));
            int total = 0, batch = 0;
            try (CSVParser parser = DataLoader.openCSVParser(file.toString(), UserConfig.separator.charAt(0),
                    UserConfig.inputFileHasHeader)) {
                var iterator = parser.iterator();
                CSVRecord pending = iterator.hasNext() ? iterator.next() : null;
                boolean tbl = DataLoader.isTbl(file.toString());
                int columns = UserConfig.inputFileHasHeader ? parser.getHeaderNames().size()
                        : pending == null ? 0 : DataLoader.recordToArray(pending, tbl).length;
                if (columns == 0)
                    throw new IOException("Cannot infer columns for empty/headerless input: " + file);
                manifest.setProperty(key + "columns", Integer.toString(columns));
                int rowsPerBatch = Math.max(1, chunkSize / columns);
                while (pending != null) {
                    String filename = String.format(Locale.ROOT, "table-%04d-batch-%08d.csv", t, batch);
                    Path shard = destination.resolve(filename);
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    int rows = 0;
                    try (var stream = new DigestOutputStream(
                            Files.newOutputStream(shard, StandardOpenOption.CREATE_NEW), digest);
                            var writer = new java.io.OutputStreamWriter(stream,
                                    java.nio.charset.StandardCharsets.UTF_8);
                            CSVPrinter printer = new CSVPrinter(writer, outputFormat)) {
                        while (pending != null && rows < rowsPerBatch) {
                            if (DataLoader.recordToArray(pending, tbl).length != columns)
                                throw new IOException(
                                        "Column mismatch in " + file + " at record " + pending.getRecordNumber());
                            // No shard header. Preserve the parser's field values, including .tbl trailing
                            // fields.
                            printer.printRecord(pending);
                            rows++;
                            pending = iterator.hasNext() ? iterator.next() : null;
                        }
                    }
                    String k = key + "part." + batch + ".";
                    manifest.setProperty(k + "file", filename);
                    manifest.setProperty(k + "rows", Integer.toString(rows));
                    manifest.setProperty(k + "start_row", Integer.toString(total));
                    manifest.setProperty(k + "bytes", Long.toString(Files.size(shard)));
                    manifest.setProperty(k + "sha256", HexFormat.of().formatHex(digest.digest()));
                    total = Math.addExact(total, rows);
                    batch++;
                }
            }
            if (Files.size(file) != size || Files.getLastModifiedTime(file).toMillis() != mtime)
                throw new IOException("Source changed while sharding: " + file);
            manifest.setProperty(key + "parts", Integer.toString(batch));
            manifest.setProperty(key + "rows", Integer.toString(total));
            System.out.printf("[Shards] %s: %d rows, %d batches%n", file.getFileName(), total, batch);
        }
        Path path = destination.resolve("manifest.properties");
        try (var out = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)) {
            manifest.store(out,
                    "DIS-IND shards; immutable input; shard headers omitted; source files retained for metadata");
        }
        return path;
    }
}
