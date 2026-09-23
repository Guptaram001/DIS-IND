package disIND.valueBased.dataset;

import disIND.valueBased.utility.UserConfig;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

final class ShardManifest {
    record Part(int tableId, int batchId, int startRow, int rows, int columns,
            boolean tbl, Path path, String sha256) {
    }

    final List<List<Part>> tables;

    ShardManifest(List<List<Part>> tables) {
        this.tables = tables;
    }

    static ShardManifest load(Path manifest, List<String> sources, List<Integer> columns, int chunkSize)
            throws IOException {
        Properties p = new Properties();
        try (var input = Files.newInputStream(manifest)) {
            p.load(input);
        }
        require("1".equals(p.getProperty("version")), "Unsupported shard manifest version");
        require(integer(p, "chunk_size") == chunkSize, "Shard chunk size differs; regenerate shards");
        require(p.getProperty("delimiter", "").equals(UserConfig.separator), "Shard delimiter differs");
        require(p.getProperty("source_header", "").equals(Boolean.toString(UserConfig.inputFileHasHeader)),
                "Shard header setting differs");
        require(integer(p, "tables") == sources.size() && columns.size() == sources.size(),
                "Shard table count differs from input metadata");
        Path root = manifest.toRealPath().getParent();
        List<List<Part>> tables = new ArrayList<>();
        Set<Path> used = new HashSet<>();
        for (int t = 0; t < sources.size(); t++) {
            String key = "table." + t + ".";
            Path source = Path.of(sources.get(t));
            require(source.getFileName().toString().equals(p.getProperty(key + "name")),
                    "Shard table order/name differs");
            require(Files.size(source) == number(p, key + "source_bytes")
                    && Files.getLastModifiedTime(source).toMillis() == number(p, key + "source_mtime_ms"),
                    "Source changed: " + source + "; regenerate shards on the coordinator");
            int cols = integer(p, key + "columns");
            require(cols > 0 && cols == columns.get(t), "Shard column count differs for " + source);
            int batchRows = Math.max(1, chunkSize / cols);
            int count = integer(p, key + "parts");
            int start = 0;
            List<Part> parts = new ArrayList<>();
            for (int b = 0; b < count; b++) {
                String k = key + "part." + b + ".";
                int rows = integer(p, k + "rows");
                require(rows > 0 && rows <= batchRows && (b == count - 1 || rows == batchRows),
                        "Invalid shard batch boundaries: " + k);
                require(integer(p, k + "start_row") == start, "Non-contiguous shard rows: " + k);
                String filename = p.getProperty(k + "file");
                require(filename != null, "Missing shard path: " + k);
                Path path = root.resolve(filename).toRealPath();
                require(path.startsWith(root) && used.add(path) && Files.isRegularFile(path),
                        "Unsafe/duplicate shard: " + path);
                require(Files.size(path) == number(p, k + "bytes"), "Shard size changed: " + path);
                String hash = p.getProperty(k + "sha256", "");
                require(hash.matches("[0-9a-f]{64}"), "Missing shard checksum: " + path);
                parts.add(new Part(t, b, start, rows, cols, DataLoader.isTbl(sources.get(t)), path, hash));
                start = Math.addExact(start, rows);
            }
            require(start == number(p, key + "rows"), "Shard row count differs");
            tables.add(List.copyOf(parts));
        }
        return new ShardManifest(List.copyOf(tables));
    }

    static long number(Properties p, String key) throws IOException {
        try {
            long n = Long.parseLong(p.getProperty(key));
            require(n >= 0, "Negative manifest field: " + key);
            return n;
        } catch (NumberFormatException e) {
            throw new IOException("Invalid manifest field: " + key, e);
        }
    }

    static int integer(Properties p, String key) throws IOException {
        long n = number(p, key);
        require(n <= Integer.MAX_VALUE, "Manifest field too large: " + key);
        return (int) n;
    }

    static void require(boolean valid, String message) throws IOException {
        if (!valid)
            throw new IOException(message);
    }
}
