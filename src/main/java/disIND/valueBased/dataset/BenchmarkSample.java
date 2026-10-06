package disIND.valueBased.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/** Validates sample provenance before ingestion or timing. */
record BenchmarkSample(Path directory, long rows, String manifestSha256, List<Table> tables) {
    record Table(int tableId, Path file, long rows) {}

    static BenchmarkSample load(Path directory, List<String> sources, char delimiter, boolean header) throws Exception {
        Path manifestPath = directory.resolve("manifest.json");
        JsonNode manifest = new ObjectMapper().readTree(manifestPath.toFile());
        if (manifest.path("version").asInt() != 1 || manifest.path("rows").asLong() <= 0
                || !manifest.path("delimiter").isTextual() || !manifest.path("header").isBoolean()
                || !manifest.path("tables").isArray())
            throw new IllegalArgumentException("Invalid sample manifest: " + manifestPath);
        if (!manifest.path("delimiter").asText().equals(String.valueOf(delimiter))
                || manifest.path("header").asBoolean() != header)
            throw new IllegalArgumentException("Sample CSV settings mismatch in " + manifestPath
                    + ": sample delimiter='" + manifest.path("delimiter").asText()
                    + "', header=" + manifest.path("header").asBoolean()
                    + "; dataset expects delimiter='" + delimiter + "', header=" + header
                    + ". Regenerate the sample with --delimiter '" + delimiter + "'"
                    + (header ? " (with headers)." : " --no-header.")
                    + " Do not edit the manifest to bypass this check.");
        List<Table> tables = new ArrayList<>();
        Set<Integer> seen = new HashSet<>();
        long total = 0;
        for (JsonNode entry : manifest.path("tables")) {
            String name = entry.path("file").asText();
            int tableId = -1;
            for (int i = 0; i < sources.size(); i++)
                if (Path.of(sources.get(i)).getFileName().toString().equals(name))
                    tableId = i;
            if (tableId < 0 || !seen.add(tableId))
                throw new IllegalArgumentException("Unknown or duplicate sample table: " + name);
            Path sample = directory.resolve(name);
            long count = entry.path("rows").asLong(-1);
            if (count < 0 || count > entry.path("source_rows").asLong(-1)
                    || !sha256(Path.of(sources.get(tableId))).equals(entry.path("source_sha256").asText())
                    || !sha256(sample).equals(entry.path("sample_sha256").asText()))
                throw new IllegalArgumentException("Sample/source integrity check failed: " + name);
            total = Math.addExact(total, count);
            tables.add(new Table(tableId, sample, count));
        }
        if (seen.size() != sources.size() || total != manifest.path("rows").asLong())
            throw new IllegalArgumentException("Sample manifest table coverage or row total mismatch");
        tables.sort(java.util.Comparator.comparingInt(Table::tableId));
        return new BenchmarkSample(directory, total, sha256(manifestPath), List.copyOf(tables));
    }

    private static String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (var stream = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536];
            int read;
            while ((read = stream.read(buffer)) != -1)
                digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
