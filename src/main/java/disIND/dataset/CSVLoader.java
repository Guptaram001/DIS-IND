package disIND.dataset;

import disIND.model.RawEvent;
import org.apache.commons.csv.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVLoader {
    private static final Logger log = LoggerFactory.getLogger(CSVLoader.class);

    private static final Set<String> NULL_TOKENS =
            Set.of("", "null", "NULL", "N/A", "n/a", "NA", "na", "none", "None", "NONE", "-");

    public record Result(String[] attrNames, List<RawEvent.Batch> batches, long rowCount) {}


    public static Result load(String path, int batchSize) throws IOException {
        System.out.println("[CSVLoader] Loading: " + path);
        Path p = Paths.get(path);
        if (!Files.exists(p)) throw new FileNotFoundException("File not found: " + path);

        List<RawEvent> events = new ArrayList<>();
        String[] attrNames;
        long rowCount = 0;

        try (Reader reader = Files.newBufferedReader(p);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader().setSkipHeaderRecord(true).setIgnoreEmptyLines(true)
                     .setTrim(true).build().parse(reader)) {

            // Extract header
            Map<String, Integer> headerMap = parser.getHeaderMap();
            attrNames = new String[headerMap.size()];
            for (Map.Entry<String, Integer> e : headerMap.entrySet()) {
                attrNames[e.getValue()] = e.getKey();
            }
            System.out.printf("[CSVLoader] Found %d attributes: %s%n",
                    attrNames.length, Arrays.toString(attrNames));

            // Parse rows
            for (CSVRecord record : parser) {
                for (int a = 0; a < attrNames.length && a < record.size(); a++) {
                    String raw = record.get(a);
                    String value = NULL_TOKENS.contains(raw) ? "__NULL__" : raw;
                    events.add(new RawEvent.Insert((short) a, value, rowCount));
                }
                rowCount++;
                if (rowCount % 10000 == 0)
                    System.out.printf("[CSVLoader] Parsed %,d rows...%n", rowCount);
            }
        }

        System.out.printf("[CSVLoader] Loaded %,d rows → %,d events%n", rowCount, events.size());
        List<RawEvent.Batch> batches = toBatches(events, batchSize);
        System.out.printf("[CSVLoader] Split into %d batches of size %d%n", batches.size(), batchSize);
        return new Result(attrNames, batches, rowCount);
    }


    public static boolean validate(String path) {
        try {
            Path p = Paths.get(path);
            if (!Files.exists(p) || !Files.isReadable(p)) return false;
            // Peek at first line to check for header
            try (BufferedReader br = Files.newBufferedReader(p)) {
                String line = br.readLine();
                return line != null && line.contains(",");
            }
        } catch (IOException e) {
            return false;
        }
    }

    public static List<RawEvent.Batch> toBatches(List<RawEvent> events, int batchSize) {
        List<RawEvent.Batch> batches = new ArrayList<>();
        long batchId = 0;
        for (int i = 0; i < events.size(); i += batchSize) {
            int end = Math.min(i + batchSize, events.size());
            batches.add(new RawEvent.Batch(events.subList(i, end), batchId++));
        }
        return batches;
    }
}
