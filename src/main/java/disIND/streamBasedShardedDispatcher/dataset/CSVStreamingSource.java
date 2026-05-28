package disIND.streamBasedShardedDispatcher.dataset;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import disIND.streamBasedShardedDispatcher.model.RawEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class CSVStreamingSource {

    private static final Set<String> NULL_TOKENS = Set.of("", "null", "NULL", "N/A", "n/a", "NA", "na", "none", "None", "NONE", "-");

    public record AttributeKey(String fileName, String columnName) {}

    private static final ConcurrentHashMap<AttributeKey, Short> attrMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Short, AttributeKey> reverseMap = new ConcurrentHashMap<>();
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private static final AtomicLong GLOBAL_BATCH_ID = new AtomicLong(0);

    public static Source<RawEvent.Batch, NotUsed> stream(String path, int batchSize) {

        return Source.unfoldResource(
                () -> {
                    Reader reader = Files.newBufferedReader(Path.of(path));
                    String fileName = Path.of(path).getFileName().toString();

                    CSVParser parser = CSVFormat.DEFAULT.builder()
                            .setDelimiter(';')
                            .setQuote('"')
                            .setHeader()
                            .setSkipHeaderRecord(true)
                            .setIgnoreEmptyLines(true)
                            .setTrim(true)
                            .build()
                            .parse(reader);
                    Map<String, Integer> headerMap = parser.getHeaderMap();
                    short[] attrIds = new short[headerMap.size()];

                    for (Map.Entry<String,Integer> e : headerMap.entrySet()) {
                        String columnName = e.getKey();
                        int index = e.getValue();
                        attrIds[index] = resolveAttrId(fileName, columnName);
                    }
                    return new CsvState(reader, parser, parser.iterator(), batchSize,attrIds);
                },


                state -> {
                    List<RawEvent> events = new ArrayList<>();

                    int rows=0;
                    while (state.iterator.hasNext() && rows < state.batchSize) {
                        CSVRecord record = state.iterator.next();

                        for (int a = 0; a < record.size(); a++) {
                            String raw = record.get(a);
                            String value = NULL_TOKENS.contains(raw) ? "__NULL__" : raw;

                            events.add(new RawEvent.Insert(
                                    state.attrIds[a],
                                    value,
                                    state.rowId
                            ));
                        }

                        state.rowId++;
                        rows++;
                    }

                    if (events.isEmpty()) {
                        return Optional.empty();
                    }

                    RawEvent.Batch batch = new RawEvent.Batch(events,  GLOBAL_BATCH_ID.getAndIncrement());

                    System.out.println("CSV created batch " + batch.batchId() + " size=" + batch.events().size());

                    return Optional.of(batch);
                },

                state -> {
                    state.parser.close();
                    state.reader.close();
                }
        );
    }

    private static class CsvState {
        final Reader reader;
        final CSVParser parser;
        final Iterator<CSVRecord> iterator;
        final int batchSize;
        final short[] attrIds;

        long rowId = 0;

        CsvState(Reader reader, CSVParser parser, Iterator<CSVRecord> iterator, int batchSize,short[] attrIds) {
            this.reader = reader;
            this.parser = parser;
            this.iterator = iterator;
            this.batchSize = batchSize;
            this.attrIds = attrIds;
        }
    }

    private static short resolveAttrId(String fileName, String columnName) {
        AttributeKey key = new AttributeKey(fileName, columnName);
        return attrMap.computeIfAbsent(key, k -> {
                    short id = (short) nextId.getAndIncrement();
                    reverseMap.put(id, k);
                    return id;
                });
    }
}