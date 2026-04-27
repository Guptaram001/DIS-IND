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


public class CSVStreamingSource {

    private static final Set<String> NULL_TOKENS =
            Set.of("", "null", "NULL", "N/A", "n/a", "NA", "na", "none", "None", "NONE", "-");

    public static Source<RawEvent.Batch, NotUsed> stream(String path, int batchSize) {

        return Source.unfoldResource(
                () -> {
                    Reader reader = Files.newBufferedReader(Path.of(path));

                    CSVParser parser = CSVFormat.DEFAULT.builder()
                            .setHeader()
                            .setSkipHeaderRecord(true)
                            .setIgnoreEmptyLines(true)
                            .setTrim(true)
                            .build()
                            .parse(reader);

                    return new CsvState(reader, parser, parser.iterator(), batchSize);
                },

                state -> {
                    List<RawEvent> events = new ArrayList<>();

                    while (state.iterator.hasNext() && events.size() < state.batchSize) {
                        CSVRecord record = state.iterator.next();

                        for (int a = 0; a < record.size(); a++) {
                            String raw = record.get(a);
                            String value = NULL_TOKENS.contains(raw) ? "__NULL__" : raw;

                            events.add(new RawEvent.Insert(
                                    (short) a,
                                    value,
                                    state.rowId
                            ));
                        }

                        state.rowId++;
                    }

                    if (events.isEmpty()) {
                        return Optional.empty();
                    }

                    RawEvent.Batch batch =
                            new RawEvent.Batch(events, state.batchId++);

                    System.out.println(
                            "CSV created batch " + batch.batchId()
                                    + " size=" + batch.events().size()
                    );

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

        long rowId = 0;
        long batchId = 0;

        CsvState(
                Reader reader,
                CSVParser parser,
                Iterator<CSVRecord> iterator,
                int batchSize
        ) {
            this.reader = reader;
            this.parser = parser;
            this.iterator = iterator;
            this.batchSize = batchSize;
        }
    }
}