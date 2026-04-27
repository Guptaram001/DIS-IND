package disIND.streamBasedNoCentralCoordinator.dataset;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import disIND.streamBasedNoCentralCoordinator.model.RawEvent;
import org.apache.commons.csv.*;

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

                    Iterator<CSVRecord> iterator = parser.iterator();

                    return new State(parser, iterator, batchSize);
                },

                state -> {
                    if (!state.iterator.hasNext()) return Optional.empty();

                    List<RawEvent> batchEvents = new ArrayList<>();

                    while (state.iterator.hasNext() && batchEvents.size() < state.batchSize) {
                        CSVRecord record = state.iterator.next();

                        for (int a = 0; a < record.size(); a++) {
                            String raw = record.get(a);
                            String value = NULL_TOKENS.contains(raw) ? "__NULL__" : raw;

                            batchEvents.add(
                                    new RawEvent.Insert(
                                            (short) a,
                                            value,
                                            state.rowId++
                                    )
                            );
                        }
                    }

                    RawEvent.Batch batch =
                            new RawEvent.Batch(batchEvents, state.batchId++);

                    return Optional.of(batch);
                },

                state -> {
                    try {
                        state.parser.close();
                    } catch (Exception ignored) {}
                }
        );
    }

    static class State {
        final CSVParser parser;
        final Iterator<CSVRecord> iterator;
        final int batchSize;

        long batchId = 0;
        long rowId = 0;

        State(CSVParser parser, Iterator<CSVRecord> iterator, int batchSize) {
            this.parser = parser;
            this.iterator = iterator;
            this.batchSize = batchSize;
        }
    }
}