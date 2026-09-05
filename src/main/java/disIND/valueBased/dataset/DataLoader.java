package disIND.valueBased.dataset;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import disIND.valueBased.actors.AsyncBatchDispatcher;
import disIND.valueBased.actors.DirectBatchAggregatorActor;
import disIND.valueBased.actors.INDGuardian;
import disIND.valueBased.actors.ValueOwnerActor;
import disIND.valueBased.ingestion.ColumnMajorBatchBuilder;
import disIND.valueBased.ingestion.ValueMajorBatchBuilder;
import disIND.valueBased.model.IngestionMode;
import disIND.valueBased.model.SharedModel;
import disIND.valueBased.model.SharedModel.BDCommand;
import disIND.valueBased.model.SharedModel.BDReply;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.model.SharedModel.ColType;
import disIND.valueBased.model.SharedModel.ColumnInfo;
import disIND.valueBased.model.SharedModel.DataOrientation;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import disIND.valueBased.model.SharedModel.IndReport;
import disIND.valueBased.model.SharedModel.IngestionResult;
import disIND.valueBased.model.SharedModel.InputBatchDetails;
import disIND.valueBased.model.SharedModel.RCCommand;
import disIND.valueBased.monitor.PipelineMetricsWriter;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.protocol.ValueOwnerProtocol.StoreBatch;
import disIND.valueBased.structures.ValueOwnerBatchCodec;
import disIND.valueBased.utility.InferDataAttributes;
import disIND.valueBased.utility.UserConfig;

public final class DataLoader {
    private static final Pattern PAT_CSV_EXT = Pattern.compile("\\.(csv|tsv|tbl)$", Pattern.CASE_INSENSITIVE);
    private static final PipelineMetricsWriter pipelineMetricsWriter = new PipelineMetricsWriter();

    public record PreparedBatch(int epoch, int tableId, int startRowId, int rowCount,
            Map<Integer, BatchBody> ownerBatches, int round, int individualBatchId, DataOrientation orientation) {

        public PreparedBatch {
            ownerBatches = Map.copyOf(ownerBatches);
        }

    }

    public interface OrientetationBatchBuilder {
        void add(int columnId, String value, int rowId, int delta);

        BatchBody build();
    }

    private DataLoader() {
    }

    // Discover the basic metadata of the dataset
    public static INDGuardian.Config discoverConfig(String csvDir, DataOrientation orientation,
            CandidateTrackingMode candidateTracking) throws IOException {
        Path dir = Paths.get(csvDir);
        List<String> files = listInputFiles(dir);

        if (files.isEmpty())
            throw new IllegalArgumentException("[Loader] No .csv or .tbl files in: " + csvDir);

        Map<Integer, ColumnInfo> columns = new LinkedHashMap<>();
        List<String> tableNames = new ArrayList<>();
        List<Integer> nCols = new ArrayList<>();
        List<Integer> offsets = new ArrayList<>();
        int totalCols = 0;
        System.out.println("[Loader] Discovering files and columns:");
        UserConfig.setDatasetDetails(UserConfig.DATASET_NAME);

        for (int tableId = 0; tableId < files.size(); tableId++) {
            String file = files.get(tableId);
            String table = tableName(file);
            tableNames.add(table);

            boolean tbl = isTbl(file);
            boolean hasHeader = UserConfig.inputFileHasHeader;
            char delimiter = UserConfig.separator.charAt(0);

            try (CSVParser parser = openCSVParser(file, delimiter, hasHeader)) {
                Iterator<CSVRecord> iterator = parser.iterator();
                String[] columnNames;
                List<String[]> sampleRows = new ArrayList<>();

                if (hasHeader)
                    columnNames = parser.getHeaderNames().stream().map(DataLoader::normalize).toArray(String[]::new);
                else {
                    if (!iterator.hasNext())
                        continue;

                    // From first row find the number of columns as header and add it to the
                    // columnnames dummy wise, also add to sample
                    String[] firstRow = recordToArray(iterator.next(), tbl);
                    columnNames = new String[firstRow.length];
                    for (int i = 0; i < columnNames.length; i++)
                        columnNames[i] = "c" + i;

                    sampleRows.add(firstRow);
                }

                while (iterator.hasNext() && sampleRows.size() < 1000) {
                    CSVRecord record = iterator.next();
                    String[] row = recordToArray(record, tbl);

                    if (row.length != columnNames.length)
                        throw new IllegalArgumentException("Mismatch rows and columns");

                    sampleRows.add(row);
                }

                nCols.add(columnNames.length);
                offsets.add(totalCols);

                for (int localCol = 0; localCol < columnNames.length; localCol++) {
                    int globalCol = totalCols + localCol;
                    String columnName = hasHeader ? InferDataAttributes.clean(columnNames[localCol]) : "c" + localCol;
                    ColType type = InferDataAttributes.inferColType(table + "." + columnName, localCol, sampleRows);
                    columns.put(globalCol, new ColumnInfo(globalCol, tableId, table, localCol, columnName, type));
                }
                totalCols += columnNames.length;
            }
        }
        DatasetMetadata metadata = new DatasetMetadata(totalCols, offsets, nCols, tableNames, columns);
        return INDGuardian.Config.withAll(metadata, orientation, candidateTracking);
    }

    public static void run(ActorRef<BDCommand> guardian, ActorSystem<?> system, DatasetMetadata metadata,
            String csvDir, int chunkSize, int timeoutSec, String outputFile, DataOrientation orientation)
            throws Exception {

        // AskPattern.ask(guardian, BDCommand.GetIngestionReady::new,
        // Duration.ofSeconds(10), system.scheduler())
        // .toCompletableFuture().get();
        Path dir = Paths.get(csvDir);
        List<String> files = listInputFiles(dir);

        if (files.isEmpty()) {
            System.err.println("[Loader] No .csv or .tbl files in: " + csvDir);
            return;
        }

        long pipelineStarted = System.nanoTime();
        long ingestionStarted = pipelineStarted;
        IngestionResult ingestion = ingestAllInterleaved(guardian, system, files, metadata.offsets(), metadata.nCols(),
                chunkSize, orientation);
        long ingestionFinished = System.nanoTime();
        long ingestionNanos = ingestionFinished - ingestionStarted;
        System.out.printf("[Loader] Ingestion done: %,d rows " + "in %.3fs%n", ingestion.totalRows(),
                ingestionNanos / 1_000_000_000.0);

        CompletionStage<BDReply> doneFuture = AskPattern.ask(guardian, replyTo -> new BDCommand.FinishDiscovery(
                ingestion.finalRound(), ingestion.finalBatchByTable(), replyTo), Duration.ofMinutes(30),
                system.scheduler());
        doneFuture.toCompletableFuture().get();
        long finalizationFinished = System.nanoTime();
        long finalizationNanos = finalizationFinished - ingestionFinished;

        System.out.println("[Loader] Waiting for discovery result...");
        CompletionStage<ActorRef<RCCommand>> rcFuture = AskPattern.ask(guardian, BDCommand.GetResultCollector::new,
                Duration.ofSeconds(5), system.scheduler());

        ActorRef<RCCommand> rcRef = rcFuture.toCompletableFuture().get();

        // IndReport report = reportFuture.toCompletableFuture().get();
        IndReport report = AskPattern.ask(rcRef, RCCommand.GetReport::new, Duration.ofSeconds(30), system.scheduler())
                .toCompletableFuture().get();

        printReport(report, outputFile);
        long pipelineFinished = System.nanoTime();
        long pipelineNanos = pipelineFinished - pipelineStarted;
        Path metricsFile = pipelineMetricsWriter.write(ingestion, ingestionNanos, finalizationNanos, pipelineNanos,
                report.confirmedUnary().size());

        guardian.tell(new BDCommand.Shutdown());
    }

    private static IngestionResult ingestAllInterleaved(ActorRef<BDCommand> guardian, ActorSystem<?> system,
            List<String> files, List<Integer> offsets, List<Integer> nCols, int chunkSize, DataOrientation orientation)
            throws Exception {
        int n = files.size();
        CSVParser[] parsers = new CSVParser[n];
        @SuppressWarnings("unchecked")
        Iterator<CSVRecord>[] iterators = new Iterator[n];
        boolean[] tblFlags = new boolean[n];
        boolean[] active = new boolean[n];
        long[] rowCounts = new long[n];
        int[] nextRowId = new int[n];
        int[] individualBatchIds = new int[n];
        Map<Integer, Integer> latestBatchByTable = new HashMap<>();
        int creditWindow = UserConfig.DL_BD_CREDIT_WINDOW;
        int preparedQueueCapacity = Math.max(2, creditWindow);
        // Ensures the delete is not processed before the insert opertion.
        boolean enforceTblOrdering = UserConfig.INGESTION_MODE == IngestionMode.INSERT_WITH_DELETE;

        AsyncBatchDispatcher dispatcher = new AsyncBatchDispatcher(guardian, system, creditWindow,
                preparedQueueCapacity, enforceTblOrdering);
        AtomicInteger nextEpoch = new AtomicInteger();

        long totalRows = 0;
        long totalBatches = 0L;
        long totalCells = 0L;
        int round = 0;
        int[] batchSize = new int[n]; // no. of rows dynamically adjusted
        long totalDeletedRows = 0L;
        long totalReinsertedRows = 0L;

        @SuppressWarnings("unchecked")
        Deque<List<String[]>>[] deletionByTable = new Deque[n];

        for (int tableId = 0; tableId < n; tableId++) {
            deletionByTable[tableId] = new ArrayDeque<>(2);
        }
        try {
            System.out.println("[Loader] Opening files...");
            for (int i = 0; i < n; i++) {
                if (nCols.get(i) == 0)
                    continue;
                // batchSize[i] = Math.max(1, chunkSize / nCols.get(i));
                batchSize[i] = UserConfig.BATCH_SIZE;
                CSVParser parser = openCSVParser(files.get(i), UserConfig.separator.charAt(0),
                        UserConfig.inputFileHasHeader);
                parsers[i] = parser;
                iterators[i] = parser.iterator();
                tblFlags[i] = isTbl(files.get(i));
                active[i] = true;
            }

            System.out.println("[Loader] Interleaved round-robin ingestion started...");
            boolean anyActive = true;
            long addedRows = 0;
            int numberOfColsSent = 0;
            while (anyActive) {
                anyActive = false;
                round++;
                long roundStartedNanos = System.nanoTime();
                addedRows = totalRows;
                numberOfColsSent = 0;
                for (int i = 0; i < n; i++) {
                    if (!active[i])
                        continue;
                    int rowsRead = 0;
                    OrientetationBatchBuilder[] builders = new OrientetationBatchBuilder[UserConfig.VALUE_OWNER_BUCKETS];
                    boolean deleteMode = UserConfig.INGESTION_MODE == IngestionMode.INSERT_WITH_DELETE;
                    int deletionCapacityPerBatch = deleteMode
                            ? (int) Math.floor(batchSize[i] * UserConfig.DELETE_PERCENT / 100)
                            : 0;
                    String[][] deletionStore = deletionCapacityPerBatch > 0 ? new String[deletionCapacityPerBatch][]
                            : null;
                    Random deletionRandom = null;
                    if (deletionStore != null) {
                        // Obtain random deletes for each different table and batches.
                        long batchSeed = UserConfig.DEFAULT_DELETE_SEED;
                        batchSeed = 31L * batchSeed + i;
                        batchSeed = 31L * batchSeed + individualBatchIds[i];
                        deletionRandom = new Random(batchSeed);

                    }
                    int batchStartRowId = nextRowId[i];
                    int expectedColumns = nCols.get(i);
                    int globalColumnOffset = offsets.get(i);
                    while (rowsRead < batchSize[i]) {
                        if (!iterators[i].hasNext()) {
                            parsers[i].close();
                            active[i] = false;
                            break;
                        }
                        CSVRecord record = iterators[i].next();
                        String[] row = recordToArray(record, tblFlags[i]);

                        if (row.length != expectedColumns)
                            throw new IllegalArgumentException();

                        int rowId = batchStartRowId + rowsRead;
                        addRowToOwnerBuilders(row, expectedColumns, globalColumnOffset, rowId, 1, orientation,
                                builders);
                        if (deletionStore != null) {
                            // Added the rows to deletionStore randomly using Algorithm R.
                            if (rowsRead < deletionCapacityPerBatch) {
                                deletionStore[rowsRead] = row;
                            } else {
                                int j = deletionRandom.nextInt(rowsRead + 1);
                                if (j < deletionCapacityPerBatch) {
                                    deletionStore[j] = row;
                                }
                            }
                        }
                        rowsRead++;
                        rowCounts[i]++;
                        totalRows++;
                    }

                    numberOfColsSent += nCols.get(i);
                    if (rowsRead > 0) {
                        int batchId = individualBatchIds[i]++;

                        if (UserConfig.INGESTION_MODE == IngestionMode.INSERT_WITH_DELETE) {

                            Deque<List<String[]>> deletionQueue = deletionByTable[i];
                            if (deletionQueue.size() == 2) {
                                List<String[]> rowsToReinsert = deletionQueue.removeFirst();
                                addRowsToOwnerBuilders(rowsToReinsert, expectedColumns, globalColumnOffset, 1,
                                        orientation, builders);
                                totalReinsertedRows = Math.addExact(totalReinsertedRows, rowsToReinsert.size());
                            }

                            if (!deletionQueue.isEmpty()) {
                                List<String[]> rowsToDelete = deletionQueue.peekFirst();
                                addRowsToOwnerBuilders(rowsToDelete, expectedColumns, globalColumnOffset, -1,
                                        orientation, builders);
                                totalDeletedRows = Math.addExact(totalDeletedRows, rowsToDelete.size());
                            }

                            List<String[]> currentDeletionSample = (active[i] && deletionStore != null)
                                    ? List.copyOf(Arrays.asList(deletionStore))
                                    : List.of();

                            deletionQueue.addLast(currentDeletionSample);

                            if (deletionQueue.size() > 2)
                                throw new IllegalStateException("Deletion queue exceeded two samples");

                        }

                        Map<Integer, BatchBody> ownerBatches = finishOwnerBatches(builders);
                        PreparedBatch preparedBatch = new PreparedBatch(nextEpoch.incrementAndGet(), i, batchStartRowId,
                                rowsRead, ownerBatches, round, batchId, orientation);
                        dispatcher.submit(preparedBatch);
                        totalBatches = Math.incrementExact(totalBatches);
                        long batchCells = Math.multiplyExact((long) rowsRead, expectedColumns);
                        totalCells = Math.addExact(totalCells, batchCells);
                        latestBatchByTable.put(i, batchId);
                        nextRowId[i] += rowsRead;
                        anyActive = true;
                    }
                }
                System.out.printf(
                        "[Loader] Round %d complete: totalRows=%,d addedRows=%,d columnsVisited=%,d submittedBatches=%,d elapsedSeconds=%.3f%n",
                        round, totalRows, totalRows - addedRows, numberOfColsSent, totalBatches,
                        (System.nanoTime() - roundStartedNanos) / 1_000_000_000.0);
            }
            if (UserConfig.INGESTION_MODE == IngestionMode.INSERT_WITH_DELETE) {
                // Last restoration of the deleted rows is reinserted to bring dataset
                // consistency.
                // dispatcher.finishAndWait();
                // dispatcher.close();
                // dispatcher = new AsyncBatchDispatcher(guardian, system, creditWindow,
                // preparedQueueCapacity,
                // enforceTblOrdering);

                int restorationRound = Math.incrementExact(round);
                round = restorationRound;
                for (int tableId = 0; tableId < n; tableId++) {
                    Deque<List<String[]>> deletionQueue = deletionByTable[tableId];
                    if (deletionQueue.size() != 2) {
                        deletionQueue.clear();
                        continue;
                    }
                    List<String[]> rowsToRestore = deletionQueue.removeFirst();
                    deletionQueue.clear();
                    if (rowsToRestore.isEmpty())
                        continue;

                    OrientetationBatchBuilder[] builders = new OrientetationBatchBuilder[UserConfig.VALUE_OWNER_BUCKETS];
                    addRowsToOwnerBuilders(rowsToRestore, nCols.get(tableId), offsets.get(tableId), 1, orientation,
                            builders);
                    Map<Integer, BatchBody> ownerBatches = finishOwnerBatches(builders);
                    int restorationBatchId = individualBatchIds[tableId]++;
                    PreparedBatch restorationBatch = new PreparedBatch(nextEpoch.incrementAndGet(), tableId,
                            nextRowId[tableId], 0, ownerBatches, restorationRound,
                            restorationBatchId, orientation);
                    dispatcher.submit(restorationBatch);
                    totalBatches = Math.incrementExact(totalBatches);
                    totalReinsertedRows = Math.addExact(totalReinsertedRows, rowsToRestore.size());
                    latestBatchByTable.put(tableId, restorationBatchId);
                }
            }

            dispatcher.finishAndWait();
            if (UserConfig.INGESTION_MODE == IngestionMode.INSERT_WITH_DELETE) {
                System.out.printf("[Loader] Deletion Stat: %,d deletions, " + "%,d reinsertions%n",
                        totalDeletedRows, totalReinsertedRows);
                if (totalDeletedRows != totalReinsertedRows)
                    throw new IllegalStateException("Deletion did not restore all deleted rows");
            }
            int finalRound = round;
            System.out.println("[Loader] Per-file row counts:");
            for (int i = 0; i < n; i++) {
                if (nCols.get(i) == 0)
                    continue;
                System.out.printf("  %-25s %,d rows%n", Paths.get(files.get(i)).getFileName(), rowCounts[i]);
            }
            return new IngestionResult(totalRows, totalBatches, totalCells, finalRound,
                    new HashMap<>(latestBatchByTable));

        } finally {
            dispatcher.close();

            for (CSVParser parser : parsers) {
                if (parser != null) {
                    try {
                        parser.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }

    private static CSVParser openCSVParser(String file, char separator, boolean inputHasHeader) throws IOException {

        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder()
                .setDelimiter(separator)
                .setQuote('"')
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .setAllowMissingColumnNames(true);

        if (inputHasHeader) {
            builder.setHeader().setSkipHeaderRecord(true);
        }
        return new CSVParser(Files.newBufferedReader(Paths.get(file), StandardCharsets.UTF_8), builder.build());
    }

    private static String[] recordToArray(CSVRecord record, boolean tbl) {
        // One parsed row into array
        int size = record.size();
        // For 1|Alice|Berlin|, remove last |
        if (tbl && size > 0 && record.get(size - 1).isEmpty())
            size--;

        String[] values = new String[size];
        for (int i = 0; i < size; i++) {
            values[i] = normalize(record.get(i));
        }
        return values;
    }

    private static void addRowsToOwnerBuilders(List<String[]> rows, int expectedColumns, int globalColumnOffset,
            int delta, DataOrientation orientation, OrientetationBatchBuilder[] builders) {

        for (String[] row : rows) {
            addRowToOwnerBuilders(row, expectedColumns, globalColumnOffset, 0, delta, orientation, builders);
        }
    }

    private static void addRowToOwnerBuilders(String[] row, int expectedColumns, int globalColumnOffset, int rowId,
            int delta, DataOrientation orientation, OrientetationBatchBuilder[] builders) {

        if (row.length != expectedColumns)
            throw new IllegalArgumentException("Record has different number of columns: expected ");

        if (delta != 1 && delta != -1)
            throw new IllegalArgumentException("delta must be 1 or -1: " + delta);

        for (int localColumn = 0; localColumn < expectedColumns; localColumn++) {
            String value = row[localColumn];
            if (value.isEmpty())
                continue;
            int globalColumn = globalColumnOffset + localColumn;
            int ownerId = Math.floorMod(value.hashCode(), UserConfig.VALUE_OWNER_BUCKETS);
            OrientetationBatchBuilder builder = builders[ownerId];

            if (builder == null) {
                builder = newBuilder(orientation);
                builders[ownerId] = builder;
            }

            builder.add(globalColumn, value, rowId, delta);
        }
    }

    private static Map<Integer, BatchBody> finishOwnerBatches(OrientetationBatchBuilder[] builders) {
        Map<Integer, BatchBody> ownerBatches = new HashMap<>();
        for (int ownerId = 0; ownerId < builders.length; ownerId++) {
            OrientetationBatchBuilder builder = builders[ownerId];
            if (builder != null)
                ownerBatches.put(ownerId, builder.build());
        }
        return ownerBatches;
    }

    public static CompletionStage<BDReply> sendTableBatch(ActorRef<BDCommand> guardian, ActorSystem<?> system,
            int epoch, int tableId, int startRowId, Map<Integer, BatchBody> ownerBatches, int round,
            int individualBatchId, DataOrientation orientation) {

        InputBatchDetails details = new InputBatchDetails(tableId, startRowId, individualBatchId, epoch, round, -1);
        ClusterSharding sharding = ClusterSharding.get(system);

        EntityRef<DirectBatchAggregatorActor.Command> aggregator = sharding.entityRefFor(
                DirectBatchAggregatorActor.TYPE_KEY, "direct-batch-" + epoch + "-" + tableId + "-" + individualBatchId);

        Duration timeout = Duration.ofSeconds(UserConfig.BATCH_ACK_TIMEOUT_SECONDS);

        return aggregator.<DirectBatchAggregatorActor.BatchHandle>ask(
                replyTo -> new DirectBatchAggregatorActor.PrepareBatch(details, ownerBatches.size(), replyTo), timeout)
                .thenCompose(handle -> {
                    ActorRef<DirectBatchAggregatorActor.Command> completionRef = handle.aggregator();
                    CompletionStage<BDReply> completion = AskPattern.ask(
                            completionRef, DirectBatchAggregatorActor.AwaitCompletion::new,
                            timeout, system.scheduler());
                    ownerBatches.forEach((ownerId, body) -> sharding
                            .entityRefFor(ValueOwnerActor.TYPE_KEY, ValueOwnerActor.entityId(ownerId))
                            .tell(new StoreBatch(epoch, tableId, individualBatchId, round,
                                    ownerId, orientation, ValueOwnerBatchCodec.encode(orientation, body),
                                    completionRef)));
                    return completion;
                });
    }

    private static OrientetationBatchBuilder newBuilder(DataOrientation orientation) {
        return switch (orientation) {
            case VALUE_MAJOR -> new ValueMajorBatchBuilder();
            case COLUMN_MAJOR -> new ColumnMajorBatchBuilder();
        };
    }

    private static List<String> listInputFiles(Path dir) throws IOException {
        List<String> files = new ArrayList<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(dir)) {
            for (Path path : paths) {
                String file = path.toString().toLowerCase(Locale.ROOT); // Language Neutral lower case

                if (file.endsWith(".csv") || file.endsWith(".tbl") || file.endsWith(".tsv"))
                    files.add(path.toString());
            }
        }
        Collections.sort(files);
        return files;
    }

    private static boolean isTbl(String file) {
        return file.toLowerCase(Locale.ROOT).endsWith(".tbl");
    }

    private static String normalize(String s) {
        if (s == null)
            return "";
        s = s.strip(); // Removes leading/trailing space.
        if (s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
            s = s.substring(1, s.length() - 1).strip();
        }
        return s;
    }

    private static String tableName(String file) {
        return PAT_CSV_EXT.matcher(Paths.get(file).getFileName().toString()).replaceFirst("");
    }

    private static void printReport(IndReport report, String outputFile) throws IOException {

        Map<Integer, String> names = report.colNames();

        StringBuilder sb = new StringBuilder();

        sb.append("\n").append("=".repeat(72)).append("\n");
        sb.append("  IND DISCOVERY REPORT\n");
        sb.append("  Snapshot epoch : ").append(report.snapshotEpoch()).append("\n");
        sb.append("=".repeat(72)).append("\n\n");

        List<SharedModel.UnaryPair> unary = report.confirmedUnary();

        sb.append("UNARY INDs  (").append(unary.size()).append(" confirmed)\n");
        sb.append("-".repeat(72)).append("\n");

        if (unary.isEmpty()) {
            sb.append("  (none)\n");
        } else {
            unary.stream()
                    .sorted(
                            Comparator.comparingInt(SharedModel.UnaryPair::lhsCol)
                                    .thenComparingInt(SharedModel.UnaryPair::rhsCol))
                    .forEach(p -> sb
                            .append(String.format("IND(%s, %s)%n", name(names, p.lhsCol()), name(names, p.rhsCol()))));
        }

        List<SharedModel.NaryPair> nary = report.confirmedNary();

        sb.append("\nN-ARY INDs  (").append(nary.size()).append(" confirmed)\n");
        sb.append("-".repeat(72)).append("\n");

        if (nary.isEmpty()) {
            sb.append("  (none)\n");
        } else {
            nary.stream()
                    .sorted(
                            Comparator.comparingInt(SharedModel.NaryPair::arity)
                                    .thenComparing(p -> p.lhsCols().toString()))
                    .forEach(p -> sb.append(String.format("  arity=%-2d  %-45s  ⊆  %s%n", p.arity(),
                            tuple(names, p.lhsCols()), tuple(names, p.rhsCols()))));
        }

        sb.append("\n").append("=".repeat(72)).append("\n");
        sb.append("  TOTAL: ").append(unary.size()).append(" unary + ").append(nary.size()).append(" n-ary = ")
                .append(unary.size() + nary.size()).append(" INDs confirmed\n");
        sb.append("=".repeat(72)).append("\n");

        System.out.printf("[Loader] Discovery complete: %d unary, %d n-ary INDs%n",
                report.confirmedUnary().size(), report.confirmedNary().size());

        if (outputFile != null && !outputFile.isBlank()) {
            try (PrintWriter pw = new PrintWriter(outputFile)) {
                pw.print(sb);
            }

            System.out.println("[Loader] Report written to: " + outputFile);
        }
    }

    private static String name(Map<Integer, String> names, int colId) {
        String n = names.get(colId);
        if (n == null || n.isBlank()) {
            return "col[" + colId + "]";
        }
        int dotC = n.lastIndexOf(".c");
        if (dotC >= 0 && dotC + 2 < n.length()) {
            String table = n.substring(0, dotC);
            String idx = n.substring(dotC + 2);
            return table + "[" + idx + "]";
        }

        return n;
    }

    private static String tuple(Map<Integer, String> names, List<Integer> cols) {
        List<String> parts = new ArrayList<>();
        for (int c : cols) {
            parts.add(name(names, c));
        }
        return "(" + String.join(", ", parts) + ")";
    }
}
