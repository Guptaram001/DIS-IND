package disIND.streamBasedShardedDispatcher.dataset;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;

import disIND.streamBasedShardedDispatcher.actors.INDGuardian;
import disIND.streamBasedShardedDispatcher.model.SharedModel;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.utility.InferDataAttributes;
import disIND.streamBasedShardedDispatcher.utility.UserConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class DataLoader {
    private static final Pattern PAT_CSV_EXT = Pattern.compile("\\.(csv|tbl)$");

    private DataLoader() {}

    //Discover the basic metadata of the dataset
    public static INDGuardian.Config discoverConfig(String csvDir) throws IOException {
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

        for (int tableId = 0; tableId < files.size(); tableId++) {
            String file = files.get(tableId);
            boolean tbl = isTbl(file);
            String table = tableName(file);
            tableNames.add(table);
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String firstLine = br.readLine();
                if (firstLine == null)
                    continue;
                String delim = delimiterFor(file, firstLine);
                String[] cols = splitRow(firstLine, delim, tbl);
                nCols.add(cols.length);
                offsets.add(totalCols);
                System.out.printf("  %s  %d cols  offset=%d  delim='%s'%n", table, cols.length, totalCols, delim);
                List<String[]> sampleRows = new ArrayList<>();

                for (int i = 0; i < 1000; i++) {
                    String row = br.readLine();
                    if (row == null)
                        break;
                    sampleRows.add(splitRow(row, delim, tbl));
                }

                for (int localCol = 0; localCol < cols.length; localCol++) {
                    int globalCol = totalCols + localCol;
                    String columnName;
                    if (tbl)
                        columnName = "c" + localCol;
                    else
                        columnName = InferDataAttributes.clean(cols[localCol]);

                    ColType type = InferDataAttributes.inferColType(table + "." + columnName, localCol, sampleRows);
                    columns.put(globalCol, new ColumnInfo(globalCol, tableId, table, localCol, columnName, type));
                }
                totalCols += cols.length;
            }
        }

        DatasetMetadata metadata = new DatasetMetadata(totalCols, offsets, nCols, tableNames, columns);

        return INDGuardian.Config.withAll(metadata);
    }

    public static void run(ActorSystem<BDCommand> system, DatasetMetadata metadata,String csvDir, int batchSize, int timeoutSec,
            String outputFile, ActorRef<SharedModel.BDCommand> bdRef) throws Exception {

        Path dir = Paths.get(csvDir);
        List<String> files = listInputFiles(dir);

        if (files.isEmpty()) {
            System.err.println("[Loader] No .csv or .tbl files in: " + csvDir);
            return;
        }

        long startMs = System.currentTimeMillis();
        IngestionResult ingestion = ingestAllInterleaved(system, files, metadata.offsets(), metadata.nCols(), metadata.totalCols(),
                batchSize, bdRef);

        System.out.printf("[Loader] Ingestion done: %,d rows in %.1fs%n", ingestion.totalRows(), (System.currentTimeMillis() - startMs) / 1000.0);
        CompletionStage<BDReply> doneFuture = AskPattern.ask(bdRef, replyTo -> new BDCommand.FinishDiscovery(
                                ingestion.finalRound(), ingestion.finalBatchByTable(),replyTo), Duration.ofMinutes(30),
                system.scheduler());
        doneFuture.toCompletableFuture().get();

        System.out.println("[Loader] Waiting for discovery result...");
        CompletionStage<ActorRef<RCCommand>> rcFuture = AskPattern.ask(system, BDCommand.GetResultCollector::new,
                Duration.ofSeconds(5), system.scheduler());

        ActorRef<RCCommand> rcRef = rcFuture.toCompletableFuture().get();

        //IndReport report = reportFuture.toCompletableFuture().get();
        IndReport report = AskPattern.ask(rcRef, RCCommand.GetReport::new, Duration.ofSeconds(30), system.scheduler())
                .toCompletableFuture().get();

        printReport(report, outputFile);

        system.tell(new BDCommand.Shutdown());
    }

    private static IngestionResult ingestAllInterleaved(ActorSystem<BDCommand> system, List<String> files, List<Integer> offsets,
                                                                List<Integer> nCols, int totalCols, int batchSize, ActorRef<BDCommand> bdRef) throws Exception {
        int n = files.size();
        BufferedReader[] readers = new BufferedReader[n];
        String[] delims = new String[n];
        boolean[] tblFlags = new boolean[n];
        boolean[] active = new boolean[n];
        long[] rowCounts = new long[n];
        long[] nextRowId = new long[n];
        int[] individualBatchIds = new int[n];
        Map<Integer, Integer> latestBatchByTable = new HashMap<>();
        Deque<CompletionStage<BDReply>> inFlight = new ArrayDeque<>();
        long totalRows = 0;
        int round = 0;

        System.out.println("[Loader] Opening files...");
        for (int i = 0; i < n; i++) {
            if (nCols.get(i) == 0)
                continue;
            boolean tbl = isTbl(files.get(i));
            tblFlags[i] = tbl;
            BufferedReader br = new BufferedReader(new FileReader(files.get(i)));
            String firstLine = br.readLine();
            if (firstLine == null) {
                br.close();
                continue;
            }
            String delim = delimiterFor(files.get(i), firstLine);
            delims[i] = delim;
            readers[i] = br;
            active[i] = true;

            /*
             * TBL:
             * first line is DATA
             *
             * CSV:
             * first line is HEADER
             */
            if (tbl) {
                int batchId = individualBatchIds[i]++;
                List<String[]> firstBatch = new ArrayList<>(1);
                firstBatch.add(splitRow(firstLine, delim, true));
                inFlight.addLast(sendTableBatch(system, bdRef, i, nextRowId[i], firstBatch,round,batchId));
                waitForCredit(inFlight);
                nextRowId[i]++;
                rowCounts[i]++;
                totalRows++;
            }
            System.out.printf("  opened %-25s offset=%d cols=%d%n", Paths.get(files.get(i)).getFileName(),
                    offsets.get(i), nCols.get(i));
        }

        System.out.println("[Loader] Interleaved round-robin ingestion started...");
        boolean anyActive = true;
        while (anyActive) {
            anyActive = false;
            round++;
            for (int i = 0; i < n; i++) {
                if (!active[i])
                    continue;
                List<String[]> batchRows = new ArrayList<>(batchSize);
                int rowsRead = 0;
                while (rowsRead < batchSize) {
                    String line = readers[i].readLine();
                    if (line == null) {
                        readers[i].close();
                        active[i] = false;
                        break;
                    }
                    if (line.isBlank())
                        continue;
                    batchRows.add(splitRow(line, delims[i], tblFlags[i]));
                    rowsRead++;
                    rowCounts[i]++;
                    totalRows++;
                }

                if (!batchRows.isEmpty()) {
                    int batchId = individualBatchIds[i]++;
                    inFlight.addLast(sendTableBatch(system, bdRef, i, nextRowId[i], batchRows,round,batchId));
                    waitForCredit(inFlight);
                    latestBatchByTable.put(i, batchId);
                    nextRowId[i] += batchRows.size();
                    anyActive = true;
                }
            }
            System.out.printf("[Loader] Round %d: %d rows ingested%n", round, totalRows);
            if(round % UserConfig.CHECKPOINT_INTERVAL == 0){
                waitForAll(inFlight);
                bdRef.tell(new BDCommand.CheckPoint(round,new HashMap<>(latestBatchByTable)));
            }
        }
        waitForAll(inFlight);
        int finalRound = round;
        System.out.println("[Loader] Per-file row counts:");
        for (int i = 0; i < n; i++) {
            if (nCols.get(i) == 0)
                continue;
            System.out.printf("  %-25s %,d rows%n", Paths.get(files.get(i)).getFileName(), rowCounts[i]);
        }
        return new IngestionResult(totalRows, finalRound, new HashMap<>(latestBatchByTable));
    }

    private static CompletionStage<BDReply> sendTableBatch(ActorSystem<BDCommand> system, ActorRef<BDCommand> bdRef, int tableId, long startRowId,
        List<String[]> rows,int round,int individualBatchId) throws Exception {

            System.out.println("[Loader] Sending table batch to "+bdRef+" "+bdRef.path().name()+" tableId="+tableId+" " +
                    "startRowId="+startRowId+" rows="+rows.size());
            return AskPattern.ask(bdRef, (ActorRef<BDReply> replyTo) -> new BDCommand.SendTableBatch(rows,
                                    new InputBatchDetails(tableId,startRowId,individualBatchId,-1,round, -1),
                                    replyTo), Duration.ofSeconds(UserConfig.BATCH_ACK_TIMEOUT_SECONDS),
                            system.scheduler());

        }

    private static void waitForCredit(Deque<CompletionStage<BDReply>> inFlight) throws Exception {
        while (inFlight.size() >= UserConfig.DL_BD_CREDIT_WINDOW) {
            inFlight.removeFirst().toCompletableFuture().get();
        }
    }

    private static void waitForAll(Deque<CompletionStage<BDReply>> inFlight) throws Exception {
        while (!inFlight.isEmpty()) {
            inFlight.removeFirst().toCompletableFuture().get();
        }
    }

    private static List<String> listInputFiles(Path dir) throws IOException {
        try (Stream<Path> paths = Files.list(dir)) {
            return paths.filter(p -> {
                        String s = p.toString().toLowerCase(Locale.ROOT);
                        return s.endsWith(".csv") || s.endsWith(".tbl");
                    })
                    .map(Path::toString)
                    .sorted()
                    .toList();
        }
    }

    private static boolean isTbl(String file) {
        return file.toLowerCase(Locale.ROOT).endsWith(".tbl");
    }

    private static String delimiterFor(String file, String sampleLine) {
        if (isTbl(file))
            return "\\|";
        return sampleLine.contains(";") ? ";" : ",";
    }

    private static String[] splitRow(String line, String delim, boolean tbl) {
        String[] vals = line.split(delim, -1);
        if (tbl && vals.length > 0 && vals[vals.length - 1].isEmpty())
            vals = Arrays.copyOf(vals, vals.length - 1);

        for (int i = 0; i < vals.length; i++)
            vals[i] = normalize(vals[i]);
        return vals;
    }

    private static String normalize(String s) {
        if (s == null)
            return "";
        s = s.strip();
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
                    .forEach(p -> sb.append(String.format(
                            "IND(%s, %s)%n",
                            name(names, p.lhsCol()),
                            name(names, p.rhsCol())
                    )));
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
                    .forEach(p -> sb.append(String.format(
                            "  arity=%-2d  %-45s  ⊆  %s%n",
                            p.arity(),
                            tuple(names, p.lhsCols()),
                            tuple(names, p.rhsCols())
                    )));
        }

        sb.append("\n").append("=".repeat(72)).append("\n");
        sb.append("  TOTAL: ")
                .append(unary.size())
                .append(" unary + ")
                .append(nary.size())
                .append(" n-ary = ")
                .append(unary.size() + nary.size())
                .append(" INDs confirmed\n");
        sb.append("=".repeat(72)).append("\n");

        System.out.print(sb);

        if (outputFile != null && !outputFile.isBlank()) {
            try (PrintWriter pw = new PrintWriter(outputFile)) {
                pw.print(sb);
            }

            System.out.println("[Loader] Report written to: " + outputFile);
        }
    }

    // private static String name(Map<Integer, String> names, int col) {
    //     return names.getOrDefault(col, "col" + col);
    // }

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
