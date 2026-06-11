package disIND.streamBasedShardedDispatcher.dataset;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;

import disIND.streamBasedShardedDispatcher.actors.INDGuardian;
import disIND.streamBasedShardedDispatcher.model.SharedModel;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.utility.InferDataAttributes;

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

import static disIND.streamBasedShardedDispatcher.utility.InferDataAttributes.*;

public final class DataLoader {
    private static DatasetMetadata metadata;

    private static final Pattern PAT_CSV_EXT = Pattern.compile("\\.(csv|tbl)$");

    private DataLoader() {}

    //Discover the basic metadata of the dataset
    public static INDGuardian.Config discoverConfig(String csvDir) throws IOException {

        Path dir = Paths.get(csvDir);
        List<String> files = listInputFiles(dir);

        if (files.isEmpty()) {
            throw new IllegalArgumentException("[Loader] No .csv or .tbl files in: " + csvDir);
        }
        Map<Integer, String> colNames = new LinkedHashMap<>();
        Map<Integer, ColType> colTypes = new LinkedHashMap<>();
        List<Integer> ncols = new ArrayList<>();
        List<Integer> offsets = new ArrayList<>();
        int totalCols = 0;
        System.out.println("[Loader] Discovering files and columns:");

        for (String file : files) {
            boolean tbl = isTbl(file);
            String table = tableName(file);

            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String firstLine = br.readLine();

                String delim = delimiterFor(file, firstLine);
                String[] cols = splitRow(firstLine, delim, tbl);
                ncols.add(cols.length);
                offsets.add(totalCols);
                System.out.printf("  %s  %d cols  offset=%d  delim='%s'%n", table, cols.length, totalCols, delim);

                List<String[]> sampleRows = new ArrayList<>();
                for (int i = 0; i < 1000; i++) {
                    String row = br.readLine();
                    if (row == null)
                        break;
                    sampleRows.add(splitRow(row, delim, tbl));
                }

                if (firstLine == null) {
                    System.out.printf("  %-25s  empty%n", table);
                    continue;
                }

                for (int local = 0; local < cols.length; local++) {
                    int global = totalCols + local;
                    String name;
                    if (tbl)
                        name = table + ".c" + local;
                    else
                        name = table + "." + InferDataAttributes.clean(cols[local]);

                    colNames.put(global, name);
                    colTypes.put(global, InferDataAttributes.inferColType(name, local, sampleRows));
                }
                totalCols += cols.length;
            }
        }
        metadata=new DatasetMetadata(totalCols,offsets,ncols,colNames,colTypes);
        return INDGuardian.Config.withAll(metadata);
    }

    public static void run(ActorSystem<BDCommand> system, String csvDir, int batchSize, int timeoutSec,
            String outputFile) throws Exception {

        Path dir = Paths.get(csvDir);
        List<String> files = listInputFiles(dir);

        if (files.isEmpty()) {
            System.err.println("[Loader] No .csv or .tbl files in: " + csvDir);
            return;
        }

        long startMs = System.currentTimeMillis();
        long totalRows = ingestAllInterleaved(system, files, metadata.offsets(), metadata.nCols(), metadata.totalCols(),
                batchSize);

        System.out.printf("[Loader] Ingestion done: %,d rows in %.1fs%n", totalRows, (System.currentTimeMillis() - startMs) / 1000.0);
        system.tell(new BDCommand.IngestionDone());
        System.out.println("[Loader] Waiting for discovery result...");
        CompletionStage<ActorRef<RCCommand>> rcFuture = AskPattern.ask(system, BDCommand.GetResultCollector::new,
                Duration.ofSeconds(10), system.scheduler());

        ActorRef<RCCommand> rcRef = rcFuture.toCompletableFuture().get();

        CompletionStage<IndReport> reportFuture = AskPattern.ask(rcRef,
                        RCCommand.GetReport::new,
                        Duration.ofSeconds(timeoutSec),
                        system.scheduler());

        IndReport report = reportFuture.toCompletableFuture().get();

        printReport(report, outputFile);

        system.tell(new BDCommand.Shutdown());
    }

    private static long ingestAllInterleaved(ActorSystem<BDCommand> system, List<String> files, List<Integer> offsets,
            List<Integer> nCols, int totalCols, int batchSize) throws Exception {

        int n = files.size();
        BufferedReader[] readers = new BufferedReader[n];
        String[] delims = new String[n];
        boolean[] tblFlags = new boolean[n];
        String[][] sentinels = new String[n][];
        long[] rowCounts = new long[n];
        boolean[] active = new boolean[n];

        String[] cells = new String[batchSize * totalCols];

        int rowsInBatch = 0;
        long totalRows = 0;

        System.out.println("[Loader] Opening files for interleaved ingestion...");

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
            String[] sentinel = new String[totalCols];
            Arrays.fill(sentinel, "\u0000");
            sentinels[i] = sentinel;
            if (tbl) {
                String[] vals = splitRow(firstLine, delim, true);
                addRowToBuffer(cells, rowsInBatch, totalCols, sentinel, offsets.get(i), nCols.get(i), vals);
                rowsInBatch++;
                rowCounts[i]++;
                totalRows++;
                if (rowsInBatch >= batchSize) {
                    System.out.println("[Loader Header] Batch Details for Cells:");
                    for(String st:cells)
                        System.out.print(st);
                    sendBatch(system, cells, rowsInBatch, totalCols);
                    rowsInBatch = 0;
                }
            }
            System.out.printf("  opened %-25s offset=%d cols=%d%n", Paths.get(files.get(i)).getFileName(),
                    offsets.get(i), nCols.get(i));
        }

        System.out.println("[Loader] Interleaved round-robin ingestion started...");
        boolean anyActive = true;
        while (anyActive) {
            anyActive = false;
            for (int i = 0; i < n; i++) {
                if (!active[i])
                    continue;
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
                    anyActive = true;
                    rowsRead++;
                    String[] vals = splitRow(line, delims[i], tblFlags[i]);
                    addRowToBuffer(cells, rowsInBatch, totalCols, sentinels[i], offsets.get(i), nCols.get(i), vals);
                    rowsInBatch++;
                    rowCounts[i]++;
                    totalRows++;
                    if (rowsInBatch >= batchSize) {
                        System.out.println("[Loader Body] Batch Details for Cells After Full size:");
                        for(String st:cells)
                            System.out.println("CElls Values: "+st);
                        sendBatch(system, cells, rowsInBatch, totalCols);
                        sendBatch(system, cells, rowsInBatch, totalCols);
                        rowsInBatch = 0;
                    }
                }
                if (rowsRead > 0)
                    anyActive = true;
            }
        }

        if (rowsInBatch > 0)
            sendBatch(system, cells, rowsInBatch, totalCols);
        System.out.println("[Loader] Per-file row counts:");
        for (int i = 0; i < n; i++) {
            if (nCols.get(i) == 0)
                continue;
            System.out.printf("  %-25s %,d rows%n", Paths.get(files.get(i)).getFileName(), rowCounts[i]);
        }
        return totalRows;
    }

    private static void addRowToBuffer(String[] cells, int rowIdx, int totalCols, String[] sentinel,
            int offset, int fileCols, String[] vals) {

        int base = rowIdx * totalCols;
        System.arraycopy(sentinel, 0, cells, base, totalCols);
        int limit = Math.min(vals.length, fileCols);
        for (int c = 0; c < limit; c++) {
            cells[base + offset + c] = clean(vals[c]);
        }
    }

    private static void sendBatch(ActorSystem<BDCommand> system, String[] cells, int numRows, int totalCols)
            throws Exception {

        //Need to look into retry logic if failed or more robust error handling.
        String[] copy = Arrays.copyOf(cells, numRows * totalCols);
//        AskPattern.ask(system,
//                (ActorRef<BDCommand> replyTo)  -> new BDCommand.IngestBatch(copy, numRows, totalCols, replyTo),
//                Duration.ofSeconds(30),
//                system.scheduler()
//        ).toCompletableFuture().get();

        ActorRef<BDCommand> bdRef = AskPattern.ask(system,
                                BDCommand.GetBatchDispatcher::new,
                                Duration.ofSeconds(10),
                                system.scheduler()
                        ).toCompletableFuture().get();
        AskPattern.ask(bdRef,
                ( ActorRef<BDReply> replyTo ) -> new BDCommand.IngestBatch(copy, numRows, totalCols, replyTo),
                Duration.ofSeconds(30),
                system.scheduler()
        ).toCompletableFuture().get();
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
        if (tbl && vals.length > 0 && vals[vals.length - 1].isEmpty()) {
            vals = Arrays.copyOf(vals, vals.length - 1);
        }
        return vals;
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
                            "  %-38s  ⊆  %s%n",
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

    private static String name(Map<Integer, String> names, int col) {
        return names.getOrDefault(col, "col" + col);
    }

    private static String tuple(Map<Integer, String> names, List<Integer> cols) {
        List<String> parts = new ArrayList<>();
        for (int c : cols) {
            parts.add(name(names, c));
        }
        return "(" + String.join(", ", parts) + ")";
    }
}