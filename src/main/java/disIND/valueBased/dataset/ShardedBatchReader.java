package disIND.valueBased.dataset;

import disIND.valueBased.utility.UserConfig;
import org.apache.commons.csv.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Single ordered consumer, parallel parsers. A lease holds its slot through dispatcher submission. */
final class ShardedBatchReader implements AutoCloseable {
    @FunctionalInterface interface PartReader { List<String[]> read(ShardManifest.Part part) throws Exception; }
    private record Task(ShardManifest.Part part, FutureTask<List<String[]>> result) { }
    private final ShardManifest manifest;
    private final int capacity;
    private final ExecutorService readers;
    private final ScheduledExecutorService sampler;
    private final ArrayDeque<Task> pending = new ArrayDeque<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicInteger active = new AtomicInteger(), occupied = new AtomicInteger();
    private final Set<CSVParser> openParsers = ConcurrentHashMap.newKeySet();
    private final PrintWriter batches, resources;
    private final PartReader partReader;
    private int nextRound, nextTable;
    private final int rounds;
    private volatile boolean closed;
    private boolean leased;
    private final long started = System.nanoTime();

    ShardedBatchReader(ShardManifest manifest, int threads, int capacity) throws IOException {
        this(manifest, threads, capacity, Path.of(System.getProperty("dis.ind.diagnostics-dir",
                System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR", "diagnostics"))), null);
    }

    ShardedBatchReader(ShardManifest manifest, int threads, int capacity, Path diagnostics, PartReader partReader)
            throws IOException {
        if (threads < 1 || capacity < 1) throw new IllegalArgumentException("Reader threads/capacity must be positive");
        this.manifest = manifest;
        this.capacity = capacity;
        this.partReader = partReader == null ? this::readPart : partReader;
        rounds = manifest.tables.stream().mapToInt(List::size).max().orElse(0);
        Files.createDirectories(diagnostics);
        batches = new PrintWriter(Files.newBufferedWriter(diagnostics.resolve("shard-reader-batches.tsv")));
        try {
            resources = new PrintWriter(Files.newBufferedWriter(diagnostics.resolve("shard-reader-resources.tsv")));
        } catch (IOException e) { batches.close(); throw e; }
        batches.println("event\ttable_id\tbatch_id\trows\tread_wall_seconds\tread_thread_cpu_seconds\tordered_wait_seconds\thead_of_line_wait_seconds");
        resources.println("elapsed_seconds\treader_threads\tcapacity\toccupied_slots\tactive_readers\tqueued_reads\tcompleted_waiting\thead_blocked_by_earlier_read");
        readers = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "dis-ind-shard-reader"); t.setDaemon(true); return t;
        });
        sampler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dis-ind-shard-metrics"); t.setDaemon(true); return t;
        });
        sampler.scheduleAtFixedRate(() -> sample(threads), 0, 1, TimeUnit.SECONDS);
        System.out.printf("[Loader] Sharded parsing: threads=%d sharedCapacity=%d tables=%d%n", threads, capacity, manifest.tables.size());
        fill();
    }

    // Called only by the scheduler/consumer, in original round-robin order. Reserve by
    // adding the task before execution; later completions cannot steal an earlier slot.
    private synchronized void fill() {
        while (!closed && failure.get() == null && occupied.get() < capacity && nextRound < rounds) {
            int table = nextTable++, batch = nextRound;
            if (nextTable == manifest.tables.size()) { nextTable = 0; nextRound++; }
            if (batch >= manifest.tables.get(table).size()) continue;
            var part = manifest.tables.get(table).get(batch);
            FutureTask<List<String[]>> future = new FutureTask<>(() -> {
                active.incrementAndGet();
                long start = System.nanoTime(), cpu = cpuTime();
                try {
                    var rows = partReader.read(part);
                    long cpuEnd = cpuTime();
                    synchronized (batches) {
                        batches.printf(Locale.ROOT, "read\t%d\t%d\t%d\t%.6f\t%.6f\t\t%n", table, batch, rows.size(),
                                (System.nanoTime() - start) / 1e9, cpu < 0 || cpuEnd < 0 ? Double.NaN : (cpuEnd - cpu) / 1e9);
                        batches.flush();
                    }
                    return rows;
                } finally { active.decrementAndGet(); }
            }) {
                @Override protected void done() {
                    if (!isCancelled()) {
                        try { get(); }
                        catch (ExecutionException e) { failure.compareAndSet(null, e.getCause()); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); failure.compareAndSet(null, e); }
                    }
                }
            };
            pending.addLast(new Task(part, future));
            occupied.incrementAndGet();
            readers.execute(future);
        }
    }

    Lease take(int table, int batch) throws Exception {
        checkFailure();
        if (leased) throw new IllegalStateException("Previous reader lease was not released");
        // The serial loop checks EOF on a following round for exactly full batches.
        if (batch == manifest.tables.get(table).size()) return new Lease(List.of(), false);
        Task task;
        synchronized (this) { task = pending.peekFirst(); }
        if (task == null || task.part.tableId() != table || task.part.batchId() != batch)
            throw new IllegalStateException("Shard request does not match original round-robin order");
        long start = System.nanoTime(), headWait = 0;
        List<String[]> rows;
        while (true) {
            checkFailure();
            boolean blocked = blockedByEarlier();
            long waitStart = System.nanoTime();
            try {
                rows = task.result.get(50, TimeUnit.MILLISECONDS);
                if (blocked) headWait += System.nanoTime() - waitStart;
                break;
            } catch (TimeoutException e) {
                if (blocked) headWait += System.nanoTime() - waitStart;
            } catch (ExecutionException e) { throw new IOException("Shard read failed: " + task.part.path(), e.getCause()); }
        }
        synchronized (batches) {
            batches.printf(Locale.ROOT, "consume\t%d\t%d\t%d\t\t\t%.6f\t%.6f%n", table, batch, rows.size(),
                    (System.nanoTime() - start) / 1e9, headWait / 1e9);
            batches.flush();
        }
        leased = true;
        return new Lease(rows, true);
    }

    final class Lease implements AutoCloseable {
        private List<String[]> rows;
        private final boolean reserved;
        private boolean released;
        Lease(List<String[]> rows, boolean reserved) { this.rows = rows; this.reserved = reserved; }
        List<String[]> rows() { return rows; }
        @Override public void close() {
            if (released) return;
            released = true;
            rows = List.of();
            if (reserved) {
                synchronized (ShardedBatchReader.this) {
                    pending.removeFirst();
                    occupied.decrementAndGet();
                    leased = false;
                    fill();
                }
            }
        }
    }

    void checkFailure() throws IOException {
        Throwable error = failure.get();
        if (error != null) throw new IOException("Parallel shard reader failed", error);
        if (closed) throw new IOException("Shard reader is closed");
    }

    private List<String[]> readPart(ShardManifest.Part part) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        var format = CSVFormat.DEFAULT.builder().setDelimiter(UserConfig.separator.charAt(0))
                .setQuote('"').setIgnoreEmptyLines(true).setTrim(true).setAllowMissingColumnNames(true).build();
        List<String[]> rows = new ArrayList<>();
        try (var input = new DigestInputStream(Files.newInputStream(part.path()), digest);
             var reader = new InputStreamReader(input, StandardCharsets.UTF_8);
             CSVParser parser = new CSVParser(reader, format)) {
            openParsers.add(parser);
            try {
                for (CSVRecord record : parser) {
                    if (Thread.currentThread().isInterrupted() || closed) throw new InterruptedException("Shard read cancelled");
                    String[] row = DataLoader.recordToArray(record, part.tbl());
                    if (row.length != part.columns() || rows.size() >= part.rows())
                        throw new IOException("Shard dimensions differ: " + part.path());
                    rows.add(row);
                }
            } finally { openParsers.remove(parser); }
        }
        if (rows.size() != part.rows() || !HexFormat.of().formatHex(digest.digest()).equals(part.sha256()))
            throw new IOException("Shard row count/checksum mismatch: " + part.path());
        return rows;
    }

    private static long cpuTime() {
        var bean = ManagementFactory.getThreadMXBean();
        return bean.isCurrentThreadCpuTimeSupported() && bean.isThreadCpuTimeEnabled()
                ? bean.getCurrentThreadCpuTime() : -1;
    }

    private synchronized boolean blockedByEarlier() {
        if (pending.isEmpty() || pending.peekFirst().result.isDone()) return false;
        return pending.stream().skip(1).anyMatch(t -> t.result.isDone() && !t.result.isCancelled());
    }

    private synchronized void sample(int threads) {
        if (closed) return;
        long ready = pending.stream().filter(t -> t.result.isDone() && !t.result.isCancelled()).count();
        resources.printf(Locale.ROOT, "%.6f\t%d\t%d\t%d\t%d\t%d\t%d\t%b%n", (System.nanoTime() - started) / 1e9,
                threads, capacity, occupied.get(), active.get(), ((ThreadPoolExecutor) readers).getQueue().size(),
                ready, blockedByEarlier());
        resources.flush();
    }

    @Override public void close() {
        synchronized (this) {
            if (closed) return;
            sample(((ThreadPoolExecutor) readers).getCorePoolSize());
            closed = true;
            pending.forEach(t -> t.result.cancel(true));
            pending.clear();
            occupied.set(0);
        }
        sampler.shutdownNow();
        readers.shutdownNow();
        for (CSVParser parser : openParsers) {
            try { parser.close(); } catch (IOException ignored) { }
        }
        try { readers.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        synchronized (batches) { batches.close(); }
        resources.close();
    }
}
