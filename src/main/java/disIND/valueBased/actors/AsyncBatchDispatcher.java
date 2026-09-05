package disIND.valueBased.actors;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.ActorRef;
import disIND.valueBased.dataset.DataLoader;
import disIND.valueBased.dataset.DataLoader.PreparedBatch;
import disIND.valueBased.model.SharedModel.BDCommand;
import disIND.valueBased.model.SharedModel.BDReply;
import disIND.valueBased.monitor.BatchTimeMonitor;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class AsyncBatchDispatcher implements AutoCloseable {

    @FunctionalInterface
    interface BatchSender {
        CompletionStage<BDReply> send(PreparedBatch batch);
    }

    private sealed interface Event permits Submitted, Completed, Finish {
    }

    private record Submitted(PreparedBatch batch) implements Event {
    }

    private record Completed(PreparedBatch batch, Throwable failure) implements Event {
    }

    private enum Finish implements Event {
        INSTANCE
    }

    private static final long WAIT_MILLIS = 100L;
    private static final long DIAGNOSTIC_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private long processedRows;
    private long ingestionStartedNanos = System.nanoTime();
    private final BatchTimeMonitor batchTimeMonitorWriter = new BatchTimeMonitor();

    private record InFlightBatch(int tableId, int batchId, int round, long startedNanos) {
    }

    private final boolean enforceTableOrdering;
    private final int creditWindow;
    private final int maximumOutstanding;
    private final Semaphore outstandingSlots;
    private final BatchSender sender;
    private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private final ExecutorService executor;

    private final Set<Integer> scheduledTables = new HashSet<>();
    private final Set<Integer> tablesInFlight = new HashSet<>();
    private final Map<Integer, ArrayDeque<PreparedBatch>> waitingByTable = new HashMap<>();
    private final ArrayDeque<PreparedBatch> ready = new ArrayDeque<>();
    private final Map<Integer, InFlightBatch> inFlightByEpoch = new HashMap<>();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean producerFinished = new AtomicBoolean();
    private final AtomicInteger queued = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong lastDiagnosticNanos = new AtomicLong();
    private final AtomicReference<InFlightBatch> oldestInFlight = new AtomicReference<>();

    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final CompletableFuture<Void> dispatcherFinished;

    public AsyncBatchDispatcher(ActorRef<BDCommand> guardian, ActorSystem<?> system, int creditWindow,
            int queueCapacity, boolean enforceTableOrdering) {
        this(creditWindow, queueCapacity, enforceTableOrdering, batch -> DataLoader.sendTableBatch(
                Objects.requireNonNull(guardian, "guardian"), Objects.requireNonNull(system, "system"),
                batch.epoch(), batch.tableId(), batch.startRowId(),
                batch.ownerBatches(), batch.round(), batch.individualBatchId(), batch.orientation()));
    }

    AsyncBatchDispatcher(int creditWindow, int queueCapacity, boolean enforceTableOrdering, BatchSender sender) {
        if (creditWindow <= 0)
            throw new IllegalArgumentException("creditWindow must be positive");
        if (queueCapacity <= 0)
            throw new IllegalArgumentException("queueCapacity must be positive");

        this.enforceTableOrdering = enforceTableOrdering;
        this.creditWindow = creditWindow;
        this.maximumOutstanding = Math.addExact(creditWindow, queueCapacity);
        this.outstandingSlots = new Semaphore(maximumOutstanding);
        this.sender = Objects.requireNonNull(sender, "sender");
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "dis-ind-batch-dispatcher");
            thread.setDaemon(true);
            return thread;
        });
        this.dispatcherFinished = CompletableFuture.runAsync(this::dispatchLoop, executor);
    }

    public void submit(PreparedBatch batch) throws Exception {
        Objects.requireNonNull(batch, "batch");
        acquireOutstandingSlot();
        boolean accepted = false;
        try {
            throwIfFailed();
            if (producerFinished.get())
                throw new IllegalStateException("Cannot submit after dispatcher finish");
            queued.incrementAndGet();
            submitted.incrementAndGet();
            events.add(new Submitted(batch));
            accepted = true;
        } finally {
            if (!accepted)
                outstandingSlots.release();
        }
    }

    public void finishAndWait() throws Exception {
        if (producerFinished.compareAndSet(false, true))
            events.offer(Finish.INSTANCE);
        try {
            drained.get();
            dispatcherFinished.get();
        } catch (ExecutionException exception) {
            recordFailure(exception.getCause());
        }
        throwIfFailed();
    }

    private void acquireOutstandingSlot() throws Exception {
        while (true) {
            throwIfFailed();
            if (outstandingSlots.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS))
                return;
            maybeLogState("producer-waiting-for-outstanding-slot");
        }
    }

    private void dispatchLoop() {
        int availableCredits = creditWindow;
        try {
            while (true) {
                Throwable currentFailure = failure.get();
                if (currentFailure != null)
                    throw new CompletionException(currentFailure);

                Event event = events.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
                if (event instanceof Submitted submittedEvent)
                    schedule(submittedEvent.batch());
                else if (event instanceof Completed completedEvent)
                    availableCredits = handleCompletion(completedEvent, availableCredits);
                else if (event == null)
                    maybeLogState("dispatcher-idle");

                availableCredits = dispatchAvailable(availableCredits);
                if (producerFinished.get() && events.isEmpty() && ready.isEmpty() && waitingByTable.isEmpty()
                        && inFlight.get() == 0) {
                    drained.complete(null);
                    return;
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            recordFailure(exception);
        } catch (Throwable throwable) {
            recordFailure(throwable);
        } finally {
            Throwable throwable = failure.get();
            if (throwable != null)
                drained.completeExceptionally(throwable);
        }
    }

    private void schedule(PreparedBatch batch) {
        if (!enforceTableOrdering || scheduledTables.add(batch.tableId())) {
            ready.addLast(batch);
            return;
        }
        waitingByTable.computeIfAbsent(batch.tableId(), ignored -> new ArrayDeque<>()).addLast(batch);
    }

    private int dispatchAvailable(int availableCredits) {
        while (availableCredits > 0 && !ready.isEmpty()) {
            PreparedBatch batch = ready.removeFirst();
            if (enforceTableOrdering && !tablesInFlight.add(batch.tableId()))
                throw new IllegalStateException("Concurrent batches selected for table " + batch.tableId());
            queued.decrementAndGet();
            availableCredits--;
            dispatch(batch);
        }
        refreshOldestInFlight();
        return availableCredits;
    }

    private void dispatch(PreparedBatch batch) {
        inFlight.incrementAndGet();
        inFlightByEpoch.put(batch.epoch(),
                new InFlightBatch(batch.tableId(), batch.individualBatchId(), batch.round(), System.nanoTime()));
        CompletionStage<BDReply> stage;
        try {
            stage = sender.send(batch);
        } catch (Throwable throwable) {
            events.offer(new Completed(batch, throwable));
            return;
        }
        if (stage == null) {
            events.offer(new Completed(batch, new NullPointerException("Batch sender returned null stage")));
            return;
        }
        stage.whenComplete((reply, throwable) -> events.offer(new Completed(batch, throwable)));
    }

    private int handleCompletion(Completed event, int availableCredits) {
        PreparedBatch batch = event.batch();
        Throwable throwable = event.failure() == null ? null : unwrap(event.failure());
        // inFlightByEpoch.remove(batch.epoch());
        InFlightBatch timing = inFlightByEpoch.remove(batch.epoch());
        if (timing == null)
            throw new IllegalStateException("No timing information for epoch " + batch.epoch());

        completed.incrementAndGet();
        inFlight.decrementAndGet();
        outstandingSlots.release();
        availableCredits++;

        if (enforceTableOrdering) {
            if (!tablesInFlight.remove(batch.tableId()))
                throw new IllegalStateException("Completed inactive table " + batch.tableId());
            if (throwable == null)
                promoteNextForTable(batch.tableId());
        }
        if (throwable != null)
            throw new CompletionException(throwable);

        recordProcessedRows(batch, timing);
        return availableCredits;
    }

    private void recordProcessedRows(PreparedBatch batch, InFlightBatch timing) {
        long completedNanos = System.nanoTime();
        processedRows = Math.addExact(processedRows, batch.rowCount());
        double dispatchStartedSec = (timing.startedNanos() - ingestionStartedNanos) / 1_000_000_000.0;
        double completionSec = (completedNanos - ingestionStartedNanos) / 1_000_000_000.0;
        double batchLatencySec = (completedNanos - timing.startedNanos()) / 1_000_000_000.0;
        batchTimeMonitorWriter.write(batch.epoch(), batch.tableId(), batch.individualBatchId(), batch.round(),
                batch.rowCount(), processedRows, dispatchStartedSec, completionSec, batchLatencySec);
    }

    private void promoteNextForTable(int tableId) {
        ArrayDeque<PreparedBatch> waiting = waitingByTable.get(tableId);
        if (waiting == null || waiting.isEmpty()) {
            waitingByTable.remove(tableId);
            scheduledTables.remove(tableId);
            return;
        }
        ready.addLast(waiting.removeFirst());
        if (waiting.isEmpty())
            waitingByTable.remove(tableId);
    }

    private void refreshOldestInFlight() {
        InFlightBatch oldest = null;
        for (InFlightBatch candidate : inFlightByEpoch.values()) {
            if (oldest == null || candidate.startedNanos() < oldest.startedNanos())
                oldest = candidate;
        }
        oldestInFlight.set(oldest);
    }

    private void maybeLogState(String reason) {
        long now = System.nanoTime();
        long previous = lastDiagnosticNanos.get();
        if (now - previous < DIAGNOSTIC_INTERVAL_NANOS || !lastDiagnosticNanos.compareAndSet(previous, now))
            return;

        InFlightBatch oldest = oldestInFlight.get();
        if (oldest == null)
            return;

    }

    private void recordFailure(Throwable throwable) {
        Throwable unwrapped = unwrap(throwable);
        if (!failure.compareAndSet(null, unwrapped))
            return;
        producerFinished.set(true);
        drained.completeExceptionally(unwrapped);
        outstandingSlots.release(maximumOutstanding);
    }

    private void throwIfFailed() throws Exception {
        Throwable throwable = failure.get();
        if (throwable == null)
            return;
        if (throwable instanceof Exception exception)
            throw exception;
        throw new RuntimeException(throwable);
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null)
            current = current.getCause();
        return current;
    }

    @Override
    public void close() {
        executor.shutdownNow();
        batchTimeMonitorWriter.close();
    }
}
