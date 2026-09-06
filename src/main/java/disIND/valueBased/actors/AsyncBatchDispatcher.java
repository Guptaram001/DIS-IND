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

public final class AsyncBatchDispatcher {

    private sealed interface Event permits SubmittedEvent, CompletedEvent, FinishedEvent {
    }

    private record SubmittedEvent(PreparedBatch batch) implements Event {
    }

    private record CompletedEvent(PreparedBatch batch, Throwable failure) implements Event {
    }

    private enum FinishedEvent implements Event {
        DONE
    }

    private static final long WAIT_MILLIS = 100L;

    private long processedRows;
    private long ingestionStartedNanos = System.nanoTime();
    private final BatchTimeMonitor batchTimeMonitorWriter = new BatchTimeMonitor();

    private record InFlightBatch(int tableId, int batchId, int round, long startedNanos) {
    }

    private final boolean enforceTableOrder;
    private final int creditWindow;
    private final int maximumOutstanding;
    private final Semaphore outstandingSlots;
    private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private final ExecutorService executor;
    private final ActorRef<BDCommand> guardian;
    private final ActorSystem<?> system;

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

    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final CompletableFuture<Void> dispatcherFinished;

    public AsyncBatchDispatcher(ActorRef<BDCommand> guardian, ActorSystem<?> system, int creditWindow,
            int queueCapacity, boolean enforceTableOrder) {
        if (creditWindow <= 0)
            throw new IllegalArgumentException("creditWindow must be positive");

        if (queueCapacity <= 0)
            throw new IllegalArgumentException("queueCapacity must be positive");

        this.guardian = guardian;
        this.system = system;
        this.enforceTableOrder = enforceTableOrder;
        this.creditWindow = creditWindow;
        this.maximumOutstanding = Math.addExact(creditWindow, queueCapacity);
        this.outstandingSlots = new Semaphore(maximumOutstanding);
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "dis-ind-batch-dispatcher");
            thread.setDaemon(true); // When main thread terminates, it too has to terminate.
            return thread;
        });
        this.dispatcherFinished = CompletableFuture.runAsync(this::dispatchLoop, executor);
    }

    private CompletionStage<BDReply> sendBatch(PreparedBatch batch) {
        return DataLoader.sendTableBatch(guardian, system, batch.epoch(), batch.tableId(), batch.startRowId(),
                batch.ownerBatches(), batch.round(), batch.individualBatchId(), batch.orientation());
    }

    public void submit(PreparedBatch batch) throws Exception {
        acquireOutstandingSlot();
        boolean accepted = false;
        try {
            throwIfFailed();
            if (producerFinished.get())
                throw new IllegalStateException("Cannot submit after dispatcher finish");
            queued.incrementAndGet();
            submitted.incrementAndGet();
            events.add(new SubmittedEvent(batch));
            accepted = true;
        } finally {
            if (!accepted)
                outstandingSlots.release();
        }
    }

    public void finishAndWait() throws Exception {
        if (producerFinished.compareAndSet(false, true))
            events.offer(FinishedEvent.DONE);
        try {
            drained.get();
            dispatcherFinished.get();
        } catch (ExecutionException exception) {
            recordFailure(exception.getCause());
        }
        throwIfFailed();
    }

    private void acquireOutstandingSlot() throws Exception {
        // tries to find slot to add work, returns when free slott found
        while (true) {
            throwIfFailed();
            if (outstandingSlots.tryAcquire(WAIT_MILLIS, TimeUnit.MILLISECONDS)) // Max time and hte unit
                return;
        }
    }

    private void dispatchLoop() {
        int availableCredits = creditWindow;
        try {
            while (true) {
                Throwable currentFailure = failure.get();
                if (currentFailure != null)
                    throw new CompletionException(currentFailure);
                // Needs wait for empty queue, if no event return null.
                Event event = events.poll(WAIT_MILLIS, TimeUnit.MILLISECONDS);
                if (event instanceof SubmittedEvent submittedEvent)
                    schedule(submittedEvent.batch());
                else if (event instanceof CompletedEvent completedEvent)
                    availableCredits = handleCompletion(completedEvent, availableCredits);

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
        // Determines if the batch should go for execution to ready or wait.
        // Table order is false then add to ready queue.
        // ScheduledTable already has a tableId so returns false, so added to
        // waitingTable.
        if (!enforceTableOrder || scheduledTables.add(batch.tableId())) {
            ready.addLast(batch);
            return;
        }
        // Adds to the batch of corresponding tableId.
        waitingByTable.computeIfAbsent(batch.tableId(), ignored -> new ArrayDeque<>()).addLast(batch);
    }

    private int dispatchAvailable(int availableCredits) {

        while (availableCredits > 0 && !ready.isEmpty()) {
            // At least a batch exist with credits.
            PreparedBatch batch = ready.removeFirst();
            // Prevent two batches of same table to be in flight to handle delete.
            if (enforceTableOrder && !tablesInFlight.add(batch.tableId()))
                throw new IllegalStateException("Concurrent batches selected for table " + batch.tableId());
            queued.decrementAndGet();
            availableCredits--;
            dispatch(batch);
        }
        return availableCredits;
    }

    private void dispatch(PreparedBatch batch) {
        inFlight.incrementAndGet();
        // Assigns the batch details with time.
        inFlightByEpoch.put(batch.epoch(),
                new InFlightBatch(batch.tableId(), batch.individualBatchId(), batch.round(), System.nanoTime()));
        CompletionStage<BDReply> stage;
        try {
            stage = sendBatch(batch);
        } catch (Throwable throwable) {
            events.offer(new CompletedEvent(batch, throwable));
            return;
        }
        if (stage == null) {
            events.offer(new CompletedEvent(batch, new NullPointerException("Batch sender returned null stage")));
            return;
        }
        stage.whenComplete((reply, throwable) -> events.offer(new CompletedEvent(batch, throwable)));
    }

    private int handleCompletion(CompletedEvent event, int availableCredits) {
        PreparedBatch batch = event.batch();
        Throwable throwable = event.failure();
        InFlightBatch completedBatch = inFlightByEpoch.remove(batch.epoch());
        if (completedBatch == null)
            throw new IllegalStateException("No timing information for epoch " + batch.epoch());

        completed.incrementAndGet();
        inFlight.decrementAndGet();
        outstandingSlots.release();
        availableCredits++;

        if (enforceTableOrder) {
            if (!tablesInFlight.remove(batch.tableId()))
                throw new IllegalStateException("Completed inactive table " + batch.tableId());
            // allow the next batch from the same table to start.
            if (throwable == null)
                promoteNextForTable(batch.tableId());
        }
        if (throwable != null)
            throw new CompletionException(throwable);

        recordProcessedRows(batch, completedBatch);
        return availableCredits;
    }

    private void recordProcessedRows(PreparedBatch batch, InFlightBatch completedBatch) {
        long completedNanos = System.nanoTime();
        processedRows = Math.addExact(processedRows, batch.rowCount());
        double dispatchStartedSec = (completedBatch.startedNanos() - ingestionStartedNanos) / 1_000_000_000.0;
        double completionSec = (completedNanos - ingestionStartedNanos) / 1_000_000_000.0;
        double batchLatencySec = completionSec - dispatchStartedSec;
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

    private void recordFailure(Throwable throwable) {
        if (!failure.compareAndSet(null, throwable))
            return;
        producerFinished.set(true);
        drained.completeExceptionally(throwable);
        outstandingSlots.release(maximumOutstanding);
    }

    private void throwIfFailed() throws Exception {
        Throwable throwable = failure.get();
        if (throwable != null)
            throw new Exception(throwable);
    }

    public void close() {
        executor.shutdownNow();
        batchTimeMonitorWriter.close();
    }
}
