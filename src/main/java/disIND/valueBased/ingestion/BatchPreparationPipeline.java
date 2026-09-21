package disIND.valueBased.ingestion;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Single producer, parallel preparation, ordered submission. Capacity includes the reader's reservation. */
public final class BatchPreparationPipeline<T> implements AutoCloseable {
    @FunctionalInterface
    public interface Sink<T> {
        void submit(T batch) throws Exception;
    }

    private final Semaphore capacity;
    private final ExecutorService workers;
    private final BlockingQueue<FutureTask<T>> ordered = new LinkedBlockingQueue<>();
    private final Set<FutureTask<T>> pending = ConcurrentHashMap.newKeySet();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean inputFinished = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> submitted = new CompletableFuture<>();
    private final Thread submitter;
    private final Sink<T> sink;

    public BatchPreparationPipeline(int threads, int outstanding, Sink<T> sink) {
        if (threads <= 0 || outstanding <= 0)
            throw new IllegalArgumentException("Preparation threads and capacity must be positive");
        this.sink = sink;
        capacity = new Semaphore(outstanding);
        workers = Executors.newFixedThreadPool(threads, task -> {
            Thread thread = new Thread(task, "dis-ind-batch-preparation");
            thread.setDaemon(true);
            return thread;
        });
        submitter = new Thread(this::submitLoop, "dis-ind-prepared-submission");
        submitter.setDaemon(true);
        submitter.start();
    }

    /** Acquire before reading rows; only the producer may reserve and enqueue work. */
    public Reservation reserve() throws Exception {
        while (true) {
            checkOpen();
            if (capacity.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                try {
                    checkOpen();
                    return new Reservation();
                } catch (Exception exception) {
                    capacity.release();
                    throw exception;
                }
            }
        }
    }

    public final class Reservation implements AutoCloseable {
        private boolean transferred;
        private boolean released;

        public void submit(Callable<T> preparation) throws Exception {
            if (transferred || released)
                throw new IllegalStateException("Reservation already used");
            checkOpen();
            FutureTask<T> task = new FutureTask<>(preparation) {
                @Override
                protected void done() {
                    if (!isCancelled()) {
                        try {
                            get();
                        } catch (ExecutionException exception) {
                            fail(exception.getCause());
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                            fail(exception);
                        }
                    }
                }
            };
            pending.add(task);
            transferred = true;
            ordered.add(task);
            try {
                workers.execute(task);
            } catch (RuntimeException exception) {
                fail(exception);
                throw exception;
            }
        }

        @Override
        public void close() {
            if (!transferred && !released) {
                released = true;
                capacity.release();
            }
        }
    }

    /** Called after all reservations have been submitted or closed, including restoration batches. */
    public void finishAndWait() throws Exception {
        inputFinished.set(true);
        workers.shutdown();
        try {
            submitted.get();
        } catch (ExecutionException exception) {
            throw new Exception("Batch preparation/submission failed", exception.getCause());
        }
        checkFailure();
    }

    private void submitLoop() {
        try {
            while (!closed.get()) {
                checkFailure();
                FutureTask<T> task = ordered.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    if (inputFinished.get() && ordered.isEmpty()) {
                        submitted.complete(null);
                        return;
                    }
                    continue;
                }
                try {
                    T batch = task.get();
                    checkFailure();
                    sink.submit(batch);
                } finally {
                    pending.remove(task);
                    capacity.release();
                }
            }
        } catch (Throwable exception) {
            fail(exception instanceof ExecutionException ? exception.getCause() : exception);
        }
    }

    private void fail(Throwable exception) {
        if (failure.compareAndSet(null, exception)) {
            submitted.completeExceptionally(exception);
            pending.forEach(task -> task.cancel(true));
            workers.shutdownNow();
            submitter.interrupt();
        }
    }

    /** Also lets a producer abort promptly during a long read. */
    public void checkFailure() throws Exception {
        Throwable exception = failure.get();
        if (exception != null)
            throw new Exception("Batch preparation/submission failed", exception);
    }

    private void checkOpen() throws Exception {
        checkFailure();
        if (closed.get() || inputFinished.get())
            throw new IllegalStateException("Preparation input is closed");
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true))
            return;
        fail(new CancellationException("Preparation pipeline closed"));
        pending.forEach(task -> task.cancel(true));
        ordered.clear();
        workers.shutdownNow();
        submitter.interrupt();
        boolean interrupted = false;
        try {
            submitter.join(5000);
            workers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            interrupted = true;
        } finally {
            pending.clear();
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
