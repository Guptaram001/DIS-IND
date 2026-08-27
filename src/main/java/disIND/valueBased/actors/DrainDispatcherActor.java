package disIND.valueBased.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import disIND.valueBased.model.SharedModel.CMCommand;
import disIND.valueBased.protocol.DrainProtocol;
import disIND.valueBased.protocol.DrainProtocol.Command;
import disIND.valueBased.protocol.DrainProtocol.DrainRecord;
import disIND.valueBased.utility.UserConfig;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A single dispatcher per worker bounds and retries final drain traffic. */
public final class DrainDispatcherActor extends AbstractBehavior<Command> {
    private record InFlight(int partitionId, List<DrainRecord> records, long sentAtNanos) {
    }

    private final ClusterSharding sharding;
    private final int batchSize;
    private final int maxInFlight;
    private final long retryNanos;
    private final int maxPending;
    private final Map<Integer, ArrayDeque<DrainRecord>> pending = new LinkedHashMap<>();
    private final Map<Long, InFlight> inFlight = new HashMap<>();
    private long nextBatchId = 1;

    public static Behavior<Command> create(ClusterSharding sharding) {
        return Behaviors.withTimers(timers -> Behaviors.setup(ctx -> new DrainDispatcherActor(ctx, timers, sharding,
                UserConfig.DRAIN_BATCH_SIZE, UserConfig.DRAIN_MAX_IN_FLIGHT,
                Duration.ofSeconds(UserConfig.DRAIN_RETRY_SECONDS))));
    }

    DrainDispatcherActor(ActorContext<Command> context, TimerScheduler<Command> timers,
            ClusterSharding sharding, int batchSize, int maxInFlight, Duration retryAfter) {
        super(context);
        this.sharding = sharding;
        this.batchSize = batchSize;
        this.maxInFlight = maxInFlight;
        this.retryNanos = retryAfter.toNanos();
        this.maxPending = Math.multiplyExact(batchSize, maxInFlight);
        timers.startTimerAtFixedRate(DrainProtocol.RetryTick.INSTANCE, retryAfter);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(DrainProtocol.Enqueue.class, this::onEnqueue)
                .onMessage(DrainProtocol.EnqueuePartition.class, this::onEnqueuePartition)
                .onMessage(DrainProtocol.BatchAcknowledged.class, this::onAcknowledged)
                .onMessageEquals(DrainProtocol.RetryTick.INSTANCE, this::onRetryTick)
                .build();
    }

    private Behavior<Command> onEnqueue(DrainProtocol.Enqueue message) {
        int pendingCount = pending.values().stream().mapToInt(ArrayDeque::size).sum();
        if (pendingCount >= maxPending)
            return this; // backpressure: producer retries until admitted
        int partitionId = CMCommand.partitionFor(message.record().lhsCol(), UserConfig.DEFAULT_CM_PARTITIONS);
        pending.computeIfAbsent(partitionId, ignored -> new ArrayDeque<>())
                .addLast(message.record());
        message.replyTo().tell(new disIND.valueBased.protocol.ValueOwnerProtocol.DrainQueued(
                message.record().finalRound(), message.record().lhsCol(), message.record().bucketId()));
        pump(false);
        return this;
    }

    private Behavior<Command> onEnqueuePartition(DrainProtocol.EnqueuePartition message) {
        int pendingCount = pending.values().stream().mapToInt(ArrayDeque::size).sum();
        if (pendingCount > 0 && pendingCount + message.records().size() > maxPending)
            return this; // producer retries the complete partition batch
        pending.computeIfAbsent(message.partitionId(), ignored -> new ArrayDeque<>()).addAll(message.records());
        DrainRecord first = message.records().get(0);
        message.replyTo().tell(new disIND.valueBased.protocol.ValueOwnerProtocol.PartitionDrainQueued(
                first.finalRound(), message.partitionId(), first.bucketId()));
        pump(false);
        return this;
    }

    private Behavior<Command> onAcknowledged(DrainProtocol.BatchAcknowledged message) {
        InFlight delivery = inFlight.get(message.batchId());
        if (delivery != null && delivery.partitionId() == message.partitionId())
            inFlight.remove(message.batchId());
        pump(false);
        return this;
    }

    private Behavior<Command> onRetryTick() {
        long now = System.nanoTime();
        new ArrayList<>(inFlight.entrySet()).forEach(entry -> {
            if (now - entry.getValue().sentAtNanos() >= retryNanos) {
                InFlight old = entry.getValue();
                InFlight retried = new InFlight(old.partitionId(), old.records(), now);
                inFlight.put(entry.getKey(), retried);
                send(entry.getKey(), retried);
            }
        });
        pump(true);
        return this;
    }

    private void pump(boolean flushPartial) {
        while (inFlight.size() < maxInFlight) {
            Integer partitionId = pending.entrySet().stream()
                    .filter(e -> e.getValue().size() >= batchSize || (flushPartial && !e.getValue().isEmpty()))
                    .map(Map.Entry::getKey).findFirst().orElse(null);
            if (partitionId == null)
                return;
            ArrayDeque<DrainRecord> queue = pending.get(partitionId);
            List<DrainRecord> records = new ArrayList<>(Math.min(batchSize, queue.size()));
            while (records.size() < batchSize && !queue.isEmpty())
                records.add(queue.removeFirst());
            if (queue.isEmpty())
                pending.remove(partitionId);
            long id = nextBatchId++;
            InFlight delivery = new InFlight(partitionId, List.copyOf(records), System.nanoTime());
            inFlight.put(id, delivery);
            send(id, delivery);
        }
    }

    private void send(long batchId, InFlight delivery) {
        sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(delivery.partitionId()))
                .tell(new CMCommand.OwnersDrained(new DrainProtocol.OwnersDrained(
                        batchId, delivery.records(), getContext().getSelf())));
    }
}
