package disIND.streamBasedShardedDispatcher.actors;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;
import disIND.streamBasedShardedDispatcher.model.RawEvent;

import java.util.*;
public class BatchDispatcherActor extends AbstractBehavior<BatchDispatcherActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record ProcessBatch(
            RawEvent.Batch batch,
            ActorRef<Done> replyTo
    ) implements Command {}

    public record ValueAck(
            long batchId,
            String entityId
    ) implements Command {}

    private static class PendingBatch {
        final ActorRef<Done> replyTo;
        final Set<String> waitingEntities;

        PendingBatch(ActorRef<Done> replyTo, Set<String> waitingEntities) {
            this.replyTo = replyTo;
            this.waitingEntities = waitingEntities;
        }
    }

    private final String dispatcherId;
    private final ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion;

    private final Map<Long, PendingBatch> pending = new HashMap<>();

    public static Behavior<Command> create(
            String dispatcherId,
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion
    ) {
        return Behaviors.setup(ctx ->
                new BatchDispatcherActor(ctx, dispatcherId, valueRegion)
        );
    }

    private BatchDispatcherActor(
            ActorContext<Command> ctx,
            String dispatcherId,
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion
    ) {
        super(ctx);
        this.dispatcherId = dispatcherId;
        this.valueRegion = valueRegion;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ProcessBatch.class, this::onProcessBatch)
                .onMessage(ValueAck.class, this::onValueAck)
                .build();
    }

    private Behavior<Command> onProcessBatch(ProcessBatch cmd) {

        RawEvent.Batch batch = cmd.batch();

        Map<String, Map<Short, Integer>> grouped =
                aggregateBatch(batch.events());

        if (grouped.isEmpty()) {
            cmd.replyTo().tell(Done.getInstance());
            return this;
        }

        Set<String> waiting = new HashSet<>(grouped.keySet());

        pending.put(
                batch.batchId(),
                new PendingBatch(cmd.replyTo(), waiting)
        );

        getContext().getLog().info(
                "Dispatcher {} processing batch={} entities={}",
                dispatcherId,
                batch.batchId(),
                waiting.size()
        );

        for (Map.Entry<String, Map<Short, Integer>> e : grouped.entrySet()) {
            String entityId = e.getKey();
            Map<Short, Integer> attrCounts = e.getValue();

            valueRegion.tell(
                    new ShardingEnvelope<>(
                            entityId,
                            new ValueOwnerActor.ApplyUpdate(
                                    batch.batchId(),
                                    entityId,
                                    attrCounts,
                                    getContext().getSelf()
                            )
                    )
            );
        }

        return this;
    }

    private Behavior<Command> onValueAck(ValueAck ack) {

        PendingBatch p = pending.get(ack.batchId());

        if (p == null) {
            return this;
        }

        p.waitingEntities.remove(ack.entityId());

        if (p.waitingEntities.isEmpty()) {
            pending.remove(ack.batchId());

            getContext().getLog().info(
                    "Dispatcher {} batch={} fully ACKed",
                    dispatcherId,
                    ack.batchId()
            );

            p.replyTo.tell(Done.getInstance());
        }

        return this;
    }

    private static Map<String, Map<Short, Integer>> aggregateBatch(
            List<RawEvent> events
    ) {
        Map<String, Map<Short, Integer>> grouped = new HashMap<>();

        for (RawEvent event : events) {
            if (event instanceof RawEvent.Insert ins) {
                String entityId =
                        Long.toUnsignedString(hashValue(ins.valueStr()), 16);

                grouped
                        .computeIfAbsent(entityId, k -> new HashMap<>())
                        .merge(ins.attrId(), 1, Integer::sum);
            }
        }

        return grouped;
    }

    public static long hashValue(String value) {
        if (value == null) return 0L;

        long hash = 0xcbf29ce484222325L;

        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001b3L;
        }

        return hash;
    }
}