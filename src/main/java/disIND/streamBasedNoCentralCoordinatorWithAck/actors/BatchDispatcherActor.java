package disIND.streamBasedNoCentralCoordinatorWithAck.actors;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.streamBasedNoCentralCoordinatorWithAck.model.AkkaSerializable;
import disIND.streamBasedNoCentralCoordinatorWithAck.model.RawEvent;

import java.util.*;

public class BatchDispatcherActor extends AbstractBehavior<BatchDispatcherActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record ProcessBatch(
            RawEvent.Batch batch,
            ActorRef<Done> replyTo
    ) implements Command {}

    public record EntityAck(
            long batchId,
            String entityId
    ) implements Command {}

    private final ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion;

    private final Map<Long, ActorRef<Done>> batchReplyTo = new HashMap<>();
    private final Map<Long, Set<String>> pendingAcks = new HashMap<>();

    public static Behavior<Command> create(
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion
    ) {
        return Behaviors.setup(ctx -> new BatchDispatcherActor(ctx, valueRegion));
    }

    private BatchDispatcherActor(
            ActorContext<Command> ctx,
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion
    ) {
        super(ctx);
        this.valueRegion = valueRegion;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ProcessBatch.class, this::onProcessBatch)
                .onMessage(EntityAck.class, this::onEntityAck)
                .build();
    }

    private Behavior<Command> onProcessBatch(ProcessBatch cmd) {
        RawEvent.Batch batch = cmd.batch();

        Map<String, Map<Short, Integer>> grouped = aggregate(batch.events());

        if (grouped.isEmpty()) {
            cmd.replyTo().tell(Done.getInstance());
            return this;
        }

        batchReplyTo.put(batch.batchId(), cmd.replyTo());
        pendingAcks.put(batch.batchId(), new HashSet<>(grouped.keySet()));

        for (var entry : grouped.entrySet()) {
            String entityId = entry.getKey();
            Map<Short, Integer> update = entry.getValue();

            valueRegion.tell(
                    new ShardingEnvelope<>(
                            entityId,
                            new ValueOwnerActor.BatchUpdate(
                                    batch.batchId(),
                                    entityId,
                                    update,
                                    getContext().getSelf()
                            )
                    )
            );
        }

        return this;
    }

    private Behavior<Command> onEntityAck(EntityAck ack) {
        Set<String> pending = pendingAcks.get(ack.batchId());

        if (pending == null) {
            return this;
        }

        pending.remove(ack.entityId());

        if (pending.isEmpty()) {
            ActorRef<Done> replyTo = batchReplyTo.remove(ack.batchId());
            pendingAcks.remove(ack.batchId());

            if (replyTo != null) {
                replyTo.tell(Done.getInstance());
            }

            getContext().getLog().info("Batch {} fully ACKed", ack.batchId());
        }

        return this;
    }

    private Map<String, Map<Short, Integer>> aggregate(List<RawEvent> events) {
        Map<String, Map<Short, Integer>> grouped = new HashMap<>();

        for (RawEvent event : events) {
            if (event instanceof RawEvent.Insert ins) {
                String entityId = Long.toUnsignedString(hashValue(ins.valueStr()), 16);

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