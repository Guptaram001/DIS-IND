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

    private record WrappedAck(int batchId, String source) implements Command {}
    private final ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion;
    private final ActorRef<ShardingEnvelope<SketchActor.Command>> sketchRegion;

    private final Map<Integer, Integer> pendingAcks = new HashMap<>();
    private final Map<Integer, ActorRef<Done>> replyMap = new HashMap<>();

    private final String dispatcherId;

    public static Behavior<Command> create(
            String dispatcherId,
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion,
            ActorRef<ShardingEnvelope<SketchActor.Command>> sketchRegion
    ) {
        return Behaviors.setup(ctx ->
                new BatchDispatcherActor(ctx, dispatcherId, valueRegion, sketchRegion)
        );
    }

    private BatchDispatcherActor(
            ActorContext<Command> ctx,
            String dispatcherId,
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion,
            ActorRef<ShardingEnvelope<SketchActor.Command>> sketchRegion
    ) {
        super(ctx);
        this.dispatcherId = dispatcherId;
        this.valueRegion = valueRegion;
        this.sketchRegion = sketchRegion;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ProcessBatch.class, this::onProcessBatch)
                .onMessage(WrappedAck.class, this::onAck)
                .build();
    }

    private Behavior<Command> onProcessBatch(ProcessBatch cmd) {

        int batchId = (int)cmd.batch().batchId();

        var entityUpdates = aggregateBatch(cmd.batch().events());
        var sketchUpdates = aggregateSketch(cmd.batch().events());

        int expected = entityUpdates.size() + sketchUpdates.size();

        pendingAcks.put(batchId, expected);
        replyMap.put(batchId, cmd.replyTo());

        ActorRef<Ack> ackAdapter =
                getContext().messageAdapter(
                        Ack.class,
                        ack -> new WrappedAck(ack.batchId(),ack.source())
                );

        getContext().getLog().info(
                "Dispatcher {} processing batch={} entities={} sketches={}",
                dispatcherId,
                batchId,
                entityUpdates.size(),
                sketchUpdates.size()
        );

        for (var e : entityUpdates.entrySet()) {
            valueRegion.tell(
                    new ShardingEnvelope<>(
                            e.getKey(),
                            new ValueOwnerActor.BatchUpdate(
                                    batchId,
                                    "value",
                                    e.getValue(),
                                    ackAdapter
                            )
                    )
            );
        }

        for (var e : sketchUpdates.entrySet()) {
            sketchRegion.tell(
                    new ShardingEnvelope<>(
                            String.valueOf(e.getKey()),
                            new SketchActor.UpdateSketch(
                                    batchId,
                                    "sketch",
                                    e.getValue(),
                                    ackAdapter
                            )
                    )
            );
        }

        return this;
    }

    private Behavior<Command> onAck(WrappedAck ack) {

        int batchId = ack.batchId();

        Integer current = pendingAcks.get(batchId);
        if (current == null) {
            getContext().getLog().warn(
                    "Dispatcher {} received late/duplicate ACK batch={} source={}",
                    dispatcherId,
                    batchId,
                    ack.source()
            );
            return this;
        }

        int remaining = current - 1;

        getContext().getLog().info(
                "Dispatcher {} received ACK batch={} source={} remaining={}",
                dispatcherId,
                batchId,
                ack.source(),
                remaining
        );

        if (remaining == 0) {
            getContext().getLog().info(
                    "Dispatcher {} batch={} fully ACKed value+sketch",
                    dispatcherId,
                    batchId
            );

            pendingAcks.remove(batchId);

            ActorRef<Done> replyTo = replyMap.remove(batchId);
            if (replyTo != null) {
                replyTo.tell(Done.getInstance());
            }

        } else {
            pendingAcks.put(batchId, remaining);
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

    private static Map<Short, List<String>> aggregateSketch(
            List<RawEvent> events
    ) {
        Map<Short, List<String>> grouped = new HashMap<>();

        for (RawEvent event : events) {
            if (event instanceof RawEvent.Insert ins) {
                grouped
                        .computeIfAbsent(ins.attrId(), k -> new ArrayList<>())
                        .add(ins.valueStr());
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