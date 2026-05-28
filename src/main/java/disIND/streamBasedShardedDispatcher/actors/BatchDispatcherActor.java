package disIND.streamBasedShardedDispatcher.actors;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.streamBasedShardedDispatcher.model.Ack;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;
import disIND.streamBasedShardedDispatcher.model.RawEvent;
import disIND.streamBasedShardedDispatcher.model.WorkType;
import it.unimi.dsi.fastutil.longs.*;


import java.util.*;
public class BatchDispatcherActor extends AbstractBehavior<BatchDispatcherActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record ProcessBatch(
            RawEvent.Batch batch,
            ActorRef<Done> replyTo
    ) implements Command {}

    private final Long2ObjectOpenHashMap<LongOpenHashSet> pendingWork = new Long2ObjectOpenHashMap<>();
    private final Long2ObjectOpenHashMap<ActorRef<Done>> replyMap = new Long2ObjectOpenHashMap<>();

    private record WrappedAck(
            long batchId,
            long targetId,
            WorkType type
    ) implements Command {}

    private final ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion;
    private final ActorRef<ShardingEnvelope<SketchActor.Command>> sketchRegion;

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

        long batchId = cmd.batch().batchId();
        var entityUpdates = aggregateBatch(cmd.batch().events());
        var sketchUpdates = aggregateSketch(cmd.batch().events());
        LongOpenHashSet workSet = new LongOpenHashSet();

        for (var e : entityUpdates.entrySet()) {
            long wid = workId(batchId, e.getKey(), WorkType.VALUE);
            workSet.add(wid);
        }

        for (var e : sketchUpdates.entrySet()) {
            short attrId = e.getKey();
            long wid = workId(batchId, attrId, WorkType.SKETCH);
            workSet.add(wid);
        }
        pendingWork.put(batchId, workSet);
        replyMap.put(batchId, cmd.replyTo());

        ActorRef<Ack> ackAdapter = getContext().messageAdapter(Ack.class, this::onAckAdapter);

        getContext().getLog().info("Dispatcher {} batch={} workItems={}", dispatcherId, batchId, workSet.size());

        for (var e : entityUpdates.entrySet()) {
            long entityHash = e.getKey();
            String entityId = Long.toUnsignedString(entityHash, 16);

            valueRegion.tell(
                    new ShardingEnvelope<>(
                            entityId,
                            new ValueOwnerActor.BatchUpdate(
                                    (int) batchId,
                                    entityHash,
                                    e.getValue(),
                                    ackAdapter
                            )
                    )
            );
        }

        for (var e : sketchUpdates.entrySet()) {
            short attrId =e.getKey();
            String shardKey = Long.toString(attrId);
            sketchRegion.tell(
                    new ShardingEnvelope<>(
                            shardKey,
                            new SketchActor.UpdateSketch(
                                    (int) batchId,
                                    attrId,
                                    e.getValue(),
                                    ackAdapter
                            )
                    )
            );
        }

        return this;
    }

    private Command onAckAdapter(Ack ack) {
        return new WrappedAck(
                ack.batchId(),
                ack.targetId(),
                ack.type()
        );
    }

    private Behavior<Command> onAck(WrappedAck ack) {
        long batchId = ack.batchId();
        LongOpenHashSet workSet = pendingWork.get(batchId);
        long wid = workId(batchId, ack.targetId(), ack.type());
        if (workSet == null) return this;

        boolean removed = workSet.remove(wid);

        if (!removed) {
            getContext().getLog().warn("Duplicate or unknown ACK batch={} target={} type={}", batchId, ack.targetId(), ack.type());
            return this;
        }

        if (workSet.isEmpty()) {
            getContext().getLog().info("Dispatcher {} batch={} fully ACKed", dispatcherId, batchId);
            pendingWork.remove(batchId);
            ActorRef<Done> reply = replyMap.remove(batchId);
            if (reply != null) {
                reply.tell(Done.getInstance());
            }
        }
        return this;
    }


    private static Map<Long, Map<Short, Integer>> aggregateBatch(List<RawEvent> events) {
        Map<Long, Map<Short, Integer>> grouped = new HashMap<>();

        for (RawEvent event : events) {
            if (event instanceof RawEvent.Insert ins) {
                long entityHash = hashValue(ins.valueStr());
                grouped.computeIfAbsent(entityHash, k -> new HashMap<>())
                        .merge(ins.attrId(), 1, Integer::sum);
            }
        }

        return grouped;
    }

    private static Map<Short, List<String>> aggregateSketch(List<RawEvent> events) {
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

    private long workId(long batchId, long targetHash, WorkType type) {
        long h = batchId * 31 + targetHash;
        return (h << 2) | type.ordinal();
    }
}