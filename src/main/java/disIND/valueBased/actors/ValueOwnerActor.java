package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.BDCommand;
import disIND.valueBased.utility.Debug;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static disIND.valueBased.utility.Debug.formLog;

public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command extends AkkaSerializable {}
    public static final EntityTypeKey<Command> TYPE_KEY =EntityTypeKey.create(Command.class,"ValueOwnerActor");

    public record ValueCount(int valueId, int colId, int count) implements AkkaSerializable {
        public ValueCount {
            if (count <= 0)
                throw new IllegalArgumentException("count must be positive");
        }
    }

    public record StoreBatch(long epoch, int tableId, int batchId, int bucketId,
        List<ValueCount> values, ActorRef<BDCommand> ackTo) implements Command {
        public StoreBatch {
            values = List.copyOf(values);
        }
    }

    public record GetBucket(ActorRef<BucketSnapshot> replyTo) implements Command {}
    public record ColumnCount(int colId, long count) implements AkkaSerializable {}
    public record BucketSnapshot(int bucketId, Map<Integer, List<ColumnCount>> values)implements AkkaSerializable {}

    private final String entityId;
    private final int bucketId;
    private final SetMultimap<Integer, ColumnCount> values = HashMultimap.create();
    private final Set<String> appliedBatches = new HashSet<>();

    public static String entityId(int bucketId) {
        return "value-bucket-" + bucketId;
    }

    public static Behavior<Command> create(String entityId) {
        return Behaviors.setup(ctx -> new ValueOwnerActor(ctx, entityId));
    }

    private ValueOwnerActor(ActorContext<Command> ctx, String entityId) {
        super(ctx);
        this.entityId = entityId;
        this.bucketId = Integer.parseInt(entityId.substring("value-bucket-".length()));
        getContext().getLog().info("[PLACEMENT] type=VO bucket={} entity={} node={}",bucketId, entityId,
                Cluster.get(ctx.getSystem()).selfMember().address());
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StoreBatch.class, this::onStoreBatch)
                .onMessage(GetBucket.class, this::onGetBucket)
                .build();
    }

    private Behavior<Command> onStoreBatch(StoreBatch msg) {
        getContext().getLog().info("[VALUE-OWNER] bucket={} epoch={} tableId={} batchId={} aggregatedUpdates={}",
                msg.bucketId(), msg.epoch(), msg.tableId(), msg.batchId(), msg.values().size());
        if (msg.bucketId() != bucketId)
            throw new IllegalArgumentException("Message for bucket " + msg.bucketId()+ " sent to value owner " + entityId);

        String batchKey = msg.tableId() + ":" + msg.batchId();
        boolean applied = appliedBatches.add(batchKey);
        if (applied) {
            for (ValueCount item : msg.values()) {
                mergeCount(item);
            }
        }
        if (Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.vo(),-1,"-",
                    String.valueOf(Debug.State.NONE),"Multimap updated bucketId={} epoch={} applied={} values={} valueColumnEntries={} sample={}",
                    bucketId, msg.epoch(), applied, values.size(), valueColumnEntryCount(),sampleEntry());
        if (msg.ackTo() != null)
            msg.ackTo().tell(new BDCommand.ValueBucketFlushed(msg.epoch(), bucketId));
        return this;
    }

    private Behavior<Command> onGetBucket(GetBucket msg) {
        Map<Integer, List<ColumnCount>> snapshot = new HashMap<>();
        values.asMap().forEach((valueId, columnCounts) ->snapshot.put(valueId, List.copyOf(columnCounts)));
        msg.replyTo().tell(new BucketSnapshot(bucketId, Map.copyOf(snapshot)));
        return this;
    }

    private void mergeCount(ValueCount item) {
        Collection<ColumnCount> columns = values.get(item.valueId());
        ColumnCount previous = null;
        for (ColumnCount candidate : columns) {
            if (candidate.colId() == item.colId()) {
                previous = candidate;
                break;
            }
        }

        long total = item.count();
        if (previous != null) {
            total += previous.count();
            columns.remove(previous);
        }
        columns.add(new ColumnCount(item.colId(), total));
    }

    private String sampleEntry() {
        if (values.isEmpty())
            return "-";
        Map.Entry<Integer, ColumnCount> sample = values.entries().iterator().next();
        return sample.getKey() + "->col" + sample.getValue().colId()+ ":" + sample.getValue().count();
    }

    private int valueColumnEntryCount() {
        return values.size();
    }
}
