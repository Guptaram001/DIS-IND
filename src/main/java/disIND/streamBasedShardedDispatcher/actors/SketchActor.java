package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.streamBasedShardedDispatcher.model.Ack;
import disIND.streamBasedShardedDispatcher.model.WorkType;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import scala.App;

import java.util.*;

public class SketchActor extends AbstractBehavior<SketchActor.Command> {

    public interface Command {}

    public record UpdateSketch(
            int batchId,
            long attrId,
            List<String> values,
            ActorRef<Ack> replyTo
    ) implements Command {}

    private final long attrId;

    private final LongOpenHashSet cqfLike = new LongOpenHashSet();
    private final LongOpenHashSet distinct = new LongOpenHashSet();
    private final Map<String, Integer> topK = new HashMap<>();
    private final ActorRef<ShardingEnvelope<AppraisalActor.Command>> appraisalRegion;

    private static final int TOP_K_LIMIT = 10;

    public static Behavior<Command> create(String attrId,ActorRef<ShardingEnvelope<AppraisalActor.Command>> appraisalRegion) {
        return Behaviors.setup(ctx -> new SketchActor(ctx, Long.parseLong(attrId), appraisalRegion));
    }

    private SketchActor(ActorContext<Command> ctx, long attrId,ActorRef<ShardingEnvelope<AppraisalActor.Command>> appraisalRegion) {
        super(ctx);
        this.attrId = attrId;
        this.appraisalRegion = appraisalRegion;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(UpdateSketch.class, this::onUpdate)
                .build();
    }

    private Behavior<Command> onUpdate(UpdateSketch cmd) {

        for (String value : cmd.values()) {
            long hash = hash(value);
            cqfLike.add(hash);
            distinct.add(hash);
            topK.merge(value, 1, Integer::sum);
        }

        if (topK.size() > TOP_K_LIMIT) {
            topK.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue())
                    .limit(topK.size() - TOP_K_LIMIT)
                    .map(Map.Entry::getKey)
                    .toList()
                    .forEach(topK::remove);
        }
        getContext().getLog().info(
                "SketchActor attr={} batch={} size={} distinct~{} topK={}",
                attrId,
                cmd.batchId(),
                cmd.values().size(),
                distinct.size(),
                topK.keySet()
        );

        Set<Long> sample = new HashSet<>();
        LongIterator it = distinct.iterator();
        int k = 0;
        while (it.hasNext() && k < 5) {
            sample.add(it.nextLong());
            k++;
        }
        appraisalRegion.tell(
                new ShardingEnvelope<>(
                        String.valueOf(attrId),
                        new AppraisalActor.SketchSummary(
                                attrId,
                                distinct.size(),
                                sample
                        )
                )
        );
        cmd.replyTo().tell(new Ack(cmd.batchId(), cmd.attrId(), WorkType.SKETCH));

        return this;
    }

    private long hash(String value) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < value.length(); i++) {
            h ^= value.charAt(i);
            h *= 0x100000001b3L;
        }
        return h;
    }
}