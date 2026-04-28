package disIND.streamBasedShardedDispatcher.actors;


import akka.actor.typed.Behavior;
import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.*;

import java.util.*;

public class SketchActor extends AbstractBehavior<SketchActor.Command> {

    public interface Command {}

    public record UpdateSketch(
            int batchId,
            String source,
            List<String> values,
            ActorRef<Ack> replyTo
    ) implements Command {}

    private final String attrId;

    private final Set<Long> cqfLike = new HashSet<>();   // simulate CQF
    private final Set<Long> distinct = new HashSet<>();  // simulate HLL
    private final Map<String, Integer> topK = new HashMap<>();

    private static final int TOP_K_LIMIT = 10;

    public static Behavior<Command> create(String attrId) {
        return Behaviors.setup(ctx -> new SketchActor(ctx, attrId));
    }

    private SketchActor(ActorContext<Command> ctx, String attrId) {
        super(ctx);
        this.attrId = attrId;
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
        cmd.replyTo().tell(new Ack(cmd.batchId(),cmd.source()));
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