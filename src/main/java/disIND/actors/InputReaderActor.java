package disIND.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.model.AkkaSerializable;
import disIND.model.RawEvent;
public class InputReaderActor extends AbstractBehavior<InputReaderActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record SubmitBatch(RawEvent.Batch batch) implements Command {}
    public record EventProcessed() implements Command {}

    private final ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion;

    public static Behavior<Command> create(
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion
    ) {
        return Behaviors.setup(ctx -> new InputReaderActor(ctx, valueRegion));
    }

    private InputReaderActor(
            ActorContext<Command> ctx,
            ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion
    ) {
        super(ctx);
        this.valueRegion = valueRegion;
    }

    private Behavior<Command> onEventProcessed(EventProcessed msg) {
        remainingEvents--;

        if (remainingEvents == 0) {
            getContext().getLog().info("ALL DONE");
            getContext().getSystem().terminate();
        }
        return this;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(SubmitBatch.class, this::onSubmitBatch)
                .onMessage(EventProcessed.class, this::onEventProcessed)
                .build();
    }

    private Behavior<Command> onSubmitBatch(SubmitBatch cmd) {
        remainingEvents += cmd.batch().events().size();

        for (RawEvent event : cmd.batch().events()) {

            long valueHash = hashValue(event.valueStr());
            String entityId = Long.toUnsignedString(valueHash, 16);

            if (event instanceof RawEvent.Insert ins) {
                valueRegion.tell(new ShardingEnvelope<>(
                        entityId,
                        new ValueOwnerActor.InsertCmd(ins.attrId(), getContext().getSelf())
                ));
            }

            else if (event instanceof RawEvent.Delete del) {
                valueRegion.tell(new ShardingEnvelope<>(
                        entityId,
                        new ValueOwnerActor.DeleteCmd(del.attrId(), getContext().getSelf())
                ));
            }
        }

        return this;
    }
    private long remainingEvents = 0;

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
