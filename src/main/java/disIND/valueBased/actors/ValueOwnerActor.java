package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command extends AkkaSerializable {}

  

    private final String entityId;
    private final BitSet attrsContainingValue = new BitSet();
    private final Map<Short, Integer> counts = new HashMap<>();

    public static Behavior<Command> create(String entityId) {
        return Behaviors.setup(ctx -> new ValueOwnerActor(ctx, entityId));
    }

    private ValueOwnerActor(ActorContext<Command> ctx, String entityId) {
        super(ctx);
        this.entityId = entityId;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .build();
    }

}