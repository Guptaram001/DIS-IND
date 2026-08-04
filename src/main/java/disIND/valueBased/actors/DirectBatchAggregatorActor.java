package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.BDReply;
import disIND.valueBased.model.SharedModel.InputBatchDetails;

import java.util.HashSet;
import java.util.Set;

/**
 * Short lived 
 */
public final class DirectBatchAggregatorActor
        extends AbstractBehavior<DirectBatchAggregatorActor.Command> {

    public interface Command extends AkkaSerializable {}
    public static final EntityTypeKey<Command> TYPE_KEY =
            EntityTypeKey.create(Command.class, "DirectBatchAggregatorActor");
    public record PrepareBatch(InputBatchDetails details, int expectedOwners,
                               ActorRef<BatchHandle> replyTo) implements Command {}
    public record BatchHandle(ActorRef<Command> aggregator) implements AkkaSerializable {}
    public record AwaitCompletion(ActorRef<BDReply> replyTo) implements Command {}
    public record ValueOwnerPersisted(int bucketId) implements Command {}

    private InputBatchDetails batchDetails;
    private int remainingOwners = -1;
    private final Set<Integer> persistedOwners = new HashSet<>();
    private ActorRef<BDReply> replyTo;
    private boolean completed;

    public static Behavior<Command> create() {
        return Behaviors.setup(DirectBatchAggregatorActor::new);
    }

    private DirectBatchAggregatorActor(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PrepareBatch.class, this::onPrepareBatch)
                .onMessage(AwaitCompletion.class, this::onAwaitCompletion)
                .onMessage(ValueOwnerPersisted.class, this::onValueOwnerPersisted)
                .build();
    }

    private Behavior<Command> onPrepareBatch(PrepareBatch msg) {
        if (batchDetails != null)
            return this;
        batchDetails = msg.details();
        remainingOwners = msg.expectedOwners();
        msg.replyTo().tell(new BatchHandle(getContext().getSelf()));
        remainingOwners -= persistedOwners.size();
        completed = remainingOwners <= 0;
        return finishIfReady();
    }

    private Behavior<Command> onAwaitCompletion(AwaitCompletion msg) {
        replyTo = msg.replyTo();
        return finishIfReady();
    }

    private Behavior<Command> onValueOwnerPersisted(ValueOwnerPersisted msg) {
        if (!persistedOwners.add(msg.bucketId()))
            return this;
        if (batchDetails != null && --remainingOwners <= 0) {
            completed = true;
            return finishIfReady();
        }
        return this;
    }

    private Behavior<Command> finishIfReady() {
        if (!completed || replyTo == null)
            return this;
        replyTo.tell(new BDReply.BatchAccepted(batchDetails.epoch()));
        return Behaviors.stopped();
    }
}
