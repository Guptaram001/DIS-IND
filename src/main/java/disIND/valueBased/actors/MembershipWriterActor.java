package disIND.valueBased.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.valueBased.protocol.MembershipWriteProtocol.Command;
import disIND.valueBased.protocol.MembershipWriteProtocol.EncodedWriteBatch;
import disIND.valueBased.protocol.ValueOwnerProtocol.MembershipWriteAcknowledged;
import disIND.valueBased.protocol.ValueOwnerProtocol.MembershipWriteFailed;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.utility.Debug;

public final class MembershipWriterActor extends AbstractBehavior<Command> {

    private final ValueOwnerMembershipStore store;
    public static Behavior<Command> create(ValueOwnerMembershipStore store) {
        return Behaviors.setup(ctx -> new MembershipWriterActor(ctx, store));
    }

    private MembershipWriterActor(ActorContext<Command> context, ValueOwnerMembershipStore store) {
        super(context);
        this.store = store;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(EncodedWriteBatch.class, this::onEncodedWriteBatch)
                .build();
    }

    private Behavior<Command> onEncodedWriteBatch(EncodedWriteBatch message) {
        long started = System.nanoTime();
        try {
            store.writeEncodedBatch(message);
            message.replyTo().tell(new MembershipWriteAcknowledged(message.bucketId(), message.batchId()));
            if (Debug.INTERNAL) {
                getContext().getLog().info(
                        "[VO-WRITER] bucket={} batchId={} entries={} bytes={} tookMicros={}",
                        message.bucketId(), message.batchId(),
                        message.membershipValueIds().length + message.candidateWrites().length,
                        message.encodedBytes(), (System.nanoTime() - started) / 1000);
            }
        } catch (RuntimeException exception) {
            getContext().getLog().error("Unable to persist VO batch bucket={} batchId={}",
                    message.bucketId(), message.batchId(), exception);
            String reason = exception.getMessage() == null
                    ? exception.getClass().getSimpleName() : exception.getMessage();
            message.replyTo().tell(new MembershipWriteFailed(message.bucketId(), message.batchId(), reason));
        }
        return this;
    }
}
