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
import disIND.valueBased.monitor.WorkerPhaseMetrics;
import disIND.valueBased.monitor.WorkerPhaseMetrics.Phase;
import java.util.Objects;

public final class MembershipWriterActor extends AbstractBehavior<Command> {

    private final ValueOwnerMembershipStore store;
    private final WorkerPhaseMetrics phaseMetrics;
    public static Behavior<Command> create(ValueOwnerMembershipStore store, WorkerPhaseMetrics phaseMetrics) {
        return Behaviors.setup(ctx -> new MembershipWriterActor(ctx, store, phaseMetrics));
    }

    private MembershipWriterActor(ActorContext<Command> context, ValueOwnerMembershipStore store,
            WorkerPhaseMetrics phaseMetrics) {
        super(context);
        this.store = store;
        this.phaseMetrics = Objects.requireNonNull(phaseMetrics);
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
            try {
                store.writeEncodedBatch(message);
            } finally {
                phaseMetrics.record(Phase.ROCKSDB_WRITE_EXECUTION,
                        System.nanoTime() - started);
            }
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
