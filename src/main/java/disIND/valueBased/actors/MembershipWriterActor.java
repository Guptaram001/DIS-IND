package disIND.valueBased.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.valueBased.protocol.MembershipWriteProtocol;
import disIND.valueBased.protocol.MembershipWriteProtocol.Command;
import disIND.valueBased.protocol.MembershipWriteProtocol.StagedWrite;
import disIND.valueBased.structures.ValueOwnerMembershipStore;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.utility.UserConfig;

import java.time.Duration;

public final class MembershipWriterActor extends AbstractBehavior<Command> {

    private final ValueOwnerMembershipStore store;
    private final int flushThreshold;

    public static Behavior<Command> create(ValueOwnerMembershipStore store) {
        return Behaviors.withTimers(timers -> Behaviors.setup(ctx -> {
            timers.startTimerAtFixedRate(MembershipWriteProtocol.FlushTick.INSTANCE,
                    Duration.ofMillis(UserConfig.DEFAULT_VO_WRITE_OVERLAY_FLUSH_INTERVAL_MS));
            return new MembershipWriterActor(ctx, store, UserConfig.DEFAULT_VO_WRITE_OVERLAY_FLUSH_THRESHOLD);
        }));
    }

    private MembershipWriterActor(ActorContext<Command> context, ValueOwnerMembershipStore store,
            int flushThreshold) {
        super(context);
        this.store = store;
        this.flushThreshold = flushThreshold;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StagedWrite.class, this::onStagedWrite)
                .onMessageEquals(MembershipWriteProtocol.FlushTick.INSTANCE, this::onFlushTick)
                .build();
    }

    private Behavior<Command> onStagedWrite(StagedWrite message) {
        if (store.pendingOverlaySize() >= flushThreshold)
            flush("threshold");
        return this;
    }

    private Behavior<Command> onFlushTick() {
        flush("tick");
        return this;
    }

    private void flush(String reason) {
        if (store.pendingOverlaySize() == 0)
            return;
        long started = System.nanoTime();
        int approxEntries = store.pendingOverlaySize();
        store.flushOverlay();
        if (Debug.INTERNAL) {
            getContext().getLog().info("[VO-WRITER] reason={} approxEntries={} tookMicros={}",
                    reason, approxEntries, (System.nanoTime() - started) / 1000);
        }
    }
}
