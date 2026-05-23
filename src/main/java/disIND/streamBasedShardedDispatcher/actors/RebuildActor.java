package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;
import org.roaringbitmap.RoaringBitmap;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;



public class RebuildActor extends AbstractBehavior<RebuildActor.Command> {

    public  interface Command extends AkkaSerializable{};
    public record BitmapResponse(RoaringBitmap bitmap) implements AkkaSerializable {}

    private record WrappedRhsBitmap(
            RoaringBitmap bitmap,
            short lhs,
            short rhs,
            ActorRef<CandidateCheckResult> replyTo
    ) implements Command {}

    public record CandidateCheckResult(
            short lhs,
            short rhs,
            int violationCount,
            int witnessValue
    ) implements AkkaSerializable {}

    public record UpdateMembership(
            int valueId,
            boolean add
    ) implements Command {}
    public record CheckCandidate(
            short lhs,
            short rhs,
            ActorRef<CandidateCheckResult> replyTo
    ) implements Command {}

    public record GetBitmap(ActorRef<BitmapResponse> replyTo) implements Command {}

    private final RoaringBitmap bitmap = new RoaringBitmap();
    private final String entityId;
    private final ClusterSharding sharding;
    private ActorRef<ShardingEnvelope<CandidateManagerActor.Command>> candidateRegion;

    public RebuildActor(ActorContext<RebuildActor.Command> context, String entityId) {
        super(context);
        this.entityId = entityId;
        this.sharding = ClusterSharding.get(context.getSystem());
    }

    public static Behavior<RebuildActor.Command> create(String entityId) {
        return Behaviors.setup(ctx -> new RebuildActor(ctx, entityId));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(UpdateMembership.class, this::onUpdateMembership)
                .onMessage(CheckCandidate.class, this::onCheckCandidate)
                .onMessage(GetBitmap.class, this::onGetBitmap)
                .onMessage(WrappedRhsBitmap.class, this::onWrappedRhsBitmap)
                .build();
    }

    private Behavior<Command> onUpdateMembership(UpdateMembership cmd) {
        if (cmd.add()) {
            bitmap.add(cmd.valueId());
        } else {
            bitmap.remove(cmd.valueId());
        }
        return this;
    }
    private Behavior<Command> onCheckCandidate(CheckCandidate cmd) {
        EntityTypeKey<Command> key =
                EntityTypeKey.create(
                        Command.class,
                        "RebuildActor"
                );

        EntityRef<Command> rhsRef =
                sharding.entityRefFor(
                        key,
                        String.valueOf(cmd.rhs())
                );

        ActorRef<BitmapResponse> adapter =
                getContext().messageAdapter(
                        BitmapResponse.class,
                        msg ->
                                new WrappedRhsBitmap(
                                        msg.bitmap(),
                                        cmd.lhs(),
                                        cmd.rhs(),
                                        cmd.replyTo()
                                )
                );

        rhsRef.tell(new GetBitmap(adapter));
        return this;
    }

    private Behavior<Command> onGetBitmap(GetBitmap cmd) {
        cmd.replyTo().tell(new BitmapResponse((RoaringBitmap) bitmap.clone()));
        return this;
    }

    private Behavior<Command> onWrappedRhsBitmap(WrappedRhsBitmap msg) {

        RoaringBitmap violations = RoaringBitmap.andNot(bitmap, msg.bitmap());
        int count = violations.getCardinality();
        int witness = violations.isEmpty() ? -1 : violations.first();

        msg.replyTo().tell(new CandidateCheckResult(
                        msg.lhs(),
                        msg.rhs(),
                        count,
                        witness
                )
        );
        return this;
    }
}
