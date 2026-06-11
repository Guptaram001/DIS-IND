package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;

import java.util.HashMap;
import java.util.Map;

public class RebuildActor_ extends AbstractBehavior<RACommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final DatasetMetadata metadata;
    private final ClusterSharding sharding;
    private ActorRef<CMCommand> cmRef;
    private final ActorRef<DeltaScanResult> deltaAdapter;
    private final ActorRef<BitmapAtEpoch> bitmapAdapter;

    public static Behavior<RACommand> create(ClusterSharding sharding, DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new RebuildActor_(ctx, sharding,metadata,statsRef));
    }

    private RebuildActor_(ActorContext<RACommand> ctx, ClusterSharding sharding, DatasetMetadata metadata,
                          ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.sharding = sharding;
        this.cmRef = null;
        this.deltaAdapter = ctx.messageAdapter(DeltaScanResult.class, RACommand.DeltaScanReady::new);
        this.bitmapAdapter = ctx.messageAdapter(BitmapAtEpoch.class, bm ->
                new RACommand.BitmapReady(bm.requestId(), bm));
        this.metadata = metadata;
        this.statsRef = statsRef;
    }

    @Override
    public Receive<RACommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
