package disIND.streamBasedShardedDispatcher.actors;

import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

public class SketchEntityKey {
    public static final EntityTypeKey<SketchActor.Command> KEY =
            EntityTypeKey.create(SketchActor.Command.class, "SketchActor");
}
