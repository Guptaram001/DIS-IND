package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;

import javax.xml.crypto.Data;
import java.util.*;

public class LatticeManagerActor extends AbstractBehavior<LMCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final DatasetMetadata metadata;
    private final int   maxArity;
    private final int   maxConcurrent;
    private final int[] tableOffsets;
    private final Map<Integer, ColType> colTypeMap;
    private int credits;

    public static Behavior<LMCommand> create(int maxArity, int maxConcurrent, List<Integer> tableOffsets,
                                             Map<Integer, ColType> colTypes, DatasetMetadata metadata,
                                             ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new LatticeManagerActor(ctx, maxArity, maxConcurrent,
                        tableOffsets, colTypes,metadata,statsRef));
    }

    private LatticeManagerActor(ActorContext<LMCommand> ctx, int maxArity, int maxConcurrent,
                                List<Integer> tableOffsets, Map<Integer, ColType> colTypes,
                                DatasetMetadata metadata,ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.maxArity      = maxArity;
        this.maxConcurrent = maxConcurrent;
        this.credits       = maxConcurrent;
        this.tableOffsets  = tableOffsets.stream().mapToInt(Integer::intValue).toArray();
        this.colTypeMap    = new HashMap<>(colTypes);
        this.metadata      = metadata;
        this.statsRef      = statsRef;
    }


    @Override
    public Receive<LMCommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
