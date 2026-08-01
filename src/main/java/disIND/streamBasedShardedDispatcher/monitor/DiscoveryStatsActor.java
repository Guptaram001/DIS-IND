package disIND.streamBasedShardedDispatcher.monitor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class DiscoveryStatsActor extends AbstractBehavior<StatsCommand> {
    private long totalRows;
    private long totalBatches;

    private long unaryRebuilds;
    private long naryRebuilds;

    private long indsFound;

    private long candidatesCreated;
    private long candidatesRemoved;
    private final Map<Integer, StatsCommand.AttributeStats> attributeStats = new HashMap<>();

    public static Behavior<StatsCommand> create() {
        return Behaviors.setup(ctx -> Behaviors.withTimers(
                timers -> {timers.startTimerAtFixedRate(new StatsCommand.PrintStats(),
                        Duration.ofSeconds(30));
                    return new DiscoveryStatsActor(ctx);
                })
        );
    }
    private DiscoveryStatsActor(ActorContext<StatsCommand> ctx) {
        super(ctx);
    }

    @Override
    public Receive<StatsCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(StatsCommand.RowBatchProcessed.class, this::onBatch)
                .onMessage(StatsCommand.CandidateCreated.class, msg -> {
                    candidatesCreated++;
                    return this;
                })
                .onMessage(StatsCommand.CandidateRemoved.class, msg -> {
                    candidatesRemoved++;
                    return this;
                })
                .onMessage(StatsCommand.UnaryRebuild.class, msg -> {
                    unaryRebuilds++;
                    return this;
                })
                .onMessage(StatsCommand.NaryRebuild.class, msg -> {
                    naryRebuilds++;
                    return this;
                })
                .onMessage(StatsCommand.IndDiscovered.class, msg -> {
                    indsFound++;
                    return this;
                })
                .onMessage(StatsCommand.AttributeStats.class, msg -> {
                    attributeStats.put(msg.colId(), msg);
                    return this;
                })
                .onMessage(StatsCommand.PrintStats.class, this::onPrint)
                .build();
    }

    private Behavior<StatsCommand> onBatch(StatsCommand.RowBatchProcessed msg) {
        totalRows += msg.rows();
        totalBatches++;
        return this;
    }

    private Behavior<StatsCommand> onPrint(StatsCommand.PrintStats msg) {
        long activeCandidates = candidatesCreated - candidatesRemoved;
        getContext().getLog().info("Rows: {}, Batches: {}, Active Cands: {}, Unary Rebuilds : {}, Nary Rebuilds  : {}, " +
                "INDs Found: {} ", totalRows, totalBatches, activeCandidates, unaryRebuilds, naryRebuilds, indsFound);
        getContext().getLog().info("Attribute stats received for {} columns", attributeStats.size());

        attributeStats.values().stream()
                .sorted(Comparator.comparingInt(StatsCommand.AttributeStats::colId))
                .limit(10)
                .forEach(s ->
                        getContext().getLog().info("col={} distinct={} bitmapCard={} sketchCard={}",
                                s.colId(), s.distinctValues(), s.bitmapCardinality(), s.sketchCardinality()));
        return this;
    }
}