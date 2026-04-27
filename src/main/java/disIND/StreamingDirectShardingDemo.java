package disIND;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.ConfigFactory;
import disIND.streamBasedNoCentralCoordinator.actors.ValueOwnerActor;
import disIND.streamBasedNoCentralCoordinator.dataset.CSVStreamingSource;
import disIND.streamBasedNoCentralCoordinator.model.RawEvent;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class StreamingDirectShardingDemo {

    public record EntityUpdate(
            String entityId,
            Map<Short, Integer> attrCounts
    ) {}

    public static void main(String[] args) throws Exception {
        ActorSystem<Void> system =
                ActorSystem.create(Behaviors.empty(), "disIND", ConfigFactory.load());
        ClusterSharding sharding = ClusterSharding.get(system);
        EntityTypeKey<ValueOwnerActor.Command> valueKey =
                EntityTypeKey.create(
                        ValueOwnerActor.Command.class,
                        "ValueOwner"
                );

        var valueRegion =
                sharding.init(
                        Entity.of(
                                valueKey,
                                ctx -> ValueOwnerActor.create(ctx.getEntityId())
                        )
                );

        Source<RawEvent.Batch, NotUsed> source =
                CSVStreamingSource.stream("/Users/gupta/Documents/DIS-IND/data/sample_data.csv", 4);
        CompletionStage<Done> done =
                source.map(batch -> {
                            System.out.println("Batch " + batch.batchId() + " size=" + batch.events().size());
                            return aggregateBatch(batch.events());
                        }).mapAsync(4, entityUpdates -> {
                            List<CompletionStage<Done>> futures = new ArrayList<>();
                            for (EntityUpdate update : entityUpdates) {
                                CompletionStage<Done> future =
                                        AskPattern.ask(
                                                valueRegion,
                                                replyTo -> new ShardingEnvelope<>(
                                                        update.entityId(),
                                                        new ValueOwnerActor.BatchUpdate(
                                                                update.attrCounts(),
                                                                replyTo
                                                        )
                                                ),
                                                Duration.ofSeconds(5),
                                                system.scheduler()
                                        );
                                futures.add(future);
                            }
                            return CompletableFuture
                                    .allOf(
                                            futures.stream()
                                                    .map(CompletionStage::toCompletableFuture)
                                                    .toArray(CompletableFuture[]::new)
                                    )
                                    .thenApply(x -> Done.getInstance());
                        }).runWith(Sink.ignore(), system);

        done.whenComplete((ok, ex) -> {
            if (ex != null) {
                system.log().error("Stream failed", ex);
            } else {
                system.log().info("Stream completed successfully");
            }
            system.terminate();
        });

        system.getWhenTerminated().toCompletableFuture().get();
    }

    private static List<EntityUpdate> aggregateBatch(List<RawEvent> events) {

        Map<String, Map<Short, Integer>> grouped = new HashMap<>();

        for (RawEvent event : events) {
            if (event instanceof RawEvent.Insert ins) {

                long hash = hashValue(ins.valueStr());
                String entityId = Long.toUnsignedString(hash, 16);

                grouped
                        .computeIfAbsent(entityId, k -> new HashMap<>())
                        .merge(ins.attrId(), 1, Integer::sum);
            }
        }

        List<EntityUpdate> result = new ArrayList<>();

        for (Map.Entry<String, Map<Short, Integer>> e : grouped.entrySet()) {
            result.add(
                    new EntityUpdate(
                            e.getKey(),
                            Map.copyOf(e.getValue())
                    )
            );
        }
        return result;
    }

    public static long hashValue(String value) {
        if (value == null) return 0L;
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001b3L;
        }

        return hash;
    }
}