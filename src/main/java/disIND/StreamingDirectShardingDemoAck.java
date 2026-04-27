package disIND;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.ConfigFactory;
import disIND.streamBasedNoCentralCoordinatorWithAck.actors.BatchDispatcherActor;
import disIND.streamBasedNoCentralCoordinatorWithAck.actors.ValueOwnerActor;
import disIND.streamBasedNoCentralCoordinatorWithAck.dataset.CSVStreamingSource;
import disIND.streamBasedNoCentralCoordinatorWithAck.model.RawEvent;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class StreamingDirectShardingDemoAck {

    public static void main(String[] args) throws Exception {

        ActorSystem<Void> system =
                ActorSystem.create(
                        Behaviors.empty(),
                        "disIND",
                        ConfigFactory.load()
                );

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

        ActorRef<BatchDispatcherActor.Command> dispatcher =
                system.systemActorOf(
                        BatchDispatcherActor.create(valueRegion),
                        "batch-dispatcher",
                        Props.empty()
                );

        Source<RawEvent.Batch, NotUsed> source =
                CSVStreamingSource.stream(
                        "/Users/gupta/Documents/DIS-IND/data/sample_data.csv",
                        4
                );

        CompletionStage<Done> done =
                source
                        .mapAsync(4, batch ->
                                AskPattern.<BatchDispatcherActor.Command, Done>ask(
                                        dispatcher,
                                        replyTo -> new BatchDispatcherActor.ProcessBatch(batch, replyTo),
                                        Duration.ofSeconds(30),
                                        system.scheduler()
                                )
                        )
                        .runWith(Sink.ignore(), system);

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
}