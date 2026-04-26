package disIND;

import akka.actor.typed.Props;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

import akka.cluster.sharding.typed.ShardingEnvelope;
import com.typesafe.config.ConfigFactory;
import disIND.actors.InputReaderActor;
import disIND.actors.ValueOwnerActor;
import disIND.dataset.CSVLoader;
import disIND.model.RawEvent;
import java.util.List;

import java.util.concurrent.TimeUnit;

public class DisINDMain {
    public static void main(String[] args) throws Exception {

        ActorSystem<Void> system = ActorSystem.create(
                Behaviors.empty(), "DisIND", ConfigFactory.load());

        Thread.sleep(2000);
        System.out.println(" Akka Cluster ready on 127.0.0.1:2551");

        runCSV(system);

        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        System.out.println(" System terminated.");
    }

    private static void runCSV(ActorSystem<Void> system) throws Exception {
        String path="/Users/gupta/Documents/DIS-IND/data/sample_data.csv";
        if (!CSVLoader.validate(path)) {
            System.out.println("  File not found or invalid CSV: " + path);
            return;
        }
        CSVLoader.Result r = CSVLoader.load(path, 256);
        run(system, (short) r.attrNames().length, r.attrNames(), r.batches());
    }


    private static void run(ActorSystem<Void> system, short numAttrs, String[] attrNames,
                            List<RawEvent.Batch> batches) throws Exception {

        String runId = String.valueOf(System.currentTimeMillis());

        ClusterSharding sharding = ClusterSharding.get(system);

        EntityTypeKey<ValueOwnerActor.Command> valueKey =
                EntityTypeKey.create(
                        ValueOwnerActor.Command.class,
                        "ValueOwner-" + runId
                );

        ActorRef<ShardingEnvelope<ValueOwnerActor.Command>> valueRegion =
                sharding.init(
                        Entity.of(
                                valueKey,
                                ctx -> ValueOwnerActor.create(ctx.getEntityId())
                        )
                );

        ActorRef<InputReaderActor.Command> inputReader =
                system.systemActorOf(
                        InputReaderActor.create(valueRegion),
                        "inputReader-" + runId,
                        Props.empty()
                );

        for (RawEvent.Batch batch : batches) {
            inputReader.tell(new InputReaderActor.SubmitBatch(batch));
        }

        system.getWhenTerminated().toCompletableFuture().get();

    }

}
