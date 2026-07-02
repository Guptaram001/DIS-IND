package disIND;

import akka.actor.typed.Props;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.sharding.typed.ShardingEnvelope;
import com.typesafe.config.ConfigFactory;
import disIND.prototypeModel.actors.InputReaderActor;
import disIND.prototypeModel.actors.ValueOwnerActor;
import disIND.prototypeModel.dataset.CSVLoader;
import disIND.prototypeModel.model.RawEvent;
import disIND.streamBasedShardedDispatcher.actors.INDGuardian;
import disIND.streamBasedShardedDispatcher.dataset.DataLoader;
import disIND.streamBasedShardedDispatcher.model.SharedModel;
import disIND.streamBasedShardedDispatcher.utility.UserConfig;

import java.time.Duration;
import java.util.List;

import java.util.concurrent.TimeUnit;

public class DisINDMain {
//    public static void main(String[] args) throws Exception {
//
//        ActorSystem<Void> system = ActorSystem.create(
//                Behaviors.empty(), "disIND", ConfigFactory.load());
//
//        Thread.sleep(2000);
//        System.out.println(" Akka Cluster ready on 127.0.0.1:2551");
//
//        runCSV(system);
//
//        system.terminate();
//        system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
//        System.out.println(" System terminated.");
//    }

    public static void main(String[] args) throws Exception {

        String inputDir = UserConfig.inputDir;
        int batchSize = UserConfig.BATCH_SIZE;
        int timeoutSec = 5;
        String outputFile = UserConfig.outputDir;

        INDGuardian.Config cfg = DataLoader.discoverConfig(inputDir);
        System.out.println("[Main] Discovered config: " + cfg);
        ActorSystem<SharedModel.BDCommand> system = ActorSystem.create(INDGuardian.create(cfg), "disIND", ConfigFactory.load());
        System.out.println("[Main] INDGuardian started.");

        //Direct reference to BDActor, instead of numerous calls at send batch
        ActorRef<SharedModel.BDCommand> bdRef = AskPattern.ask(
                                system,
                                SharedModel.BDCommand.GetBatchDispatcher::new,
                                Duration.ofSeconds(10),
                                system.scheduler()
                        ).toCompletableFuture()
                        .get();

        Thread.sleep(3000);

        try {
            DataLoader.run(system, cfg.metadata(),inputDir, batchSize, timeoutSec, outputFile, bdRef);
        } finally {
            System.out.println("[Main] Terminating actor system.");
            system.terminate();
            system.getWhenTerminated()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            System.out.println("[Main] System terminated.");
        }
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