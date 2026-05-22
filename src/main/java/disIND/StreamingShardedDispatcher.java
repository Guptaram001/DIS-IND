package disIND;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.ConfigFactory;
import disIND.streamBasedShardedDispatcher.actors.*;
import disIND.streamBasedShardedDispatcher.dataset.CSVStreamingSource;
import disIND.streamBasedShardedDispatcher.model.RawEvent;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class StreamingShardedDispatcher {

    private static final int NUM_DISPATCHERS = 8;

    public static void main(String[] args) throws Exception {

        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "disIND", ConfigFactory.load());

        //ActorRef<CandidateManagerActor.Command> candidateManager = system.systemActorOf(CandidateManagerActor.create(), "candidate-manager",akka.actor.typed.Props.empty());
        //ActorRef<AppraisalActor.Command> appraisalActor = system.systemActorOf(AppraisalActor.create(candidateManager), "appraisal-actor",akka.actor.typed.Props.empty());

        ClusterSharding sharding = ClusterSharding.get(system);

        EntityTypeKey<CandidateManagerActor.Command> candidateKey = EntityTypeKey.create(CandidateManagerActor.Command.class, "CandidateManager");
        var candidateRegion = sharding.init(Entity.of(candidateKey,ctx -> CandidateManagerActor.create(ctx.getEntityId())));


        //EntityTypeKey<AppraisalActor.Command> appraisalKey = EntityTypeKey.create(AppraisalActor.Command.class, "AppraisalActor");
        //var appraisalRegion = sharding.init(Entity.of(appraisalKey,ctx -> AppraisalActor.create(candidateRegion)));
        ActorRef<AppraisalActor.Command> appraisalActor = system.systemActorOf(AppraisalActor.create(candidateRegion), "appraisal-actor",akka.actor.typed.Props.empty());

        EntityTypeKey<ValueOwnerActor.Command> valueKey = EntityTypeKey.create(ValueOwnerActor.Command.class, "ValueOwner");
        var valueRegion = sharding.init(Entity.of(valueKey,ctx -> ValueOwnerActor.create(ctx.getEntityId(),
                candidateRegion)));

        EntityTypeKey<SketchActor.Command> sketchKey =EntityTypeKey.create(SketchActor.Command.class, "SketchActor");
        //var sketchRegion = sharding.init(Entity.of(sketchKey, ctx -> SketchActor.create(ctx.getEntityId(), appraisalRegion)));
        var sketchRegion = sharding.init(Entity.of(sketchKey, ctx -> SketchActor.create(ctx.getEntityId(), appraisalActor)));


        EntityTypeKey<BatchDispatcherActor.Command> dispatcherKey = EntityTypeKey.create(BatchDispatcherActor.Command.class, "BatchDispatcher");
        var dispatcherRegion = sharding.init(Entity.of(dispatcherKey, ctx -> BatchDispatcherActor.create(
                ctx.getEntityId(),
                valueRegion,
                sketchRegion)));

        Source<RawEvent.Batch, NotUsed> source = CSVStreamingSource.stream("/Users/gupta/Documents/DIS-IND/data/sample_data.csv", 4);

        CompletionStage<Done> done = source
                        .mapAsync(4, batch -> {
                            String dispatcherId =
                                    String.valueOf(batch.batchId() % NUM_DISPATCHERS);

                            return AskPattern.<
                                    ShardingEnvelope<BatchDispatcherActor.Command>,
                                    Done
                                    >ask(
                                    dispatcherRegion,
                                    replyTo -> new ShardingEnvelope<>(
                                            dispatcherId,
                                            new BatchDispatcherActor.ProcessBatch(
                                                    batch,
                                                    replyTo
                                            )
                                    ),
                                    Duration.ofSeconds(30),
                                    system.scheduler()
                            );
                        })
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