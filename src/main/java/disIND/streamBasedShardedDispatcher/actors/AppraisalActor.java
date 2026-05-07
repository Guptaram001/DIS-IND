package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AppraisalActor extends AbstractBehavior<AppraisalActor.Command> {

    public interface Command {}

    public record SketchSummary(
            long attrId,
            long distinctCount,
            Set<String> topK,
            long sketchSize
    ) {}

    public record UpdateSummary(
            SketchSummary summary
    ) implements Command {}

    private final Map<Long, SketchSummary> summaries =
            new HashMap<>();

    public static Behavior<Command> create() {
        return Behaviors.setup(AppraisalActor::new);
    }

    private AppraisalActor(ActorContext<Command> ctx) {
        super(ctx);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(UpdateSummary.class, this::onSummary)
                .build();
    }

    private Behavior<Command> onSummary(UpdateSummary cmd) {

        summaries.put(cmd.summary().attrId(), cmd.summary());

        getContext().getLog().info(
                "Received sketch summary attr={} distinct={}",
                cmd.summary().attrId(),
                cmd.summary().distinctCount()
        );

        // candidate generation logic here

        return this;
    }
}
