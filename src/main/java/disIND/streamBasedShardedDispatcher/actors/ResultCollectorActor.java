package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.utility.Debug;

import java.util.*;

import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;

public class ResultCollectorActor extends AbstractBehavior<RCCommand> {
    private final ActorRef<StatsCommand> statsRef;
    private final DatasetMetadata metadata;
    private final BitSet finishedCms;
    private ActorRef<BDReply> finishReplyTo;
    private int finalRound = -1;
    private boolean discoveryFinished = false;
    private final Map<Integer,List<UnaryPair>> unaryResults = new HashMap<>();
    private final Map<Integer,List<NaryPair>> naryResults = new HashMap<>();
    private final List<ActorRef<IndReport>> pendingReportReplies = new ArrayList<>();
    public static Behavior<RCCommand> create( DatasetMetadata metadata, ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new ResultCollectorActor(ctx,metadata,statsRef));
    }

    private ResultCollectorActor(ActorContext<RCCommand> ctx, DatasetMetadata metadata, ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.metadata = metadata;
        this.statsRef = statsRef;
        this.finishedCms = new BitSet(metadata.totalCols());
    }

    @Override
    public Receive createReceive() {
        return newReceiveBuilder()
                .onMessage(RCCommand.AwaitDiscoveryFinished.class, this::onAwaitDiscoveryFinished)
                .onMessage(RCCommand.CmDiscoveryComplete.class, this::onCmDiscoveryComplete)
                .onMessage(RCCommand.GetReport.class, this::onGetReport)
                .build();
    }

    private Behavior<RCCommand> onCmDiscoveryComplete(RCCommand.CmDiscoveryComplete msg) {
        unaryResults.put(msg.lhsOwnerCol(), msg.unaryPairs());
        naryResults.put(msg.lhsOwnerCol(), msg.naryPairs());
        finishedCms.set(msg.lhsOwnerCol());

        if (Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.rc(),
                    -1, "", String.valueOf(Debug.State.NONE),
                    " Received CM {} unary={} nary={} ",
                    msg.lhsOwnerCol(),msg.unaryPairs().size(),msg.naryPairs().size());
        tryFinishDiscovery();
        return this;
    }

    private IndReport buildReport() {
        List<UnaryPair> allUnary = new ArrayList<>();
        List<NaryPair> allNary = new ArrayList<>();
        for (List<UnaryPair> list : unaryResults.values())
            allUnary.addAll(list);
        for (List<NaryPair> list : naryResults.values())
            allNary.addAll(list);

        allUnary.sort(Comparator.comparingInt(UnaryPair::lhsCol).thenComparingInt(UnaryPair::rhsCol));

        Map<Integer, String> names = new HashMap<>();
        for (int col = 0; col < metadata.totalCols(); col++)
            names.put(col, metadata.qualifiedName(col));

        return new IndReport(allUnary, allNary, names, finalRound);
    }

    private Behavior<RCCommand> onGetReport(RCCommand.GetReport msg) {
        if (!discoveryFinished) {
            pendingReportReplies.add(msg.replyTo());
            return this;
        }
        msg.replyTo().tell(buildReport());
        return this;
    }

    private Behavior<RCCommand> onAwaitDiscoveryFinished(RCCommand.AwaitDiscoveryFinished msg) {
        this.finalRound = msg.finalRound();
        this.finishReplyTo = msg.replyTo();
        tryFinishDiscovery();
        return this;
    }

    private void tryFinishDiscovery() {
        if (discoveryFinished) return;
        if (finishReplyTo == null) return;

        if (finishedCms.cardinality() < metadata.totalCols())
            return;

        discoveryFinished = true;
        finishReplyTo.tell(new BDReply.DiscoveryFinished(finalRound));
        IndReport report = buildReport();
        for (ActorRef<IndReport> replyTo : pendingReportReplies)
            replyTo.tell(report);
        pendingReportReplies.clear();

        if (Debug.STATE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.STATE), Debug.rc(),
                    -1, "", String.valueOf(Debug.State.NONE),
                    "Discovery finished for finalRound={} confirmedCms={}/{} ",
                    finalRound, finishedCms.cardinality(),metadata.totalCols());
    }

}
