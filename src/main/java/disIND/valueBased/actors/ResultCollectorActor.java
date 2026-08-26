package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.valueBased.model.SharedModel.*;
import disIND.valueBased.utility.Debug;
import disIND.valueBased.monitor.ResultMetricsWriter;
import java.util.*;

import static disIND.valueBased.utility.Debug.formLog;

public class ResultCollectorActor extends AbstractBehavior<RCCommand> {
    private final DatasetMetadata metadata;
    private final BitSet finishedCms;
    private ActorRef<BDReply> finishReplyTo;
    private int finalRound = -1;
    private boolean discoveryFinished = false;
    private final Map<Integer, List<UnaryPair>> unaryResults = new HashMap<>();
    private final Map<Integer, List<NaryPair>> naryResults = new HashMap<>();
    private final List<ActorRef<IndReport>> pendingReportReplies = new ArrayList<>();
    private long exactComparisonsWithoutPruning;
    private long candidateEvaluationsWithoutPruning;
    private PruneMetrics pruneMetrics = PruneMetrics.empty();
    private long activeClusterEntriesAcrossBuckets;
    private final Set<BitSet> distinctActiveClusterSignatures = new HashSet<>();
    private final ResultMetricsWriter metricsWriter;

    public static Behavior<RCCommand> create(DatasetMetadata metadata) {
        return Behaviors.setup(ctx -> new ResultCollectorActor(ctx, metadata));
    }

    private ResultCollectorActor(ActorContext<RCCommand> ctx, DatasetMetadata metadata) {
        super(ctx);
        this.metadata = metadata;
        this.finishedCms = new BitSet(metadata.totalCols());
        this.metricsWriter = new ResultMetricsWriter(ctx.getLog());

    }

    @Override
    public Receive<RCCommand> createReceive() {
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
        exactComparisonsWithoutPruning = Math.addExact(
                exactComparisonsWithoutPruning, msg.exactValueProbesWithoutPruning());
        candidateEvaluationsWithoutPruning = Math.addExact(
                candidateEvaluationsWithoutPruning,
                msg.candidateEvaluationsWithoutPruning());
        pruneMetrics = pruneMetrics.plus(msg.pruneMetrics());
        activeClusterEntriesAcrossBuckets = Math.addExact(activeClusterEntriesAcrossBuckets,
                msg.activeClusterEntriesAcrossBuckets());
        for (long[] words : msg.distinctActiveClusterSignatures())
            distinctActiveClusterSignatures.add(BitSet.valueOf(words));

        if (Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.rc(),
                    -1, "", String.valueOf(Debug.State.NONE),
                    " Received CM {} unary={} nary={} ",
                    msg.lhsOwnerCol(), msg.unaryPairs().size(), msg.naryPairs().size());
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
        if (discoveryFinished)
            return;
        if (finishReplyTo == null)
            return;

        if (finishedCms.cardinality() < metadata.totalCols())
            return;

        discoveryFinished = true;
        metricsWriter.writeAll(candidateEvaluationsWithoutPruning, exactComparisonsWithoutPruning,
                finalRound, pruneMetrics, activeClusterEntriesAcrossBuckets, distinctActiveClusterSignatures.size());
        finishReplyTo.tell(new BDReply.DiscoveryFinished(finalRound));
        IndReport report = buildReport();
        for (ActorRef<IndReport> replyTo : pendingReportReplies)
            replyTo.tell(report);
        pendingReportReplies.clear();

        if (Debug.STATE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.STATE), Debug.rc(),
                    -1, "", String.valueOf(Debug.State.NONE),
                    "Discovery finished for finalRound={} confirmedCms={}/{} ",
                    finalRound, finishedCms.cardinality(), metadata.totalCols());
    }

}
