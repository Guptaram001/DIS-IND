package disIND.valueBased.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import disIND.valueBased.model.SharedModel.*;
import disIND.valueBased.monitor.StatsCommand;
import disIND.valueBased.utility.Debug;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static disIND.valueBased.utility.Debug.formLog;

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
    private final Path comparisonMetricsFile;
    private final Path pruneMetricsFile;
    private long exactComparisonsWithoutPruning;
    private long candidateEvaluationsWithoutPruning;
    private PruneMetrics pruneMetrics = PruneMetrics.empty();
    public static Behavior<RCCommand> create( DatasetMetadata metadata, ActorRef<StatsCommand> statsRef) {
        return Behaviors.setup(ctx -> new ResultCollectorActor(ctx,metadata,statsRef));
    }

    private ResultCollectorActor(ActorContext<RCCommand> ctx, DatasetMetadata metadata, ActorRef<StatsCommand> statsRef) {
        super(ctx);
        this.metadata = metadata;
        this.statsRef = statsRef;
        this.finishedCms = new BitSet(metadata.totalCols());
        this.comparisonMetricsFile = Path.of(
                System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR", "diagnostics"),
                "comparisons-without-pruning.tsv");
        this.pruneMetricsFile = Path.of(
                System.getenv().getOrDefault("DIS_IND_DIAGNOSTICS_DIR", "diagnostics"),
                "prune-metrics.tsv");
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
        exactComparisonsWithoutPruning = Math.addExact(
                exactComparisonsWithoutPruning, msg.exactValueProbesWithoutPruning());
        candidateEvaluationsWithoutPruning = Math.addExact(
                candidateEvaluationsWithoutPruning,
                msg.candidateEvaluationsWithoutPruning());
        pruneMetrics = pruneMetrics.plus(msg.pruneMetrics());

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
        writeComparisonMetrics();
        writePruneMetrics();
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

    private void writeComparisonMetrics() {
        String contents = "metric\tcount\n"
                + "candidate_evaluations_without_pruning\t"
                + candidateEvaluationsWithoutPruning + "\n"
                + "exact_value_probes_without_pruning\t"
                + exactComparisonsWithoutPruning + "\n"
                + "final_round\t" + finalRound + "\n";
        try {
            Files.createDirectories(comparisonMetricsFile.getParent());
            Files.writeString(comparisonMetricsFile, contents, StandardCharsets.UTF_8);
            getContext().getLog().info(
                    "[NO-PRUNING-METRICS] candidateEvaluations={} exactValueProbes={} writtenTo={}",
                    candidateEvaluationsWithoutPruning, exactComparisonsWithoutPruning,
                    comparisonMetricsFile.toAbsolutePath());
        } catch (IOException exception) {
            getContext().getLog().error(
                    "Unable to write no-pruning comparison metrics to {}",
                    comparisonMetricsFile, exception);
        }
    }

    private void writePruneMetrics() {
        long filterPruned = Math.addExact(pruneMetrics.wholeCountPruned(),
                Math.addExact(pruneMetrics.partitionCountPruned(), pruneMetrics.cqfPruned()));
        String contents = "metric\tcount\tunit\n"
                + "invalid_lhs_skips\t" + pruneMetrics.invalidLhsSkips() + "\tcandidate_value_checks\n"
                + "valid_rhs_skips\t" + pruneMetrics.validRhsSkips() + "\tcandidate_value_checks\n"
                + "same_batch_skips\t" + pruneMetrics.sameBatchSkips() + "\tcandidate_value_checks\n"
                + "direct_lhs_rejections\t" + pruneMetrics.directLhsRejections() + "\tcandidate_batch_events\n"
                + "whole_count_pruned\t" + pruneMetrics.wholeCountPruned() + "\tcandidates\n"
                + "partition_count_pruned\t" + pruneMetrics.partitionCountPruned() + "\tcandidates\n"
                + "cqf_pruned\t" + pruneMetrics.cqfPruned() + "\tcandidates\n"
                + "filter_pruned_total\t" + filterPruned + "\tcandidates\n"
                + "exact_tested\t" + pruneMetrics.exactTested() + "\tcandidates\n"
                + "exact_rejected\t" + pruneMetrics.exactRejected() + "\tcandidates\n"
                + "exact_validated\t" + pruneMetrics.exactValidated() + "\tcandidates\n";
        try {
            Files.createDirectories(pruneMetricsFile.getParent());
            Files.writeString(pruneMetricsFile, contents, StandardCharsets.UTF_8);
            getContext().getLog().info("[PRUNE-METRICS] writtenTo={}",
                    pruneMetricsFile.toAbsolutePath());
        } catch (IOException exception) {
            getContext().getLog().error("Unable to write prune metrics to {}",
                    pruneMetricsFile, exception);
        }
    }

}
