package disIND.valueBased.model;

import akka.actor.typed.ActorRef;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class SharedModel {

    private SharedModel() {
    }

    public enum ColType implements AkkaSerializable {
        INTEGER, DECIMAL, DATE, BOOLEAN, STRING, UNKNOWN
    }

    public enum PairState {
        ACTIVE, INACTIVE
    }

    public enum DataOrientation implements AkkaSerializable {
        VALUE_MAJOR, COLUMN_MAJOR
    }

    public enum CandidateTrackingMode implements AkkaSerializable {
        COUNT, WITNESS, PRUNE, EXACT
    }

    public sealed interface MembershipUpdates permits ValueUpdates, ColumnUpdates {
    }

    // valueId -> columnId ->count
    public record ValueUpdates(Int2ObjectMap<Int2IntMap> byValue) implements MembershipUpdates {
    }

    // columnId -> valueId -> count
    public record ColumnUpdates(Map<Integer, Int2IntMap> byValue) implements MembershipUpdates {
    }

    public record UnaryPair(int lhsCol, int rhsCol) implements AkkaSerializable {
    }

    public record NaryPair(List<Integer> lhsCols, List<Integer> rhsCols) implements AkkaSerializable {
        public int arity() {
            return lhsCols.size();
        }

        public NaryPair canonical() {
            List<int[]> z = new ArrayList<>();
            for (int i = 0; i < lhsCols.size(); i++)
                z.add(new int[] { lhsCols.get(i), rhsCols.get(i) });
            z.sort(Comparator.comparingInt(a -> a[0]));
            List<Integer> l = new ArrayList<>(), r = new ArrayList<>();
            z.forEach(p -> {
                l.add(p[0]);
                r.add(p[1]);
            });
            return new NaryPair(l, r);
        }
    }

    public record DatasetMetadata(int totalCols, List<Integer> offsets, List<Integer> nCols,
            List<String> tableNames, Map<Integer, ColumnInfo> columns) {

        public ColumnInfo column(int globalId) {
            return columns.get(globalId);
        }

        public String qualifiedName(int globalColumnId) {
            ColumnInfo column = columns().get(globalColumnId);
            return column.tableName() + "[" + column.localColumnId() + "]";
        }

        public int tableOf(int globalId) {
            return columns.get(globalId).tableId();
        }

        public ColType typeOf(int globalId) {
            return columns.get(globalId).type();
        }

        public String displayName(int globalCol) {
            return columns().get(globalCol).displayName();
        }
    }

    public record ScanResult(UnaryPair pair, int violationCount, List<Integer> witnesses, RoaringBitmap violationBitmap,
            int round) implements AkkaSerializable {
    }

    public record NaryCheckResult(NaryPair pair, int violationCount, int totalLhs, List<String> witnesses,
            long evalEpoch) implements AkkaSerializable {
    }

    public record ColumnInfo(int globalId, int tableId, String tableName, int localColumnId,
            String columnName, ColType type) {

        public String qualifiedName() {
            return tableName + "." + columnName;
        }

        public String displayName() {
            return tableName + "[" + localColumnId + "]";
        }
    }

    public record InputBatchDetails(int tableId, int startRowId, int batchId, int epoch, int round, int colId) {
    }

    public record ColumnBatch(int colId, long[] rowIds, int[] valueIds) implements AkkaSerializable {
        public ColumnBatch {
            Objects.requireNonNull(rowIds, "rowIds");
            Objects.requireNonNull(valueIds, "valueIds");
            if (rowIds.length != valueIds.length) {
                throw new IllegalArgumentException("rowIds and valueIds must have the same length");
            }
        }
    }

    public record IngestionResult(long totalRows, long totalBatches, long totalCells, int finalRound,
            Map<Integer, Integer> finalBatchByTable) {
    }

    public record IngestionReady() implements AkkaSerializable {
    }

    public record ColumnSlice(
            int colId,
            Map<Integer, RoaringBitmap> valToRows,
            long epoch) implements AkkaSerializable {
    }

    public record CandidateLocalStatus(int rhsCol, boolean valid) implements AkkaSerializable {
    }

    public record PruneMetrics(long invalidLhsSkips, long validRhsSkips, long sameBatchSkips, long directLhsRejections,
            long wholeCountPruned, long partitionCountPruned, long cqfPruned, long transitivelyValidated,
            long exactTested, long exactRejected, long exactValidated) implements AkkaSerializable {

        public static PruneMetrics empty() {
            return new PruneMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        public PruneMetrics plus(PruneMetrics other) {
            Objects.requireNonNull(other, "other");
            return new PruneMetrics(Math.addExact(invalidLhsSkips, other.invalidLhsSkips),
                    Math.addExact(validRhsSkips, other.validRhsSkips),
                    Math.addExact(sameBatchSkips, other.sameBatchSkips),
                    Math.addExact(directLhsRejections, other.directLhsRejections),
                    Math.addExact(wholeCountPruned, other.wholeCountPruned),
                    Math.addExact(partitionCountPruned, other.partitionCountPruned),
                    Math.addExact(cqfPruned, other.cqfPruned),
                    Math.addExact(transitivelyValidated, other.transitivelyValidated),
                    Math.addExact(exactTested, other.exactTested), Math.addExact(exactRejected, other.exactRejected),
                    Math.addExact(exactValidated, other.exactValidated));
        }
    }

    public sealed interface BDReply extends AkkaSerializable permits BDReply.BatchAccepted,
            BDReply.DiscoveryFinished {

        record BatchAccepted(long epoch) implements BDReply {
        }

        record DiscoveryFinished(int finalRound) implements BDReply {
        }
    }

    public sealed interface BDCommand extends AkkaSerializable
            permits BDCommand.IngestBatch, BDCommand.FinishDiscovery, BDCommand.Shutdown,
            BDCommand.GetResultCollector, BDCommand.GetIngestionReady,
            BDCommand.CheckPoint, BDCommand.MissingBatchRequest, BDCommand.AaCheckpointStatus {

        record IngestBatch(String[] cells, int numRows, int numCols, ActorRef<BDReply> replyTo) implements BDCommand {
        }

        record FinishDiscovery(int finalRound, Map<Integer, Integer> finalBatchByTable, ActorRef<BDReply> replyTo)
                implements BDCommand {
        }

        record Shutdown() implements BDCommand {
        }

        record GetResultCollector(ActorRef<ActorRef<RCCommand>> replyTo) implements BDCommand {
        }

        record GetIngestionReady(ActorRef<IngestionReady> replyTo) implements BDCommand {
        }

        record CheckPoint(int round, Map<Integer, Integer> maxBatchIdByTable) implements BDCommand {
        }

        record MissingBatchRequest(int tableId, int batchId, int colId) implements BDCommand {
        }

        record AaCheckpointStatus(int round, int colId, boolean clean, List<InputBatchDetails> missing)
                implements BDCommand {
        }
    }

    public record UnaryCandidate(UnaryPair pair, int checkpointRound) implements AkkaSerializable {
    }

    public sealed interface CMCommand extends AkkaSerializable
            permits CMCommand.UnaryCandidateProposed, CMCommand.UnaryViolationReport,
            CMCommand.NaryViolationReport, CMCommand.EpochTick,
            CMCommand.DistinctValueDelta, CMCommand.IngestionDone,
            CMCommand.NaryDispatched, CMCommand.NaryQuiesced, CMCommand.DistinctDeltaBatch,
            CMCommand.LhsReplayDelta, CMCommand.RhsReplayDelta, CMCommand.LhsLiveDelta, CMCommand.RhsLiveDelta,
            CMCommand.MembershipResult, CMCommand.ValueOwnerCandidateStatusUpdate, CMCommand.ValueOwnerDrained,
            CMCommand.DrainReadyProbe, CMCommand.PartitionDrainReadyProbe, CMCommand.OwnersDrained,
            CMCommand.NoMoreCandidates, CMCommand.ForceFinish, CMCommand.EnsurePartitionInitialized {

        static String entityId(int partitionId) {
            return "cm-part-" + partitionId;
        }

        static int partitionFor(int lhsCol, int partitionCount) {
            return Math.floorMod(Integer.hashCode(lhsCol), partitionCount);
        }

        record UnaryCandidateProposed(UnaryCandidate candidate) implements CMCommand {
        }

        record UnaryViolationReport(ScanResult result) implements CMCommand {
        }

        record NaryViolationReport(NaryCheckResult result) implements CMCommand {
        }

        record EpochTick(long epoch) implements CMCommand {
        }

        record DistinctValueDelta(int colId, RoaringBitmap newValues, long epoch) implements CMCommand {
        }

        record IngestionDone() implements CMCommand {
        }

        record NaryDispatched() implements CMCommand {
        }

        record EnsurePartitionInitialized(int partitionId, int finalRound) implements CMCommand {
        }

        record DistinctDeltaBatch(int colId, long epoch, RoaringBitmap insertedDistinctValues) implements CMCommand {
        }

        record LhsLiveDelta(int colId, int round, RoaringBitmap newValues) implements CMCommand {
        }

        record RhsLiveDelta(int colId, int round, RoaringBitmap newValues) implements CMCommand {
        }

        record MembershipResult(UnaryPair pair, int round, RoaringBitmap missingValues) implements CMCommand {
        }

        record LhsCandidateStatusUpdate(int lhsCol, List<CandidateLocalStatus> statuses) implements AkkaSerializable {
            public LhsCandidateStatusUpdate {
                statuses = List.copyOf(statuses);
            }
        }

        record ValueOwnerCandidateStatusUpdate(int cmPartition, long epoch, int voSequence, int round, int bucketId,
                List<LhsCandidateStatusUpdate> lhsUpdates,
                ActorRef<disIND.valueBased.protocol.ValueOwnerProtocol.Command> replyTo) implements CMCommand {
            public ValueOwnerCandidateStatusUpdate {
                if (voSequence <= 0)
                    throw new IllegalArgumentException("voSequence must be positive");
                lhsUpdates = List.copyOf(lhsUpdates);
                Objects.requireNonNull(replyTo, "replyTo");
            }
        }

        record ValueOwnerDrained(int finalRound, int bucketId, int expectedBuckets, RoaringBitmap locallyRejectedRhs,
                long candidateEvaluationsWithoutPruning, long exactValueProbesWithoutPruning,
                PruneMetrics pruneMetrics, List<long[]> activeClusterSignatures) implements CMCommand {
            public ValueOwnerDrained {
                locallyRejectedRhs = locallyRejectedRhs.clone();
                Objects.requireNonNull(pruneMetrics, "pruneMetrics");
                Objects.requireNonNull(activeClusterSignatures, "activeClusterSignatures");
                activeClusterSignatures = activeClusterSignatures.stream()
                        .map(long[]::clone)
                        .toList();
            }
        }

        record DrainReadyProbe(int finalRound, int lhsCol, int bucketId,
                ActorRef<disIND.valueBased.protocol.ValueOwnerProtocol.Command> replyTo) implements CMCommand {
        }

        record PartitionDrainReadyProbe(int finalRound, int partitionId, int bucketId, int requiredSequence,
                ActorRef<disIND.valueBased.protocol.ValueOwnerProtocol.Command> replyTo) implements CMCommand {
        }

        record OwnersDrained(disIND.valueBased.protocol.DrainProtocol.OwnersDrained batch) implements CMCommand {
        }

        record LhsReplayDelta(UnaryPair pair, int colId, int fromRound, int toRound,
                RoaringBitmap newValues) implements CMCommand {
        }

        record RhsReplayDelta(UnaryPair pair, int colId, int fromRound, int toRound,
                RoaringBitmap newValues) implements CMCommand {
        }

        record NoMoreCandidates(int finalRound) implements CMCommand {
        }

        record ForceFinish(int finalRound) implements CMCommand {
        }

        record NaryQuiesced() implements CMCommand {
        }
    }

    public sealed interface RCCommand extends AkkaSerializable
            permits RCCommand.UnaryConfirmed, RCCommand.NaryConfirmed,
            RCCommand.UnaryRetracted, RCCommand.GetReport,
            RCCommand.PipelineDone, RCCommand.AwaitDiscoveryFinished, RCCommand.CmDiscoveryComplete {

        record UnaryConfirmed(UnaryPair pair, int round) implements RCCommand {
        }

        record NaryConfirmed(NaryPair pair, long epoch) implements RCCommand {
        }

        record UnaryRetracted(UnaryPair pair, long epoch) implements RCCommand {
        }

        record GetReport(ActorRef<IndReport> replyTo) implements RCCommand {
        }

        record PipelineDone() implements RCCommand {
        }

        record AwaitDiscoveryFinished(int finalRound, ActorRef<BDReply> replyTo) implements RCCommand {
        }

        record CmDiscoveryComplete(int lhsOwnerCol, int round, List<UnaryPair> unaryPairs,
                List<NaryPair> naryPairs, long candidateEvaluationsWithoutPruning,
                long exactValueProbesWithoutPruning, PruneMetrics pruneMetrics,
                long activeClusterEntriesAcrossBuckets,
                List<long[]> distinctActiveClusterSignatures) implements RCCommand {
            public CmDiscoveryComplete {
                Objects.requireNonNull(distinctActiveClusterSignatures, "distinctActiveClusterSignatures");
                distinctActiveClusterSignatures = distinctActiveClusterSignatures.stream()
                        .map(long[]::clone)
                        .toList();
            }
        }
    }

    public record IndReport(
            List<UnaryPair> confirmedUnary,
            List<NaryPair> confirmedNary,
            Map<Integer, String> colNames,
            long snapshotEpoch) implements AkkaSerializable {
    }
}
