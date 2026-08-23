package disIND.valueBased.model;

import akka.actor.typed.ActorRef;
import disIND.valueBased.structures.BitmapStore;
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
        INTEGER, DECIMAL, DATE, BOOLEAN, STRING, KEY, UNKNOWN
    }

    public enum PairState {
        ACTIVE, INACTIVE
    }

    public enum DataOrientation implements AkkaSerializable {
        VALUE_MAJOR, COLUMN_MAJOR
    }

    public enum CandidateTrackingMode implements AkkaSerializable {
        COUNT, WITNESS, PRUNE
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

        public String qualifiedName(int globalId) {
            return columns.get(globalId).qualifiedName();
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

    public record ScanResult(
            UnaryPair pair,
            int violationCount,
            List<Integer> witnesses,
            RoaringBitmap violationBitmap,
            int round) implements AkkaSerializable {
    }

    public record NaryCheckResult(
            NaryPair pair,
            int violationCount,
            int totalLhs,
            List<String> witnesses,
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

    // Data carriers

    public record KMVSketch(int k, long[] hashes, int size) implements AkkaSerializable {

        public double containmentIn(KMVSketch rhs) {
            long threshold = Math.min(maxObserved(), rhs.maxObserved());
            int lhsConsidered = 0;
            int common = 0;
            for (int i = 0; i < size; i++) {
                if (Long.compareUnsigned(hashes[i], threshold) <= 0) {
                    lhsConsidered++;
                    if (rhs.contains(hashes[i]))
                        common++;
                }
            }
            if (lhsConsidered == 0)
                return 1.0;
            return (double) common / lhsConsidered;
        }

        public double jaccard(KMVSketch rhs) {
            long threshold = Math.min(maxObserved(), rhs.maxObserved());
            int intersection = 0;
            int union = 0;
            for (int i = 0; i < size; i++) {
                if (Long.compareUnsigned(hashes[i], threshold) <= 0) {
                    union++;
                    if (rhs.contains(hashes[i]))
                        intersection++;
                }
            }
            for (int i = 0; i < rhs.size; i++) {
                long h = rhs.hashes[i];
                if (Long.compareUnsigned(h, threshold) <= 0 && !contains(h))
                    union++;
            }
            if (union == 0)
                return 1.0;
            return (double) intersection / union;
        }

        private boolean contains(long h) {
            for (int i = 0; i < size; i++) {
                if (hashes[i] == h)
                    return true;
            }
            return false;
        }

        private long maxObserved() {
            if (size < k)
                return Long.MAX_VALUE;
            long max = hashes[0];
            for (int i = 1; i < size; i++) {
                if (Long.compareUnsigned(hashes[i], max) > 0)
                    max = hashes[i];
            }
            return max;
        }
    }

    public record SketchSummary(
            int colId,
            long epoch,
            int round,
            long distinctValues,
            KMVSketch kmv) implements AkkaSerializable {
    }

    public record BitmapAtEpoch(
            int colId,
            RoaringBitmap bitmap,
            long epoch,
            long requestId) implements AkkaSerializable {
    }

    public record AttributeDelta(int round, RoaringBitmap distinctValues) implements AkkaSerializable {
    }

    public record AttributeCheckPoint(
            int round,
            BitmapStore bitmapStore,
            SketchSummary sketchSummary) {
    }

    public record InputBatchDetails(int tableId, int startRowId, int batchId, int epoch, int round, int colId) {
    }

    public record RawColumnBatch(int colId, int[] rowIds, String[] values) implements AkkaSerializable {
        public RawColumnBatch {
            Objects.requireNonNull(rowIds, "rowIds");
            Objects.requireNonNull(values, "values");
            if (rowIds.length != values.length)
                throw new IllegalArgumentException("rowIds and values must have the same length");
        }
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

    public record IngestionResult(long totalRows, int finalRound, Map<Integer, Integer> finalBatchByTable) {
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

    public record PruneMetrics(
            long invalidLhsSkips,
            long validRhsSkips,
            long sameBatchSkips,
            long directLhsRejections,
            long wholeCountPruned,
            long partitionCountPruned,
            long cqfPruned,
            long exactTested,
            long exactRejected,
            long exactValidated) implements AkkaSerializable {

        public static PruneMetrics empty() {
            return new PruneMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        public PruneMetrics plus(PruneMetrics other) {
            Objects.requireNonNull(other, "other");
            return new PruneMetrics(
                    Math.addExact(invalidLhsSkips, other.invalidLhsSkips),
                    Math.addExact(validRhsSkips, other.validRhsSkips),
                    Math.addExact(sameBatchSkips, other.sameBatchSkips),
                    Math.addExact(directLhsRejections, other.directLhsRejections),
                    Math.addExact(wholeCountPruned, other.wholeCountPruned),
                    Math.addExact(partitionCountPruned, other.partitionCountPruned),
                    Math.addExact(cqfPruned, other.cqfPruned),
                    Math.addExact(exactTested, other.exactTested),
                    Math.addExact(exactRejected, other.exactRejected),
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

    public sealed interface AACommand extends AkkaSerializable
            permits AACommand.InsertBatch, AACommand.SetPresetType,
            AACommand.GetSketch, AACommand.GetBitmap,
            AACommand.DeltaScan, AACommand.GetColumnSlice,
            AACommand.UpdateWatermarks, AACommand.EmitSketch, AACommand.CheckPoint, AACommand.GetSnapshot,
            AACommand.SendColumnData, AACommand.CompareBitmap, AACommand.CheckMembership, AACommand.DeactiveUnaryPair,
            AACommand.RequestSketch, AACommand.CheckpointPersisted,
            AACommand.CheckpointPersistenceFailed, AACommand.RetryCheckpointPersistence,
            AACommand.BatchRowsPersisted, AACommand.BatchRowsPersistenceFailed,
            AACommand.RetryBatchRowsPersistence {

        static String entityId(int colId) {
            return "col-" + colId;
        }

        record InsertBatch(InputBatchDetails inputBatchDetails, long[] rows, int[] valueIds,
                ActorRef<disIND.valueBased.actors.DirectBatchAggregatorActor.Command> ackTo) implements AACommand {
        }

        record SetPresetType(ColType preset) implements AACommand {
        }

        record GetSketch(long epoch, ActorRef<SketchSummary> replyTo) implements AACommand {
        }

        record GetBitmap(long epoch, long requestId, ActorRef<BitmapAtEpoch> replyTo) implements AACommand {
        }

        record DeltaScan(RoaringBitmap rhsBitmap, long sinceEpoch, long untilEpoch, UnaryPair pair,
                ActorRef<ScanResult> replyTo) implements AACommand {
        }

        record GetColumnSlice(long epoch, long requestId,
                ActorRef<NRACommand> replyTo) implements AACommand {
        }

        record EmitSketch(int round, long epoch, ActorRef<AppraiserCommand> replyTo) implements AACommand {
        }

        record UpdateWatermarks(long binaryWm, long naryWm) implements AACommand {
        }

        record CheckPoint(long epoch, int colId, int round, Map<Integer, Integer> maxBatchIdByTable,
                ActorRef<BDCommand> replyTo, ActorRef<AppraiserCommand> appraiserRef) implements AACommand {
        }

        record GetSnapshot(long epoch, ActorRef<RACommand> replyTo) implements AACommand {
        }

        record SendColumnData(UnaryCandidate candidate) implements AACommand {
        }

        record CompareBitmap(UnaryCandidate candidate, RoaringBitmap lhsBitmap) implements AACommand {
        }

        record CheckMembership(UnaryPair pair, int round, RoaringBitmap values, int replyOwnerCol)
                implements AACommand {
        }

        record DeactiveUnaryPair(UnaryPair pair, boolean lhsSide) implements AACommand {
        }

        record RequestSketch(int round, long epoch, ActorRef<AppraiserCommand> replyTo) implements AACommand {
        }

        record CheckpointPersisted(int round) implements AACommand {
        }

        record CheckpointPersistenceFailed(int round, String reason) implements AACommand {
        }

        record RetryCheckpointPersistence(int round) implements AACommand {
        }

        record BatchRowsPersisted(InputBatchDetails inputBatchDetails) implements AACommand {
        }

        record BatchRowsPersistenceFailed(InputBatchDetails inputBatchDetails, String reason) implements AACommand {
        }

        record RetryBatchRowsPersistence(int tableId, int batchId) implements AACommand {
        }
    }

    public sealed interface AppraiserCommand extends AkkaSerializable
            permits AppraiserCommand.SketchArrived,
            AppraiserCommand.CheckPoint,
            AppraiserCommand.FinishDiscovery, AppraiserCommand.PairStateChanged,
            AppraiserCommand.CheckMissingSketches {

        record SketchArrived(SketchSummary summary) implements AppraiserCommand {
        }

        record CheckPoint(int round, long epoch, Map<Integer, Integer> maxBatchIdByTable) implements AppraiserCommand {
        }

        record CheckMissingSketches(int round) implements AppraiserCommand {
        }

        record FinishDiscovery(int finalRound, ActorRef<RCCommand> rcRef) implements AppraiserCommand {
        }

        record PairStateChanged(UnaryPair pair, PairState state) implements AppraiserCommand {
        }
    }

    public record UnaryCandidate(UnaryPair pair, int checkpointRound) implements AkkaSerializable {
    }

    public sealed interface RACommand extends AkkaSerializable
            permits RACommand.EvaluateCandidate, RACommand.RhsBitmapReady,
            RACommand.ScanReady, RACommand.RebuildRequest,
            RACommand.InjectCm, RACommand.BitmapReady {

        record BitmapReady(long requestId, BitmapAtEpoch bm) implements RACommand {
        }

        record EvaluateCandidate(UnaryCandidate candidate) implements RACommand {
        }

        record RhsBitmapReady(BitmapAtEpoch bm, UnaryCandidate candidate) implements RACommand {
        }

        record ScanReady(ScanResult result) implements RACommand {
        }

        record RebuildRequest(UnaryPair pair, long sinceEpoch,
                long untilEpoch) implements RACommand {
        }

        record InjectCm(ActorRef<CMCommand> cmRef) implements RACommand {
        }
    }

    public sealed interface CMCommand extends AkkaSerializable
            permits CMCommand.UnaryCandidateProposed, CMCommand.UnaryViolationReport,
            CMCommand.NaryViolationReport, CMCommand.EpochTick,
            CMCommand.DistinctValueDelta, CMCommand.IngestionDone,
            CMCommand.NaryDispatched, CMCommand.NaryQuiesced, CMCommand.DistinctDeltaBatch,
            CMCommand.LhsReplayDelta, CMCommand.RhsReplayDelta, CMCommand.LhsLiveDelta, CMCommand.RhsLiveDelta,
            CMCommand.MembershipResult, CMCommand.ValueOwnerCandidateStatusUpdate, CMCommand.ValueOwnerDrained,
            CMCommand.DrainReadyProbe, CMCommand.OwnersDrained,
            CMCommand.NoMoreCandidates, CMCommand.ForceFinish {

        static String entityId(int lhsCol) {
            return "cm-lhs-" + lhsCol;
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

        record DistinctDeltaBatch(int colId, long epoch, RoaringBitmap insertedDistinctValues) implements CMCommand {
        }

        record LhsLiveDelta(int colId, int round, RoaringBitmap newValues) implements CMCommand {
        }

        record RhsLiveDelta(int colId, int round, RoaringBitmap newValues) implements CMCommand {
        }

        record MembershipResult(UnaryPair pair, int round, RoaringBitmap missingValues) implements CMCommand {
        }

        record ValueOwnerCandidateStatusUpdate(long epoch, int voSequence, int round, int bucketId,
                List<CandidateLocalStatus> statuses) implements CMCommand {
            public ValueOwnerCandidateStatusUpdate {
                if (voSequence <= 0)
                    throw new IllegalArgumentException("voSequence must be positive");
                statuses = List.copyOf(statuses);
            }
        }

        record ValueOwnerDrained(int finalRound, int bucketId, int expectedBuckets, RoaringBitmap locallyRejectedRhs,
                long candidateEvaluationsWithoutPruning, long exactValueProbesWithoutPruning,
                PruneMetrics pruneMetrics) implements CMCommand {
            public ValueOwnerDrained {
                locallyRejectedRhs = locallyRejectedRhs.clone();
                Objects.requireNonNull(pruneMetrics, "pruneMetrics");
            }
        }

        record DrainReadyProbe(int finalRound, int lhsCol, int bucketId,
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

        /**
         * Sent by LM → CM when its queue is fully empty and nraInFlight=0.
         * This is the definitive n-ary quiescence signal. CM uses it instead
         * of counting NaryDispatched/NaryViolationReport pairs, which races
         * when confirmed n-ary INDs trigger new generalisations via generalise().
         */
        record NaryQuiesced() implements CMCommand {
        }
    }

    // ── LatticeManagerActor commands ─────────────────────────────────────

    public sealed interface LMCommand extends AkkaSerializable
            permits LMCommand.UnaryResult, LMCommand.NaryResult,
            LMCommand.CreditReplenish, LMCommand.InjectNra,
            LMCommand.InjectCm, LMCommand.IngestionDone,
            LMCommand.CheckQuiescence {

        record UnaryResult(boolean confirmed, UnaryPair pair, long epoch) implements LMCommand {
        }

        record NaryResult(boolean confirmed, NaryPair pair, long epoch) implements LMCommand {
        }

        record CreditReplenish(int n) implements LMCommand {
        }

        record InjectNra(ActorRef<NRACommand> nraRef) implements LMCommand {
        }

        /** Injected by INDGuardian so LM can notify CM when n-ary work quiesces. */
        record InjectCm(ActorRef<CMCommand> cmRef) implements LMCommand {
        }

        /**
         * Forwarded from BD via CM/Appraisal chain; LM needs to know when to
         * signal NaryQuiesced after its queue drains.
         */
        record IngestionDone() implements LMCommand {
        }

        /**
         * CM → LM: "are you idle?" LM replies NaryQuiesced if yes, or when next idle.
         */
        record CheckQuiescence() implements LMCommand {
        }
    }

    // ── NaryRebuildActor commands ─────────────────────────────────────────

    public sealed interface NRACommand extends AkkaSerializable
            permits NRACommand.EvaluateNary, NRACommand.ColumnSliceArrived {

        record EvaluateNary(NaryPair pair, long evalEpoch) implements NRACommand {
        }

        record ColumnSliceArrived(ColumnSlice slice, long requestId) implements NRACommand {
        }
    }

    // ── ResultCollectorActor commands ─────────────────────────────────────

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

        /**
         * Sent by CM once all unary + n-ary evaluations have quiesced and no
         * further changes are possible. RC uses this to immediately satisfy any
         * pending GetReport ask so TpchLoader.run() unblocks without a sleep.
         */
        record PipelineDone() implements RCCommand {
        }

        record AwaitDiscoveryFinished(int finalRound, ActorRef<BDReply> replyTo) implements RCCommand {
        }

        record CmDiscoveryComplete(int lhsOwnerCol, int round, List<UnaryPair> unaryPairs,
                List<NaryPair> naryPairs, long candidateEvaluationsWithoutPruning,
                long exactValueProbesWithoutPruning, PruneMetrics pruneMetrics) implements RCCommand {
        }
    }

    // ── Report ────────────────────────────────────────────────────────────

    public record IndReport(
            List<UnaryPair> confirmedUnary,
            List<NaryPair> confirmedNary,
            Map<Integer, String> colNames,
            long snapshotEpoch) implements AkkaSerializable {
    }
}
