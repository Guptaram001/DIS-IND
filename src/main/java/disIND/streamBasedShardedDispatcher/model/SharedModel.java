package disIND.streamBasedShardedDispatcher.model;

import akka.actor.typed.ActorRef;
import disIND.prototypeModel.model.AkkaSerializable;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class SharedModel {

    private SharedModel() {}

    public enum ColType implements AkkaSerializable {
        INTEGER, DECIMAL, DATE, BOOLEAN, STRING, KEY,UNKNOWN
    }

    public record UnaryPair(int lhsCol, int rhsCol) implements AkkaSerializable {}

    public record NaryPair(List<Integer> lhsCols, List<Integer> rhsCols) implements AkkaSerializable {

        public int arity() { return lhsCols.size(); }

        public NaryPair canonical() {
            List<int[]> z = new ArrayList<>();
            for (int i = 0; i < lhsCols.size(); i++)
                z.add(new int[]{lhsCols.get(i), rhsCols.get(i)});
            z.sort(Comparator.comparingInt(a -> a[0]));
            List<Integer> l = new ArrayList<>(), r = new ArrayList<>();
            z.forEach(p -> { l.add(p[0]); r.add(p[1]); });
            return new NaryPair(l, r);
        }
    }

    public record DatasetMetadata(
            int totalCols,
            List<Integer> offsets,
            List<Integer> nCols,
            Map<Integer,String> colNames,
            Map<Integer,ColType> colTypes
    ) {}

    public record DeltaScanResult(
            UnaryPair pair,
            int violationCount,
            int totalLhs,
            List<String> witnesses,
            long sinceEpoch,
            long untilEpoch,
            RoaringBitmap violationBitmap,
            RoaringBitmap rhsBitmap
    ) implements AkkaSerializable {}

    public record NaryCheckResult(
            NaryPair pair,
            int violationCount,
            int totalLhs,
            List<String> witnesses,
            long evalEpoch
    ) implements AkkaSerializable {}

    //  Data carriers

    public record SketchSummary(
            int colId,
            long cardinality,
            RoaringBitmap bitmap,
            long epoch,
            ColType colType
    ) implements AkkaSerializable {}

    public record BitmapAtEpoch(
            int colId,
            RoaringBitmap bitmap,
            long epoch,
            long requestId
    ) implements AkkaSerializable {}

    public record ColumnSlice(
            int colId,
            Map<Integer, RoaringBitmap> valToRows,
            long epoch
    ) implements AkkaSerializable {}


    public sealed interface BDReply extends AkkaSerializable permits BDReply.BatchAccepted {

        record BatchAccepted(long epoch) implements BDReply {}
    }

    public sealed interface BDCommand extends AkkaSerializable
            permits BDCommand.IngestBatch, BDCommand.BatchDispatched, BDCommand.BatchFlushed,
            BDCommand.IngestionDone, BDCommand.Shutdown,
            BDCommand.GetResultCollector, BDCommand.GetBatchDispatcher, BDCommand.SendTableBatch {

        record IngestBatch(String[] cells, int numRows, int numCols, ActorRef<BDReply> replyTo) implements BDCommand {}

        record SendTableBatch(int tableId, long startRowId, List<String []> rows, ActorRef<BDReply> replyTo) implements BDCommand {}

        record BatchDispatched(long epoch) implements BDCommand {}

        record BatchFlushed(long epoch, int colId) implements BDCommand {}

        record IngestionDone() implements BDCommand {}

        record Shutdown() implements BDCommand {}

        record GetResultCollector(ActorRef<ActorRef<RCCommand>> replyTo) implements BDCommand {}

        record GetBatchDispatcher(ActorRef<ActorRef<BDCommand>> replyTo) implements BDCommand {}
    }


    public sealed interface AACommand extends AkkaSerializable
            permits AACommand.InsertBatch, AACommand.SetPresetType,
            AACommand.GetSketch, AACommand.GetBitmap,
            AACommand.DeltaScan, AACommand.GetColumnSlice,
            AACommand.UpdateWatermarks {

        static String entityId(int colId) { return "col-" + colId; }

        record InsertBatch(long epoch, long[] rows, int[] valueIds, ActorRef<BDCommand> ackTo) implements AACommand {}

        record SetPresetType(ColType preset) implements AACommand {}

        record GetSketch(long epoch, ActorRef<SketchSummary> replyTo) implements AACommand {}

        record GetBitmap(long epoch, long requestId, ActorRef<BitmapAtEpoch> replyTo) implements AACommand {}

        record DeltaScan(
                RoaringBitmap rhsBitmap, long sinceEpoch, long untilEpoch,
                UnaryPair pair, ActorRef<DeltaScanResult> replyTo
        ) implements AACommand {}

        record GetColumnSlice(long epoch, long requestId,
                              ActorRef<NRACommand> replyTo) implements AACommand {}

        record UpdateWatermarks(long binaryWm, long naryWm) implements AACommand {}
    }


    public sealed interface AppraiserCommand extends AkkaSerializable
            permits AppraiserCommand.SketchArrived,
            AppraiserCommand.EpochComplete,
            AppraiserCommand.IngestionDone {

        record SketchArrived(SketchSummary summary) implements AppraiserCommand {}
        record EpochComplete(long epoch)            implements AppraiserCommand {}

        record IngestionDone()                      implements AppraiserCommand {}
    }

    public record UnaryCandidate(UnaryPair pair, long evalEpoch) implements AkkaSerializable {}


    public sealed interface RACommand extends AkkaSerializable
            permits RACommand.EvaluateCandidate, RACommand.RhsBitmapReady,
            RACommand.DeltaScanReady, RACommand.RebuildRequest,
            RACommand.InjectCm, RACommand.BitmapReady {

        record BitmapReady(long requestId, BitmapAtEpoch bm)              implements RACommand {}
        record EvaluateCandidate(UnaryCandidate candidate)                 implements RACommand {}
        record RhsBitmapReady(BitmapAtEpoch bm, UnaryCandidate candidate)  implements RACommand {}
        record DeltaScanReady(DeltaScanResult result)                      implements RACommand {}
        record RebuildRequest(UnaryPair pair, long sinceEpoch,
                              long untilEpoch)                             implements RACommand {}
        record InjectCm(ActorRef<CMCommand> cmRef)                        implements RACommand {}
    }


    public sealed interface CMCommand extends AkkaSerializable
            permits CMCommand.UnaryCandidateProposed, CMCommand.UnaryViolationReport,
            CMCommand.NaryViolationReport, CMCommand.EpochTick,
            CMCommand.DistinctValueDelta, CMCommand.IngestionDone,
            CMCommand.NaryDispatched, CMCommand.NaryQuiesced {

        record UnaryCandidateProposed(UnaryCandidate candidate) implements CMCommand {}
        record UnaryViolationReport(DeltaScanResult result) implements CMCommand {}
        record NaryViolationReport(NaryCheckResult result)                 implements CMCommand {}
        record EpochTick(long epoch)                                       implements CMCommand {}
        record DistinctValueDelta(int colId, RoaringBitmap newValues, long epoch) implements CMCommand {}

        record IngestionDone()                                             implements CMCommand {}


        record NaryDispatched()                                            implements CMCommand {}

        /**
         * Sent by LM → CM when its queue is fully empty and nraInFlight=0.
         * This is the definitive n-ary quiescence signal.  CM uses it instead
         * of counting NaryDispatched/NaryViolationReport pairs, which races
         * when confirmed n-ary INDs trigger new generalisations via generalise().
         */
        record NaryQuiesced()                                              implements CMCommand {}
    }

    // ── LatticeManagerActor commands ─────────────────────────────────────

    public sealed interface LMCommand extends AkkaSerializable
            permits LMCommand.UnaryResult, LMCommand.NaryResult,
            LMCommand.CreditReplenish, LMCommand.InjectNra,
            LMCommand.InjectCm, LMCommand.IngestionDone,
            LMCommand.CheckQuiescence {

        record UnaryResult(boolean confirmed, UnaryPair pair, long epoch)  implements LMCommand {}
        record NaryResult(boolean confirmed, NaryPair pair, long epoch)    implements LMCommand {}
        record CreditReplenish(int n)                                      implements LMCommand {}
        record InjectNra(ActorRef<NRACommand> nraRef)                      implements LMCommand {}
        /** Injected by INDGuardian so LM can notify CM when n-ary work quiesces. */
        record InjectCm(ActorRef<CMCommand> cmRef)                         implements LMCommand {}
        /** Forwarded from BD via CM/Appraisal chain; LM needs to know when to
         *  signal NaryQuiesced after its queue drains. */
        record IngestionDone()                                             implements LMCommand {}
        /** CM → LM: "are you idle?" LM replies NaryQuiesced if yes, or when next idle. */
        record CheckQuiescence()                                           implements LMCommand {}
    }

    // ── NaryRebuildActor commands ─────────────────────────────────────────

    public sealed interface NRACommand extends AkkaSerializable
            permits NRACommand.EvaluateNary, NRACommand.ColumnSliceArrived {

        record EvaluateNary(NaryPair pair, long evalEpoch)                 implements NRACommand {}
        record ColumnSliceArrived(ColumnSlice slice, long requestId)       implements NRACommand {}
    }

    // ── ResultCollectorActor commands ─────────────────────────────────────

    public sealed interface RCCommand extends AkkaSerializable
            permits RCCommand.UnaryConfirmed, RCCommand.NaryConfirmed,
            RCCommand.UnaryRetracted, RCCommand.GetReport,
            RCCommand.PipelineDone {

        record UnaryConfirmed(UnaryPair pair, long epoch)                  implements RCCommand {}
        record NaryConfirmed(NaryPair pair, long epoch)                    implements RCCommand {}
        record UnaryRetracted(UnaryPair pair, long epoch)                  implements RCCommand {}
        record GetReport(ActorRef<IndReport> replyTo)                      implements RCCommand {}

        /**
         * Sent by CM once all unary + n-ary evaluations have quiesced and no
         * further changes are possible.  RC uses this to immediately satisfy any
         * pending GetReport ask so TpchLoader.run() unblocks without a sleep.
         */
        record PipelineDone()                                              implements RCCommand {}
    }

    // ── Report ────────────────────────────────────────────────────────────

    public record IndReport(
            List<UnaryPair>      confirmedUnary,
            List<NaryPair>       confirmedNary,
            Map<Integer, String> colNames,
            long                 snapshotEpoch
    ) implements AkkaSerializable {}
}