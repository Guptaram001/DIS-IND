package disIND.valueBased.protocol;

import akka.actor.typed.ActorRef;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.PruneMetrics;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Objects;

/**
 * Reliable, worker-local delivery protocol for the final VO -> CM drain
 * fan-out.
 */
public final class DrainProtocol {
    private DrainProtocol() {
    }

    public sealed interface Command extends AkkaSerializable
            permits Enqueue, EnqueuePartition, BatchAcknowledged, RetryTick {
    }

    public record DrainRecord(int finalRound, int lhsCol, int bucketId, int expectedBuckets,
            RoaringBitmap locallyRejectedRhs, long candidateEvaluationsWithoutPruning,
            long exactValueProbesWithoutPruning, PruneMetrics pruneMetrics,
            List<long[]> activeClusterSignatures) implements AkkaSerializable {
        public DrainRecord {
            locallyRejectedRhs = locallyRejectedRhs.clone();
            Objects.requireNonNull(pruneMetrics, "pruneMetrics");
            Objects.requireNonNull(activeClusterSignatures, "activeClusterSignatures");
            activeClusterSignatures = activeClusterSignatures.stream()
                    .map(long[]::clone)
                    .toList();
        }
    }

    public record Enqueue(DrainRecord record,
            ActorRef<disIND.valueBased.protocol.ValueOwnerProtocol.Command> replyTo) implements Command {
        public Enqueue {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(replyTo, "replyTo");
        }
    }

    public record EnqueuePartition(int partitionId, List<DrainRecord> records,
            ActorRef<disIND.valueBased.protocol.ValueOwnerProtocol.Command> replyTo) implements Command {
        public EnqueuePartition {
            records = List.copyOf(records);
            Objects.requireNonNull(replyTo, "replyTo");
            if (records.isEmpty())
                throw new IllegalArgumentException("records must not be empty");
        }
    }

    public record BatchAcknowledged(long batchId, int partitionId) implements Command {
    }

    public enum RetryTick implements Command {
        INSTANCE
    }

    public record OwnersDrained(long batchId, List<DrainRecord> owners,
            ActorRef<Command> replyTo) implements AkkaSerializable {
        public OwnersDrained {
            owners = List.copyOf(owners);
            Objects.requireNonNull(replyTo, "replyTo");
            if (owners.isEmpty())
                throw new IllegalArgumentException("owners must not be empty");
        }
    }
}
