package disIND.valueBased.protocol;

import akka.actor.typed.ActorRef;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import disIND.valueBased.actors.DirectBatchAggregatorActor;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.DataOrientation;

import java.util.List;
import java.util.Objects;

public final class ValueOwnerProtocol {
    private ValueOwnerProtocol() {
    }

    public sealed interface Command extends AkkaSerializable
            permits StoreBatch, FinalizeMembership, CandidateManagerReady, PartitionCandidateManagerReady,
            DrainQueued, PartitionDrainQueued, RetryDrainProbe, MembershipWriteAcknowledged,
            MembershipWriteFailed, RetryMembershipWrite {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "batchType")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ValueMajorBatch.class, name = "value-major"),
            @JsonSubTypes.Type(value = ColumnMajorBatch.class, name = "column-major")
    })
    public sealed interface BatchBody extends AkkaSerializable
            permits ValueMajorBatch, ColumnMajorBatch {
    }

    public record ColumnRows(int columnId, int count) implements AkkaSerializable {
    }

    public record ValueData(String value, List<ColumnRows> columns) implements AkkaSerializable {
    }

    public record ValueMajorBatch(List<ValueData> values) implements BatchBody {
    }

    public record ValueRows(String value, long[] rowIds) implements AkkaSerializable {
        public ValueRows {
            Objects.requireNonNull(value, "value");
            rowIds = rowIds.clone();
        }
    }

    public record ColumnValues(int colId, List<ValueRows> values) implements AkkaSerializable {
        public ColumnValues {
            values = List.copyOf(values);
        }
    }

    public record ColumnMajorBatch(List<ColumnValues> columns) implements BatchBody {
        public ColumnMajorBatch {
            columns = List.copyOf(columns);
        }
    }

    public record StoreBatch(long epoch, int tableId, int batchId, int round,
            int bucketId, DataOrientation orientation, BatchBody body,
            ActorRef<DirectBatchAggregatorActor.Command> ackTo) implements Command {

        public StoreBatch {
            Objects.requireNonNull(orientation, "orientation");
            Objects.requireNonNull(body, "body");
            boolean matchingBody = switch (orientation) {
                case VALUE_MAJOR -> body instanceof ValueMajorBatch;
                case COLUMN_MAJOR -> body instanceof ColumnMajorBatch;
            };
            if (!matchingBody) {
                throw new IllegalArgumentException("Batch orientation " + orientation + " does not match body "
                        + body.getClass().getSimpleName());
            }
        }
    }

    public record FinalizeMembership(int finalRound, int expectedBuckets, int totalColumns) implements Command {
    }

    public record CandidateManagerReady(int finalRound, int lhsCol, int bucketId) implements Command {
    }

    public record PartitionCandidateManagerReady(int finalRound, int partitionId, int bucketId) implements Command {
    }

    public record DrainQueued(int finalRound, int lhsCol, int bucketId) implements Command {
    }

    public record PartitionDrainQueued(int finalRound, int partitionId, int bucketId) implements Command {
    }

    public enum RetryDrainProbe implements Command {
        INSTANCE
    }

    public record MembershipWriteAcknowledged(int bucketId, long batchId) implements Command {
    }

    public record MembershipWriteFailed(int bucketId, long batchId, String reason) implements Command {
    }

    public enum RetryMembershipWrite implements Command {
        INSTANCE
    }

}
