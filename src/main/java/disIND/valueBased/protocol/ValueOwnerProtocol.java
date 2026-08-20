package disIND.valueBased.protocol;

import akka.actor.typed.ActorRef;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import disIND.valueBased.actors.DirectBatchAggregatorActor;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.DataOrientation;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ValueOwnerProtocol {
    private ValueOwnerProtocol() {
    }

    public sealed interface Command extends AkkaSerializable
            permits StoreBatch, GetBucket, FinalizeMembership {
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

    public record GetBucket(ActorRef<BucketSnapshot> replyTo) implements Command {
    }

    public record FinalizeMembership(int finalRound, int expectedBuckets, int totalColumns) implements Command {
    }

    public record ColumnCount(int colId, long count) implements AkkaSerializable {
    }

    public record BucketSnapshot(int bucketId, Map<Integer, List<ColumnCount>> values)
            implements AkkaSerializable {
    }
}
