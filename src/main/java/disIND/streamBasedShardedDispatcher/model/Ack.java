package disIND.streamBasedShardedDispatcher.model;

public record Ack(long batchId,long targetId,WorkType type) implements AkkaSerializable {

}

