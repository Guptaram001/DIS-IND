package disIND.streamBasedShardedDispatcher.actors;

import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;

public record Ack(int batchId,String source) implements AkkaSerializable {}