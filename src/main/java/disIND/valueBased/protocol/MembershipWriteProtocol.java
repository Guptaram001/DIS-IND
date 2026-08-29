package disIND.valueBased.protocol;

import disIND.valueBased.model.AkkaSerializable;

public final class MembershipWriteProtocol {
    private MembershipWriteProtocol() {
    }

    public sealed interface Command extends AkkaSerializable permits StagedWrite, FlushTick {
    }

    public record StagedWrite(int bucketId) implements Command {
    }

    public enum FlushTick implements Command {
        INSTANCE
    }
}
