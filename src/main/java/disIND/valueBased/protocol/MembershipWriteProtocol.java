package disIND.valueBased.protocol;

import akka.actor.typed.ActorRef;
import disIND.valueBased.model.AkkaSerializable;

public final class MembershipWriteProtocol {
    private MembershipWriteProtocol() {
    }

    public sealed interface Command extends AkkaSerializable permits EncodedWriteBatch {
    }

    public record CandidateWrite(byte[] key, byte[] value, boolean delete) implements AkkaSerializable {
    }

    public record EncodedWriteBatch(int bucketId, long batchId, int[] membershipValueIds, byte[][] membershipValues,
            CandidateWrite[] candidateWrites, long encodedBytes, ActorRef<ValueOwnerProtocol.Command> replyTo)
            implements Command {
    }
}
