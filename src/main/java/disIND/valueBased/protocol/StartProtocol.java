package disIND.valueBased.protocol;

import akka.actor.typed.ActorRef;
import akka.actor.typed.receptionist.Receptionist;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.model.SharedModel.DataOrientation;
import disIND.valueBased.model.SharedModel.DatasetMetadata;

public interface StartProtocol {

        sealed interface Command {
        }

        record RequestConfig(String workerId, ActorRef<Command> replyTo) implements Command, AkkaSerializable {
        }

        record InstallConfig(DatasetConfig config) implements Command, AkkaSerializable {
        }

        record WorkerReady(String workerId) implements Command, AkkaSerializable {
        }

        record WorkerFailed(String workerId, String reason) implements Command, AkkaSerializable {
        }

        record ConfigServiceListing(Receptionist.Listing listing) implements Command {
        }

        record DatasetConfig(DatasetMetadata metadata, DataOrientation orientation,
                        CandidateTrackingMode candidateTracking) implements AkkaSerializable {
        }
}
