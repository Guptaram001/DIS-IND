package disIND.valueBased.ingestion;

import disIND.valueBased.model.SharedModel.MembershipUpdates;
import disIND.valueBased.protocol.ValueOwnerProtocol.BatchBody;
import disIND.valueBased.structures.WorkerValueIdStore;

public interface BatchProcessor {
    MembershipUpdates process(int bucketId, BatchBody batch, WorkerValueIdStore valueIds);
}
