package disIND.streamBasedNoCentralCoordinator.model;


public sealed interface RawEvent extends AkkaSerializable permits RawEvent.Insert, RawEvent.Delete{
    short attrId();
    String valueStr();
    long   rowId();


    record Insert(short attrId, String valueStr, long rowId) implements RawEvent {}

    record Delete(short attrId, String valueStr, long rowId) implements RawEvent {}

    record Batch(java.util.List<RawEvent> events, long batchId){}
}
