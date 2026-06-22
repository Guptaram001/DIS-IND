package disIND.streamBasedShardedDispatcher.structures;


import disIND.streamBasedShardedDispatcher.model.SharedModel;

public final class SketchStore {

    private final HLLSketchStore hll;
    private final KMVSketchStore kmv;

    public SketchStore() {
        this(12, 256);
    }

    public SharedModel.SketchSummary getSummary(int colId, long epoch) {
        return new SharedModel.SketchSummary(colId, epoch, estimateDistinct(), kmv.getKMVSketch());
    }

    public SketchStore(int hllPrecision, int kmvSize) {
        this.hll = new HLLSketchStore(hllPrecision);
        this.kmv = new KMVSketchStore(kmvSize);
    }

    public void insert(long valueId) {
        long h = Hashing.mix64(valueId);
        hll.addHash(h);
        kmv.addHash(h);
    }

    public long estimateDistinct() {
        return hll.estimate();
    }

}