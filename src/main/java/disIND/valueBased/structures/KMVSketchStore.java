package disIND.valueBased.structures;

import disIND.valueBased.model.SharedModel;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.Arrays;

public final class KMVSketchStore {

    private final int k;
    private final long[] hashes;
    private final LongOpenHashSet retainedHashes;
    private int size = 0;
    private long maxHash = Long.MAX_VALUE;

    public KMVSketchStore(int k) {
        if (k <= 0)
            throw new IllegalArgumentException("KMV size must be positive");
        this.k = k;
        this.hashes = new long[k];
        this.retainedHashes = new LongOpenHashSet(k);
    }

    public int k() { return k;}

    public SharedModel.KMVSketch getKMVSketch() {
        return new SharedModel.KMVSketch(k, Arrays.copyOf(hashes, size), size);
    }

    public void add(long value) {
        addHash(Hashing.mix64(value));
    }

    public void addHash(long hash) {
        long unsignedHash = hash & Long.MAX_VALUE;
        if (retainedHashes.contains(unsignedHash))
            return;
        if (size < k) {
            hashes[size++] = unsignedHash;
            retainedHashes.add(unsignedHash);
            recomputeMax();
            return;
        }
        if (unsignedHash < maxHash) {
            int maxIndex = 0;
            for (int i = 1; i < size; i++) {
                if (Long.compareUnsigned(hashes[i], hashes[maxIndex]) > 0) {
                    maxIndex = i;
                }
            }
            retainedHashes.remove(hashes[maxIndex]);
            hashes[maxIndex] = unsignedHash;
            retainedHashes.add(unsignedHash);
            recomputeMax();
        }
    }

    private void recomputeMax() {
        if (size == 0) {
            maxHash = Long.MAX_VALUE;
            return;
        }
        long max = hashes[0];
        for (int i = 1; i < size; i++) {
            if (Long.compareUnsigned(hashes[i], max) > 0) {
                max = hashes[i];
            }
        }
        maxHash = max;
    }
}
