package disIND.streamBasedShardedDispatcher.structures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * O(k) memory uniform random sampler (Vitter's Algorithm R).
 * Used for violation witnesses at both unary and n-ary levels.
 * Composable: merge(other) feeds other's samples through this sampler.
 */
public final class ReservoirSampler<T> {

    private final int k;
    private final List<T> reservoir;
    private int count = 0;
    private final Random rng;

    public ReservoirSampler(int k) {
        this.k         = k;
        this.reservoir = new ArrayList<>(k);
        this.rng       = new Random(42L);
    }

    public void observe(T item) {
        count++;
        if (reservoir.size() < k) {
            reservoir.add(item);
        } else {
            int j = rng.nextInt(count);
            if (j < k) reservoir.set(j, item);
        }
    }

    public void merge(ReservoirSampler<T> other) {
        other.reservoir.forEach(this::observe);
    }

    public List<T> samples() { return Collections.unmodifiableList(reservoir); }
    public int totalSeen()   { return count; }
}
