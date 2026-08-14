package disIND.valueBased.structures;

public final class HLLSketchStore {

    private final int numberOfBuckets;
    private final int numberOfRegisters;
    private final byte[] registers;

    public HLLSketchStore(int precision) {
        if (precision < 4 || precision > 18)
            throw new IllegalArgumentException("HLL precision must be in [4,18]");
        this.numberOfBuckets = precision;
        this.numberOfRegisters = 1 << precision;
        this.registers = new byte[numberOfRegisters];
    }


    public void add(long value) {
        addHash(Hashing.mix64(value));
    }

    public void addHash(long hash) {
        int idx = (int) (hash >>> (64 - numberOfBuckets));
        long w = hash << numberOfBuckets;
        int rank = Long.numberOfLeadingZeros(w) + 1;
        if (rank > 64 - numberOfBuckets + 1) {
            rank = 64 - numberOfBuckets + 1;
        }
        if (rank > registers[idx])
            registers[idx] = (byte) rank;
    }

    public long estimate() {
        double alpha;

        if (numberOfRegisters == 16) alpha = 0.673;
        else if (numberOfRegisters == 32) alpha = 0.697;
        else if (numberOfRegisters == 64) alpha = 0.709;
        else alpha = 0.7213 / (1.0 + 1.079 / numberOfRegisters);

        double sum = 0.0;
        int zeros = 0;

        for (byte r : registers) {
            sum += Math.pow(2.0, -r);
            if (r == 0) zeros++;
        }

        double raw = alpha * numberOfRegisters * numberOfRegisters / sum;

        if (raw <= 2.5 * numberOfRegisters && zeros > 0)
            raw = numberOfRegisters * Math.log((double) numberOfRegisters / zeros);

        return Math.max(0L, Math.round(raw));
    }


}
