package disIND.valueBased.membership;

public final class CandidateSetFactory {

    private static final int BITSET_MAX_COLUMNS = 2_000;

    private CandidateSetFactory() {
    }

    public static CandidateSet create(int totalColumns, int capacity) {
        if (totalColumns <= 0)
            throw new IllegalArgumentException("totalColumns must be positive");

        if (totalColumns <= BITSET_MAX_COLUMNS)
            return new BitSetCandidateSet(Math.toIntExact(capacity));

        return new RoaringCandidateSet();
    }
}