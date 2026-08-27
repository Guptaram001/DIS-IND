package disIND.valueBased.utility;

import disIND.valueBased.model.SharedModel.ColType;

public final class ColTypeCompatibility {

    private ColTypeCompatibility() {
    }

    private static final int[] COMPATIBLE_MASK = new int[ColType.values().length];;

    static {
        allow(ColType.INTEGER, ColType.INTEGER, ColType.DECIMAL, ColType.STRING, ColType.UNKNOWN);
        allow(ColType.DECIMAL, ColType.INTEGER, ColType.DECIMAL, ColType.STRING, ColType.UNKNOWN);
        allow(ColType.DATE, ColType.DATE, ColType.STRING, ColType.UNKNOWN);
        allow(ColType.BOOLEAN, ColType.BOOLEAN, ColType.STRING, ColType.UNKNOWN);
        allow(ColType.STRING, ColType.STRING, ColType.UNKNOWN);
        allow(ColType.STRING, ColType.values());
        allow(ColType.UNKNOWN, ColType.values());
    }

    private static void allow(ColType lhs, ColType... rhsTypes) {
        int mask = 0;
        for (ColType rhs : rhsTypes) {
            mask |= 1 << rhs.ordinal();
        }
        COMPATIBLE_MASK[lhs.ordinal()] = mask;
    }

    public static boolean testCompatibility(ColType lhs, ColType rhs) {
        if (lhs == null || rhs == null)
            return true;
        int rhsBit = 1 << rhs.ordinal();
        return (COMPATIBLE_MASK[lhs.ordinal()] & rhsBit) != 0;
    }
}