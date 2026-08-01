package disIND.valueBased.utility;

import disIND.valueBased.model.SharedModel.ColType;

public final class ColTypeCompatibility {

    private ColTypeCompatibility() {}

    private static final boolean[][] COMPATIBLE = new boolean[ColType.values().length][ColType.values().length];

    static {
        allow(ColType.KEY,     ColType.KEY);
        allow(ColType.KEY,     ColType.INTEGER);
        allow(ColType.INTEGER, ColType.KEY);
        allow(ColType.INTEGER, ColType.INTEGER);

        allow(ColType.INTEGER, ColType.DECIMAL);
        allow(ColType.DECIMAL, ColType.INTEGER);
        allow(ColType.DECIMAL, ColType.DECIMAL);

        allow(ColType.DATE,    ColType.DATE);
        allow(ColType.BOOLEAN, ColType.BOOLEAN);
        allow(ColType.STRING,  ColType.STRING);

        allow(ColType.UNKNOWN, ColType.UNKNOWN);
        allow(ColType.UNKNOWN, ColType.STRING);
        allow(ColType.STRING,  ColType.UNKNOWN);
    }

    private static void allow(ColType a, ColType b) {
        COMPATIBLE[a.ordinal()][b.ordinal()] = true;
    }

    public static boolean testCompatibility(ColType lhs, ColType rhs) {
        if (lhs == null || rhs == null)
            return true;

        return COMPATIBLE[lhs.ordinal()][rhs.ordinal()];
    }
}