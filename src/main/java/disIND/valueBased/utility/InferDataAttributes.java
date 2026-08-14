package disIND.valueBased.utility;

import disIND.valueBased.model.SharedModel.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public final class InferDataAttributes {

    public static final class ColTypeStats {
        int integerCount;
        int decimalCount;
        int dateCount;
        int booleanCount;
        int stringCount;
        long total;
        Set<String> distinct = new HashSet<>();
    }

    public static ColType inferColType(String columnName, int columnIndex, List<String[]> sampleRows) {
        ColTypeStats stats = new ColTypeStats();
        for (String[] row : sampleRows) {
            if (columnIndex >= row.length) {
                System.err.printf("Malformed row: expected >= %d columns, got %d%n", columnIndex + 1, row.length);
                continue;
            }
            String value = clean(row[columnIndex]);
            if (value.isEmpty())
                continue;
            stats.total++;
            stats.distinct.add(value);

            if (isInteger(value))
                stats.integerCount++;
            else if (isDecimal(value))
                stats.decimalCount++;
            else if (isDate(value))
                stats.dateCount++;
            else if (isBoolean(value))
                stats.booleanCount++;
            else
                stats.stringCount++;
        }

        String lower = columnName.toLowerCase(Locale.ROOT);
        if ((lower.contains("key") || lower.endsWith("id")) && stats.total > 0) {
            double distinctRatio = ((double) stats.distinct.size()) / stats.total;

            if (distinctRatio > 0.95 && stats.integerCount >= stats.total * 0.90) {
                return ColType.KEY;
            }
        }

        if (stats.dateCount >= stats.total * 0.90)
            return ColType.DATE;
        if (stats.booleanCount >= stats.total * 0.90)
            return ColType.BOOLEAN;
        if (stats.integerCount >= stats.total * 0.90)
            return ColType.INTEGER;
        if ((stats.integerCount + stats.decimalCount) >= stats.total * 0.90)
            return ColType.DECIMAL;
        return ColType.STRING;
    }

    public static String clean(String s) {
        if (s == null)
            return "";
        s = s.trim();
        if (s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
            s = s.substring(1, s.length() - 1).trim();
        }
        return s;
    }

    private static boolean isInteger(String v) {
        try {
            Long.parseLong(v);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private static boolean isDecimal(String v) {
        try {
            Double.parseDouble(v);
            return v.contains(".");
        }
        catch (Exception e) {
            return false;
        }
    }

    private static boolean isBoolean(String v) {
        String s = v.toLowerCase(Locale.ROOT);
        return s.equals("true") || s.equals("false") || s.equals("0") || s.equals("1") || s.equals("yes")
                || s.equals("no");
    }

    private static final DateTimeFormatter[] DATE_FORMATS = {
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy")
    };

    private static boolean isDate(String value) {
        for (DateTimeFormatter f : DATE_FORMATS) {
            try {
                LocalDate.parse(value, f);
                return true;
            }
            catch (Exception ignored) {
            }
        }
        return false;
    }
}
