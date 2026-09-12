package disIND.valueBased.structures;

import java.util.Locale;

public enum CacheMode {
    LRU, CAFFEINE;

    public static CacheMode parse(String value) {
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "lru" -> LRU;
            case "caffeine" -> CAFFEINE;
            default -> throw new IllegalArgumentException("Cache mode must be lru or caffeine: " + value);
        };
    }
}
