package disIND.streamBasedShardedDispatcher.structures;

import disIND.streamBasedShardedDispatcher.utility.UserConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class ValueIdMap {

    private final ConcurrentHashMap<String, Integer> strToId = new ConcurrentHashMap<>();
    private final List<String> idToStr = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger counter = new AtomicInteger(0);

    public int getOrInsert(String value) {
        if (!UserConfig.STORE_VALUE_STRINGS)
            return stableHashId(value);
        return strToId.computeIfAbsent(value, v -> {
            int id = counter.getAndIncrement();
            idToStr.add(v);
            return id;
        });
    }

    public Optional<Integer> getId(String value) {
        if (!UserConfig.STORE_VALUE_STRINGS)
            return Optional.of(stableHashId(value));
        return Optional.ofNullable(strToId.get(value));
    }

    public String getValue(int id) {
        if (!UserConfig.STORE_VALUE_STRINGS)
            return String.valueOf(id);
        return idToStr.get(id);
    }

    public int size() {
        return strToId.size();
    }

    private static int stableHashId(String value) {
        long hash = Hashing.mix64(value == null ? 0 : value.hashCode());
        return (int) (hash & 0x7fffffffL);
    }
}
