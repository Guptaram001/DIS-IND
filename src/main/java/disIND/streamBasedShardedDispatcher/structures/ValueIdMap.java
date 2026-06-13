package disIND.streamBasedShardedDispatcher.structures;

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
        return strToId.computeIfAbsent(value, v -> {
            int id = counter.getAndIncrement();
            idToStr.add(v);
            return id;
        });
    }

    public Optional<Integer> getId(String value) {
        return Optional.ofNullable(strToId.get(value));
    }

    public String getValue(int id) {
        return idToStr.get(id);
    }

    public int size() {
        return strToId.size();
    }
}
