package disIND.valueBased.structures;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.Hash;

/**
 * One RocksDB db and one bounded LRU cache shared by all valueowners hosted on a worker.
 */
public final class WorkerValueIdStore implements AutoCloseable {
    private static final byte VALUE_PREFIX = 0;
    private static final byte NEXT_ID_PREFIX = 1;
    private static final long WRITE_BUFFER_BYTES = 32L * 1024 * 1024;
    private static final long MAX_TOTAL_WAL_BYTES = 128L * 1024 * 1024;
    private static final float BLOOM_FILTER_BITS_PER_KEY = UserConfig.BLOOM_FILTER_BITS_PER_KEY;

    private record CacheKey(int ownerId, String value) {}

    static {
        RocksDB.loadLibrary();
    }

    private final int ownerCount;
    private final Path databasePath;
    private final Cache<CacheKey, Integer> hotValues;
    //private final Map<Integer, Integer> nextIds = new HashMap<>();
    private final int[] nextIds;
    private final byte[][] nextIdKeys;
    private final BloomFilter bloomFilter;
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB database;
    private volatile boolean closed;

    public WorkerValueIdStore(Path databasePath, int maxHotEntries, int ownerCount) {
        if (maxHotEntries < 0)
            throw new IllegalArgumentException("maxHotEntries must be zero or greater");
        if (ownerCount <= 0)
            throw new IllegalArgumentException("ownerCount must be positive");
        this.databasePath = databasePath;
        this.ownerCount = ownerCount;
        this.nextIds=new int[ownerCount];
        Arrays.fill(nextIds, -1);
        this.nextIdKeys = new byte[ownerCount][];
        this.hotValues = CacheBuilder.newBuilder().maximumSize(maxHotEntries).build();
        for (int ownerId = 0; ownerId < ownerCount; ownerId++) {
        nextIdKeys[ownerId] = ByteBuffer
            .allocate(1 + Integer.BYTES)
            .put(NEXT_ID_PREFIX)
            .putInt(ownerId)
            .array();
}
        try {
            Files.createDirectories(databasePath);
            this.bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY, false);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setFilterPolicy(bloomFilter)
                    .setWholeKeyFiltering(true);
            this.options = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setWriteBufferSize(WRITE_BUFFER_BYTES)
                    .setMaxWriteBufferNumber(2)
                    .setMinWriteBufferNumberToMerge(1)
                    .setMaxTotalWalSize(MAX_TOTAL_WAL_BYTES);
            this.writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);
            this.database = RocksDB.open(options, databasePath.toString());
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("Cannot open worker value-ID RocksDB at " + databasePath, exception);
        }
    }

    public Map<String, Integer> resolveBatch(int ownerId, Collection<String> values) {
        validateOwner(ownerId);
        Objects.requireNonNull(values, "values");
        ensureOpen();
            return resolveOwnerBatch(ownerId, values);
    }

    private Map<String, Integer> resolveOwnerBatch(int ownerId, Collection<String> values) {
        LinkedHashSet<String> distinct = new LinkedHashSet<>(values.size());
        for (String value : values)
            distinct.add(Objects.requireNonNull(value, "value"));

        Map<String, Integer> resolved = new LinkedHashMap<>(distinct.size());
        List<String> coldValues = new ArrayList<>();
        List<byte[]> coldKeys = new ArrayList<>();
        for (String value : distinct) {
            Integer id = hotValues.getIfPresent(new CacheKey(ownerId, value));
            if (id == null) {
                coldValues.add(value);
                coldKeys.add(valueKey(ownerId, value));
            } else {
                resolved.put(value, id);
            }
        }
        if (coldKeys.isEmpty())
            return resolved;

        try {
            List<byte[]> storedIds = database.multiGetAsList(coldKeys);
            List<Integer> missingIndexes = new ArrayList<>();
            for (int index = 0; index < coldValues.size(); index++) {
                String value = coldValues.get(index);
                byte[] stored = storedIds.get(index);
                if (stored == null) {
                    missingIndexes.add(index);
                } else {
                    int id = decodeId(stored);
                    resolved.put(value, id);
                    hotValues.put(new CacheKey(ownerId, value), id);
                }
            }

            if (!missingIndexes.isEmpty()) {
                int committedNextId = nextId(ownerId);
                Map<String, Integer> allocated = new LinkedHashMap<>(missingIndexes.size());
                try (WriteBatch batch = new WriteBatch()) {
                    for (int index : missingIndexes) {
                        String value = coldValues.get(index);
                        int id = globalId(ownerId, committedNextId++);
                        batch.put(coldKeys.get(index), encodeId(id));
                        allocated.put(value, id);
                    }
                    //batch.put(nextIdKeys(ownerId), encodeId(committedNextId));
                    batch.put(nextIdKeys[ownerId], encodeId(committedNextId));
                    database.write(writeOptions, batch);
                    nextIds[ownerId] = committedNextId;
                }
                //nextIds.put(ownerId, committedNextId);
                allocated.forEach((value, id) -> {
                    resolved.put(value, id);
                    hotValues.put(new CacheKey(ownerId, value), id);
                });
            }
            return resolved;
        } catch (RocksDBException exception) {
            throw storageFailure("resolve owner " + ownerId + " batch", exception);
        }
    }

    public int size(int ownerId) {
        validateOwner(ownerId);
        ensureOpen();
            try {
                return nextId(ownerId);
            } catch (RocksDBException exception) {
                throw storageFailure("read owner " + ownerId + " size", exception);
            }
    }

    private int nextId(int ownerId) throws RocksDBException {
        Integer cached = nextIds[ownerId];
        if (cached >= 0)
            return cached;
        byte[] stored = database.get(nextIdKeys[ownerId]);
        int next = stored == null ? 0 : decodeId(stored);
        //nextIds.put(ownerId, next);
        nextIds[ownerId]=next;
        return next;
    }

    private int globalId(int ownerId, int localId) {
        long id = (long) localId * ownerCount + ownerId;
        if (localId < 0 || id > Integer.MAX_VALUE
                || ((long) localId + 1) * ownerCount + ownerId > Integer.MAX_VALUE)
            throw new IllegalStateException("Value-ID space exhausted for owner " + ownerId);
        return (int) id;
    }

    private void validateOwner(int ownerId) {
        if (ownerId < 0 || ownerId >= ownerCount)
            throw new IllegalArgumentException("ownerId must be in [0, ownerCount)");
    }

    private static byte[] valueKey(int ownerId, String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + Integer.BYTES + text.length)
                .put(VALUE_PREFIX).putInt(ownerId).put(text).array();
    }

    // private static byte[] nextIdKey(int ownerId) {
    //     return ByteBuffer.allocate(1 + Integer.BYTES).put(NEXT_ID_PREFIX).putInt(ownerId).array();
    // }

    private static byte[] encodeId(int id) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(id).array();
    }

    private static int decodeId(byte[] bytes) {
        if (bytes.length != Integer.BYTES)
            throw new IllegalStateException("Invalid value-ID record length: " + bytes.length);
        return ByteBuffer.wrap(bytes).getInt();
    }

    private void ensureOpen() {
        if (closed)
            throw new IllegalStateException("WorkerValueIdStore is closed");
    }

    private IllegalStateException storageFailure(String operation, RocksDBException cause) {
        return new IllegalStateException(
                "Cannot " + operation + " in worker value-ID RocksDB at " + databasePath, cause);
    }

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        hotValues.invalidateAll();
        //nextIds.clear();
        database.close();
        writeOptions.close();
        options.close();
        bloomFilter.close();
    }
}
