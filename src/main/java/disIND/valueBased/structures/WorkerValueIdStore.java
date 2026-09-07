package disIND.valueBased.structures;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Arrays;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueData;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import disIND.valueBased.monitor.WorkerValueIdMetrics;
import disIND.valueBased.monitor.WorkerValueIdMetrics.Snapshot;

/**
 * One RocksDB db with one bounded LRU cache for each VOs.
 */
public final class WorkerValueIdStore implements AutoCloseable {
    private static final byte VALUE_PREFIX = 0;
    private static final byte NEXT_ID_PREFIX = 1;
    private static final long WRITE_BUFFER_BYTES = 32L * 1024 * 1024;
    private static final long MAX_TOTAL_WAL_BYTES = 128L * 1024 * 1024;
    private static final float BLOOM_FILTER_BITS_PER_KEY = UserConfig.BLOOM_FILTER_BITS_PER_KEY;
    public static final int UNRESOLVED = Integer.MIN_VALUE; // Keys missing update to min value as ids

    static {
        RocksDB.loadLibrary();
    }

    private final int ownerCount;
    private final Path databasePath;
    // Since each value is ultimately mapped to one bucket, ownercache[x] -> cache
    // for owner x.
    private final OwnerCache[] ownerCaches;
    // In memory next id for each owner.
    private final int[] nextIds;
    // Rocks db key to retrieve nextId for the owner, stores owener metadata to
    // fetch id.
    private final byte[][] nextIdKeys;
    private final BloomFilter bloomFilter;
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB database;
    private volatile boolean closed;
    private final int maxHotEntries;
    private final int maxEntriesPerOwner;
    private final WorkerValueIdMetrics metrics;

    private static final class OwnerCache {
        // Cache for value id, like String -> int
        private final Object2IntLinkedOpenHashMap<String> values;

        private OwnerCache(int initialCapacity) {
            values = new Object2IntLinkedOpenHashMap<>(initialCapacity);
            values.defaultReturnValue(UNRESOLVED);
        }
    }

    public WorkerValueIdStore(Path databasePath, int maxHotEntries, int ownerCount,
            WorkerValueIdMetrics metrics) {
        if (maxHotEntries < 0)
            throw new IllegalArgumentException("maxHotEntries must be zero or greater");
        if (ownerCount <= 0)
            throw new IllegalArgumentException("ownerCount must be positive");
        this.databasePath = databasePath;
        this.ownerCount = ownerCount;
        this.nextIds = new int[ownerCount];
        Arrays.fill(nextIds, -1);
        this.nextIdKeys = new byte[ownerCount][];
        this.maxHotEntries = maxHotEntries;
        this.maxEntriesPerOwner = Math.max(1, maxHotEntries / ownerCount);
        this.ownerCaches = new OwnerCache[ownerCount];
        this.metrics = metrics;
        // Stores the nextidkeys metadata precomputed once.
        for (int ownerId = 0; ownerId < ownerCount; ownerId++) {
            byte[] nextIdKey = new byte[1 + Integer.BYTES];
            nextIdKey[0] = NEXT_ID_PREFIX;
            putInt(nextIdKey, 1, ownerId);
            nextIdKeys[ownerId] = nextIdKey;
        }
        try {
            Files.createDirectories(databasePath);
            this.bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY, false);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setFilterPolicy(bloomFilter)
                    .setWholeKeyFiltering(true);
            this.options = new Options();
            this.options.setCreateIfMissing(true);
            this.options.setTableFormatConfig(tableConfig);
            this.options.setWriteBufferSize(WRITE_BUFFER_BYTES);
            this.options.setMaxWriteBufferNumber(2);
            this.options.setMinWriteBufferNumberToMerge(1);
            this.options.setMaxTotalWalSize(MAX_TOTAL_WAL_BYTES);
            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(false).setDisableWAL(false);
            this.database = RocksDB.open(options, databasePath.toString());
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("Cannot open worker value-ID RocksDB at " + databasePath, exception);
        }
    }

    private Object2IntLinkedOpenHashMap<String> getOrCreateOwnerCache(int ownerId) {
        OwnerCache cache = ownerCaches[ownerId];
        if (cache == null) {
            cache = new OwnerCache(Math.min(maxEntriesPerOwner, 1_024));
            ownerCaches[ownerId] = cache;
        }
        return cache.values;
    }

    public Object2IntMap<String> resolveBatch(int ownerId, List<ValueData> values) {
        if (ownerId < 0 || ownerId >= ownerCount)
            throw new IllegalArgumentException("ownerId must be in [0, ownerCount)");
        Objects.requireNonNull(values, "values");
        ensureOpen();
        return resolveOwnerBatch(ownerId, values);
    }

    private Object2IntMap<String> resolveOwnerBatch(int ownerId, List<ValueData> values) {
        Object2IntOpenHashMap<String> resolved = new Object2IntOpenHashMap<>(values.size());
        resolved.defaultReturnValue(UNRESOLVED); // min value as return values so no null checks.

        List<String> coldValues = new ArrayList<>(values.size()); // Not found in cache
        List<byte[]> coldKeys = new ArrayList<>(values.size());
        Object2IntLinkedOpenHashMap<String> cache = getOrCreateOwnerCache(ownerId);

        for (ValueData valueData : values) {
            String value = valueData.value();
            if (resolved.containsKey(value)) // Remove duplicates if any
                continue;

            int cached = cache.getAndMoveToLast(value); // GEts id
            if (cached != UNRESOLVED) {
                metrics.cacheHit();
                resolved.put(value, cached);
            } else {
                metrics.cacheMiss();
                // Mark as seen so duplicates are ignored.
                resolved.put(value, UNRESOLVED);
                coldValues.add(value); // Add to the cold values for db search
                coldKeys.add(valueKey(ownerId, value));
            }
        }
        if (coldKeys.isEmpty())
            return resolved;

        try {
            // For misses in cache, if Id already created in DB, if then fetch.
            long readStarted = System.nanoTime();
            List<byte[]> storedIds;
            try {
                storedIds = database.multiGetAsList(coldKeys);
            } finally {
                metrics.rocksRead(coldKeys.size(), System.nanoTime() - readStarted);
            }
            IntArrayList missingIndexes = new IntArrayList(coldValues.size());
            for (int index = 0; index < coldValues.size(); index++) {
                // Checks for missing cold values in dbstore, to create new ids.
                String value = coldValues.get(index);
                byte[] stored = storedIds.get(index);
                if (stored == null) {
                    missingIndexes.add(index);
                } else {
                    int id = decodeId(stored);
                    resolved.put(value, id);
                    cacheValue(ownerId, value, id);
                }
            }

            // Final creation of new ids
            if (!missingIndexes.isEmpty()) {
                int[] allocatedIds = new int[missingIndexes.size()];
                int committedNextId = nextId(ownerId);

                try (WriteBatch batch = new WriteBatch()) {
                    for (int position = 0; position < missingIndexes.size(); position++) {
                        int index = missingIndexes.getInt(position);
                        // Converts each local nextid for owner to unique global ids.
                        int id = globalId(ownerId, committedNextId++);
                        allocatedIds[position] = id;
                        // Adds the value-id to batch for write.
                        batch.put(coldKeys.get(index), encodeId(id));
                    }

                    batch.put(nextIdKeys[ownerId], encodeId(committedNextId));
                    long writeStarted = System.nanoTime();
                    // Written to file
                    database.write(writeOptions, batch);
                    metrics.rocksWrite(missingIndexes.size() + 1L, System.nanoTime() - writeStarted);
                    nextIds[ownerId] = committedNextId;
                }

                // Adds ids to cache and resolved.
                for (int position = 0; position < missingIndexes.size(); position++) {
                    int index = missingIndexes.getInt(position);
                    String value = coldValues.get(index);
                    int id = allocatedIds[position];
                    resolved.put(value, id);
                    cacheValue(ownerId, value, id);
                }
            }
            return resolved;

        } catch (RocksDBException exception) {
            throw storageFailure("resolve owner " + ownerId + " batch", exception);
        }
    }

    private void cacheValue(int ownerId, String value, int valueId) {
        if (maxHotEntries <= 0)
            return;
        Object2IntLinkedOpenHashMap<String> cache = getOrCreateOwnerCache(ownerId);
        cache.putAndMoveToLast(value, valueId);
        if (cache.size() > maxEntriesPerOwner) {
            cache.removeFirstInt();
            metrics.cacheEviction();
        }
    }

    public Snapshot metricsSnapshot() {
        // Total current entries per worker
        long currentEntries = 0L;
        for (OwnerCache ownerCache : ownerCaches) {
            if (ownerCache != null) {
                currentEntries = Math.addExact(currentEntries, ownerCache.values.size());
            }
        }
        return metrics.snapshot(currentEntries, maxHotEntries);
    }

    public record DBSnapshot(long valueRecords, long valueKeyBytes, long valueBytes,
            long metadataRecords, long metadataKeyBytes, long metadataValueBytes, long physicalDiskBytes) {
        public long logicalBytes() {
            return valueKeyBytes + valueBytes + metadataKeyBytes + metadataValueBytes;
        }
    }

    public DBSnapshot finalStorageSnapshot() {
        // final snapshot calculation for reporting the db memory and other status.
        // Flush pending data to db
        ensureOpen();
        try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
            database.flush(flush);
        } catch (RocksDBException exception) {
            throw storageFailure("flush final value-ID state", exception);
        }

        long valueRecords = 0, valueKeyBytes = 0, valueBytes = 0;
        long metadataRecords = 0, metadataKeyBytes = 0, metadataValueBytes = 0;
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                if (key.length > 0 && key[0] == VALUE_PREFIX) {
                    valueRecords++;
                    valueKeyBytes += key.length;
                    valueBytes += value.length;
                } else if (key.length > 0 && key[0] == NEXT_ID_PREFIX) {
                    metadataRecords++;
                    metadataKeyBytes += key.length;
                    metadataValueBytes += value.length;
                } else {
                    throw new IllegalStateException("Unknown value-ID RocksDB key type");
                }
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw storageFailure("scan final value-ID state", exception);
        }
        return new DBSnapshot(valueRecords, valueKeyBytes, valueBytes,
                metadataRecords, metadataKeyBytes, metadataValueBytes, directoryBytes(databasePath));
    }

    private static long directoryBytes(Path root) {
        // Adds the final data size for each db file.
        try (var paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).mapToLong(path -> {
                try {
                    return Files.size(path);
                } catch (IOException exception) {
                    throw new java.io.UncheckedIOException(exception);
                }
            }).sum();
        } catch (IOException | java.io.UncheckedIOException exception) {
            throw new IllegalStateException("Unable to measure RocksDB directory " + root, exception);
        }
    }

    private int nextId(int ownerId) throws RocksDBException {
        Integer cachedNextId = nextIds[ownerId];
        if (cachedNextId >= 0)
            return cachedNextId;
        long readStarted = System.nanoTime();
        byte[] stored = database.get(nextIdKeys[ownerId]);
        metrics.rocksRead(1, System.nanoTime() - readStarted);
        int next = stored == null ? 0 : decodeId(stored);
        nextIds[ownerId] = next;
        return next;
    }

    private int globalId(int ownerId, int localId) {
        long id = (long) localId * ownerCount + ownerId;
        if (localId < 0 || id > Integer.MAX_VALUE
                || ((long) localId + 1) * ownerCount + ownerId > Integer.MAX_VALUE)
            throw new IllegalStateException("Value-ID space exhausted for owner " + ownerId);
        return (int) id;
    }

    private static byte[] valueKey(int ownerId, String value) {
        // [VALUE_PREFIX: 0][OwnerID: 2]["Hello" : 5 byte representation]
        byte[] text = value.getBytes(StandardCharsets.UTF_8); // Each value string to ind. bytes.
        byte[] key = new byte[1 + Integer.BYTES + text.length];
        key[0] = VALUE_PREFIX;
        putInt(key, 1, ownerId);
        System.arraycopy(text, 0, key, 1 + Integer.BYTES, text.length);
        return key;
    }

    private static byte[] encodeId(int id) {
        byte[] encoded = new byte[Integer.BYTES];
        putInt(encoded, 0, id);
        return encoded;
    }

    private static int decodeId(byte[] bytes) {
        if (bytes.length != Integer.BYTES)
            throw new IllegalStateException("Invalid value-ID record length: " + bytes.length);
        return readInt(bytes, 0);
    }

    private static void putInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static int readInt(byte[] source, int offset) {
        return ((source[offset] & 0xff) << 24) | ((source[offset + 1] & 0xff) << 16)
                | ((source[offset + 2] & 0xff) << 8) | (source[offset + 3] & 0xff);
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

        for (OwnerCache cache : ownerCaches)
            if (cache != null)
                cache.values.clear();

        database.close();
        writeOptions.close();
        options.close();
        bloomFilter.close();
    }
}
