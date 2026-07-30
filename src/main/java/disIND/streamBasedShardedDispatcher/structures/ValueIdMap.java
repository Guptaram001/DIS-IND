package disIND.streamBasedShardedDispatcher.structures;

import disIND.streamBasedShardedDispatcher.utility.UserConfig;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


public final class ValueIdMap implements AutoCloseable {
    private static final byte VALUE_KEY_PREFIX = 0;
    private static final byte[] NEXT_ID_KEY = new byte[] {1, 'n', 'e', 'x', 't', '-', 'i', 'd'};

    static {
        RocksDB.loadLibrary();
    }

    private final boolean storeValueStrings;
    private final int maxHotEntries;
    private final Map<String, Integer> hotValues;
    private final Path databasePath;
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB database;
    private int nextId;
    private boolean closed;

    public ValueIdMap() {
        this(UserConfig.STORE_VALUE_STRINGS, UserConfig.VALUE_ID_HOT_ENTRIES,databasePath(UserConfig.STORE_VALUE_STRINGS, 
            UserConfig.VALUE_ID_DISK_DIR));
    }


    public ValueIdMap(Path databasePath, int maxHotEntries) {
        this(true, maxHotEntries, databasePath);
    }

    private ValueIdMap(boolean storeValueStrings, int maxHotEntries, Path databasePath) {
        if (maxHotEntries < 0) {
            throw new IllegalArgumentException("maxHotEntries must be zero or greater");
        }
        this.storeValueStrings = storeValueStrings;
        this.maxHotEntries = maxHotEntries;
        this.databasePath = databasePath;
        this.hotValues = new LinkedHashMap<>(Math.max(16, maxHotEntries), 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
                return size() > ValueIdMap.this.maxHotEntries;
            }
        };

        if (!storeValueStrings) {
            this.options = null;
            this.writeOptions = null;
            this.database = null;
            return;
        }

        try {
            Files.createDirectories(databasePath);
            this.options = new Options().setCreateIfMissing(true);
            this.writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);
            this.database = RocksDB.open(options, databasePath.toString());
            byte[] storedNextId = database.get(NEXT_ID_KEY);
            this.nextId = storedNextId == null ? 0 : decodeId(storedNextId);
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("Cannot open value-ID RocksDB at " + databasePath, exception);
        }
    }


    public synchronized Map<String, Integer> resolveBatch(Collection<String> values) {
        Objects.requireNonNull(values, "values");
        ensureOpen();

        LinkedHashSet<String> distinctValues = new LinkedHashSet<>(values.size());
        for (String value : values) {
            distinctValues.add(Objects.requireNonNull(value, "value"));
        }

        Map<String, Integer> resolved = new LinkedHashMap<>(distinctValues.size());
        if (!storeValueStrings) {
            for (String value : distinctValues) {
                resolved.put(value, stableHashId(value));
            }
            return resolved;
        }

        List<String> coldValues = new ArrayList<>();
        List<byte[]> coldKeys = new ArrayList<>();
        for (String value : distinctValues) {
            Integer hotId = hotValues.get(value);
            if (hotId != null) {
                resolved.put(value, hotId);
            } else {
                coldValues.add(value);
                coldKeys.add(valueKey(value));
            }
        }
        if (coldKeys.isEmpty()) {
            return resolved;
        }

        try {
            List<byte[]> storedIds = database.multiGetAsList(coldKeys);
            List<Integer> missingIndexes = new ArrayList<>();
            for (int index = 0; index < coldValues.size(); index++) {
                String value = coldValues.get(index);
                byte[] storedId = storedIds.get(index);
                if (storedId == null) {
                    missingIndexes.add(index);
                    continue;
                }
                int id = decodeId(storedId);
                resolved.put(value, id);
                hotValues.put(value, id);
            }

            if (!missingIndexes.isEmpty()) {
                try (WriteBatch writeBatch = new WriteBatch()) {
                    for (int index : missingIndexes) {
                        if (nextId == Integer.MAX_VALUE) {
                            throw new IllegalStateException("Value-ID space exhausted at " + Integer.MAX_VALUE);
                        }
                        String value = coldValues.get(index);
                        int id = nextId++;
                        writeBatch.put(coldKeys.get(index), encodeId(id));
                        resolved.put(value, id);
                        hotValues.put(value, id);
                    }
                    writeBatch.put(NEXT_ID_KEY, encodeId(nextId));
                    database.write(writeOptions, writeBatch);
                }
            }
            return resolved;
        } catch (RocksDBException exception) {
            throw storageFailure("resolve batch", exception);
        }
    }

    public int getOrInsert(String value) {
        return resolveBatch(List.of(value)).get(value);
    }

    public synchronized Optional<Integer> getId(String value) {
        Objects.requireNonNull(value, "value");
        ensureOpen();
        if (!storeValueStrings) {
            return Optional.of(stableHashId(value));
        }

        Integer hotId = hotValues.get(value);
        if (hotId != null) {
            return Optional.of(hotId);
        }
        try {
            byte[] storedId = database.get(valueKey(value));
            if (storedId == null) {
                return Optional.empty();
            }
            int id = decodeId(storedId);
            hotValues.put(value, id);
            return Optional.of(id);
        } catch (RocksDBException exception) {
            throw storageFailure("read", exception);
        }
    }

    public synchronized int size() {
        return storeValueStrings ? nextId : 0;
    }

    public Path coldValuesPath() {
        return databasePath;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        hotValues.clear();
        if (database != null) {
            database.close();
            writeOptions.close();
            options.close();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ValueIdMap is closed");
        }
    }

    private IllegalStateException storageFailure(String operation, RocksDBException cause) {
        return new IllegalStateException("Cannot " + operation + " in value-ID RocksDB at " + databasePath, cause);
    }

    private static Path databasePath(boolean enabled, String directory) {
        Path base = Path.of(directory);
        if (!enabled) {
            return base;
        }
        try {
            Files.createDirectories(base);
            return Files.createTempDirectory(base, "value-ids-");
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot create value-ID RocksDB in " + directory, exception);
        }
    }

    private static byte[] valueKey(String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[text.length + 1];
        key[0] = VALUE_KEY_PREFIX;
        System.arraycopy(text, 0, key, 1, text.length);
        return key;
    }

    private static byte[] encodeId(int id) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(id).array();
    }

    private static int decodeId(byte[] bytes) {
        if (bytes.length != Integer.BYTES) {
            throw new IllegalStateException("Invalid value-ID record length: " + bytes.length);
        }
        return ByteBuffer.wrap(bytes).getInt();
    }

    private static int stableHashId(String value) {
        long hash = Hashing.mix64(value.hashCode());
        return (int) (hash & 0x7fffffffL);
    }
}
