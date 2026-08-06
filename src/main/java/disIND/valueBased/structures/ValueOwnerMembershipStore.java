package disIND.valueBased.structures;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import disIND.valueBased.utility.UserConfig;

/**
 * Worker-local, disk-backed membership state shared by all ValueOwner
 */
public final class ValueOwnerMembershipStore implements AutoCloseable {
    private static final float BLOOM_FILTER_BITS_PER_KEY = UserConfig.BLOOM_FILTER_BITS_PER_KEY;

    private record CacheKey(int bucketId, int valueId) {}
    public record MembershipWrite(int bucketId, Map<Integer, Map<Integer, Long>> changedRecords) {}

    static {
        RocksDB.loadLibrary();
    }

    private final BloomFilter bloomFilter;
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;
    private final Cache<CacheKey, Map<Integer, Long>> hotCache;

    public ValueOwnerMembershipStore(Path directory, long hotEntries) {
        try {
            Files.createDirectories(directory);
            this.bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY, false);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setFilterPolicy(bloomFilter)
                    .setWholeKeyFiltering(true);
            this.options = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig);
            this.writeOptions = new WriteOptions();
            this.db = RocksDB.open(options, directory.toString());
            this.hotCache = CacheBuilder.newBuilder()
                    .maximumSize(hotEntries)
                    .build();
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException(
                    "Unable to open ValueOwner membership store at " + directory, exception);
        }
    }


    public Map<Integer, Map<Integer, Long>> loadBatch(int bucketId, Set<Integer> valueIds) {
        Map<Integer, Map<Integer, Long>> result = new HashMap<>(valueIds.size());
        List<Integer> misses = new ArrayList<>();
        List<byte[]> missKeys = new ArrayList<>();

        for (int valueId : valueIds) {
            Map<Integer, Long> cached = hotCache.getIfPresent(new CacheKey(bucketId, valueId));
            if (cached == null) {
                misses.add(valueId);
                missKeys.add(key(bucketId, valueId));
            } else {
                result.put(valueId, new HashMap<>(cached));
            }
        }

        if (!missKeys.isEmpty()) {
            try {
                List<byte[]> encoded = db.multiGetAsList(missKeys);
                for (int index = 0; index < misses.size(); index++) {
                    int valueId = misses.get(index);
                    Map<Integer, Long> record = encoded.get(index) == null
                            ? new HashMap<>()
                            : decode(encoded.get(index));
                    hotCache.put(new CacheKey(bucketId, valueId), Map.copyOf(record));
                    result.put(valueId, record);
                }
            } catch (RocksDBException exception) {
                throw new IllegalStateException(
                        "Unable to batch-load VO bucket " + bucketId, exception);
            }
        }
        return result;
    }


    public void writeBatch(int bucketId, Map<Integer, Map<Integer, Long>> changedRecords) {
        writeBatches(List.of(new MembershipWrite(bucketId, changedRecords)));
    }

    public void writeBatches(List<MembershipWrite> writes) {
        try (WriteBatch batch = new WriteBatch()) {
            for (MembershipWrite write : writes) {
                for (Map.Entry<Integer, Map<Integer, Long>> entry : write.changedRecords().entrySet()) {
                    batch.put(key(write.bucketId(), entry.getKey()), encode(entry.getValue()));
                }
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to persist value-owner write batch", exception);
        }

        for (MembershipWrite write : writes) {
            write.changedRecords().forEach((valueId, record) ->
                    hotCache.put(new CacheKey(write.bucketId(), valueId), Map.copyOf(record)));
        }
    }

    public Map<Integer, Map<Integer, Long>> snapshotBucket(int bucketId) {
        Map<Integer, Map<Integer, Long>> snapshot = new LinkedHashMap<>();
        byte[] prefix = bucketPrefix(bucketId);
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid() && hasPrefix(iterator.key(), prefix)) {
                int valueId = ByteBuffer.wrap(iterator.key()).getInt(Integer.BYTES);
                snapshot.put(valueId, decode(iterator.value()));
                iterator.next();
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw new IllegalStateException(
                    "Unable to read VO bucket snapshot " + bucketId, exception);
        }
        return snapshot;
    }

    private static byte[] bucketPrefix(int bucketId) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(bucketId).array();
    }

    private static byte[] key(int bucketId, int valueId) {
        return ByteBuffer.allocate(Integer.BYTES * 2)
                .putInt(bucketId)
                .putInt(valueId)
                .array();
    }

    private static boolean hasPrefix(byte[] key, byte[] prefix) {
        if (key.length < prefix.length)
            return false;
        for (int index = 0; index < prefix.length; index++) {
            if (key[index] != prefix[index])
                return false;
        }
        return true;
    }

    private static byte[] encode(Map<Integer, Long> columns) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    Integer.BYTES + columns.size() * (Integer.BYTES + Long.BYTES));
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(columns.size());
                columns.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> {
                            try {
                                output.writeInt(entry.getKey());
                                output.writeLong(entry.getValue());
                            } catch (IOException exception) {
                                throw new EncodingException(exception);
                            }
                        });
            }
            return bytes.toByteArray();
        } catch (IOException | EncodingException exception) {
            Throwable cause = exception instanceof EncodingException
                    ? exception.getCause() : exception;
            throw new IllegalStateException("Unable to encode VO membership", cause);
        }
    }

    private static Map<Integer, Long> decode(byte[] encoded) {
        try (DataInputStream input =
                     new DataInputStream(new ByteArrayInputStream(encoded))) {
            int size = input.readInt();
            Map<Integer, Long> columns = new LinkedHashMap<>(size);
            for (int index = 0; index < size; index++)
                columns.put(input.readInt(), input.readLong());
            return columns;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to decode VO membership", exception);
        }
    }

    private static final class EncodingException extends RuntimeException {
        private EncodingException(IOException cause) {
            super(cause);
        }
    }

    @Override
    public void close() {
        hotCache.invalidateAll();
        db.close();
        writeOptions.close();
        options.close();
        bloomFilter.close();
    }
}
