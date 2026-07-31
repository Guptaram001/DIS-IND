package disIND.streamBasedShardedDispatcher.structures;

import org.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

/**
 * One RocksDB instance shared by all AAs on a node. 
 */
public final class NodeValueToRowsStore implements AutoCloseable {
    private static final byte VALUE_ROWS_PREFIX = 1;
    private static final byte CURRENT_PREFIX = 2;

    static {
        RocksDB.loadLibrary();
    }

    private final Path databasePath;
    private final Options options;
    private final WriteOptions durableWriteOptions;
    private final RocksDB database;
    private boolean closed;

    public NodeValueToRowsStore(Path databasePath) {
        this.databasePath = databasePath;
        try {
            Files.createDirectories(databasePath);
            this.options = new Options().setCreateIfMissing(true);
            this.durableWriteOptions = new WriteOptions().setDisableWAL(false).setSync(true);
            this.database = RocksDB.open(options, databasePath.toString());
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("Cannot open RocksDB "+ databasePath, exception);
        }
    }

    public synchronized void mergeCheckpoint(int colId, int round, ValueToRowsStore delta) {
        ensureOpen();
        OptionalInt previousRound = currentRound(colId);
        if (previousRound.isPresent() && round <= previousRound.getAsInt()) {
            if (round == previousRound.getAsInt() && delta.isEmpty()) 
                return;
            throw new IllegalArgumentException("Checkpoint round " + round+ " must be newer than current round " 
            + previousRound.getAsInt()+ " for column " + colId);
        }

        try (WriteBatch checkpoint = new WriteBatch()) {
            try {
                delta.forEach((valueId, newRows) -> mergeValue(colId, valueId, newRows, checkpoint));
            } catch (StorageRuntimeException exception) {
                throw exception.rocksCause;
            }
            checkpoint.put(currentKey(colId), encodeInt(round));
            database.write(durableWriteOptions, checkpoint);
        } catch (RocksDBException exception) {
            throw storageFailure("merge checkpoint " + round + " for column " + colId, exception);
        }
    }

    private void mergeValue(int colId, int valueId, IntArrayList newRows, WriteBatch checkpoint) {
        try {
            byte[] key = valueRowsKey(colId, valueId);
            byte[] existingBytes = database.get(key);
            int[] existingRows = existingBytes == null ? new int[0] : decodeRows(existingBytes);
            checkpoint.put(key, encodeMergedRows(existingRows, newRows));
        } catch (RocksDBException exception) {
            throw new StorageRuntimeException(exception);
        }
    }

    public synchronized OptionalInt currentRound(int colId) {
        ensureOpen();
        try {
            byte[] value = database.get(currentKey(colId));
            return value == null ? OptionalInt.empty() : OptionalInt.of(decodeInt(value));
        } catch (RocksDBException exception) {
            throw storageFailure("read current checkpoint for column " + colId, exception);
        }
    }

    public synchronized Map<Integer, int[]> readCurrentSnapshot(int colId) {
        ensureOpen();
        if (currentRound(colId).isEmpty()) 
            return Map.of();
        
        Map<Integer, int[]> result = new HashMap<>();
        byte[] prefix = valueRowsPrefix(colId);
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seek(prefix);
                 iterator.isValid() && hasPrefix(iterator.key(), prefix);
                 iterator.next()) {
                result.put(decodeValueId(iterator.key()), decodeRows(iterator.value()));
            }
            iterator.status();
            return result;
        } catch (RocksDBException exception) {
            throw storageFailure("read current checkpoint for column " + colId, exception);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) 
            return;
        closed = true;
        database.close();
        durableWriteOptions.close();
        options.close();
    }

    private static byte[] encodeMergedRows(int[] existingRows, IntArrayList newRows) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Math.multiplyExact(existingRows.length + newRows.size(), Integer.BYTES));
        buffer.putInt(existingRows.length + newRows.size());
        for (int row : existingRows) 
            buffer.putInt(row);
        
        newRows.forEach((int row) -> buffer.putInt(row));
        return buffer.array();
    }

    private static int[] decodeRows(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.remaining() < Integer.BYTES) 
            throw new IllegalStateException("Invalid rows record header");
        int count = buffer.getInt();
        if (count < 0 || buffer.remaining() != Math.multiplyExact(count, Integer.BYTES)) 
            throw new IllegalStateException("Invalid rows record length");
        
        int[] rows = new int[count];
        for (int index = 0; index < count; index++) {
            rows[index] = buffer.getInt();
        }
        return rows;
    }

    private static byte[] valueRowsKey(int colId, int valueId) {
        return ByteBuffer.allocate(1 + Integer.BYTES * 2).put(VALUE_ROWS_PREFIX).putInt(colId).putInt(valueId).array();
    }

    private static byte[] valueRowsPrefix(int colId) {
        return ByteBuffer.allocate(1 + Integer.BYTES).put(VALUE_ROWS_PREFIX).putInt(colId).array();
    }

    private static byte[] currentKey(int colId) {
        return ByteBuffer.allocate(1 + Integer.BYTES).put(CURRENT_PREFIX).putInt(colId).array();
    }

    private static int decodeValueId(byte[] key) {
        if (key.length != 1 + Integer.BYTES * 2 || key[0] != VALUE_ROWS_PREFIX) 
            throw new IllegalStateException("Invalid value-to-rows key");
        
        return ByteBuffer.wrap(key, 1 + Integer.BYTES, Integer.BYTES).getInt();
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

    private static byte[] encodeInt(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static int decodeInt(byte[] bytes) {
        if (bytes.length != Integer.BYTES) 
            throw new IllegalStateException("Invalid integer record length: " + bytes.length);
        return ByteBuffer.wrap(bytes).getInt();
    }

    private void ensureOpen() {
        if (closed)
            throw new IllegalStateException("Value-to-rows RocksDB is closed");
    }

    private IllegalStateException storageFailure(String operation, RocksDBException cause) {
        return new IllegalStateException("Cannot " + operation
                + " in value-to-rows RocksDB at " + databasePath, cause);
    }

    private static final class StorageRuntimeException extends RuntimeException {
        private final RocksDBException rocksCause;

        private StorageRuntimeException(RocksDBException rocksCause) {
            this.rocksCause = rocksCause;
        }
    }
}
