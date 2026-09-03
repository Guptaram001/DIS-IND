package disIND.valueBased.structures;

import com.github.luben.zstd.Zstd;
import disIND.valueBased.model.SharedModel.DataOrientation;
import disIND.valueBased.protocol.ValueOwnerProtocol.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Versioned, bounded binary wire codec for value-owner batches. */
public final class ValueOwnerBatchCodec {
    static final int FORMAT_VERSION = 1;
    static final int COMPRESSION_THRESHOLD_BYTES = 4 * 1024;
    static final int MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024;
    private static final int MAX_COLLECTION_ENTRIES = 50_000_000;
    private ValueOwnerBatchCodec() {}

    public static BatchBody encode(DataOrientation orientation, BatchBody body) {
        Objects.requireNonNull(orientation); Objects.requireNonNull(body);
        if (body instanceof CompressedBatch) return body;
        byte[] raw = encodeBinary(orientation, body);
        if (raw.length < COMPRESSION_THRESHOLD_BYTES) return body;
        byte[] compressed = Zstd.compress(raw, 1);
        if (compressed.length >= raw.length) return body;
        return new CompressedBatch(FORMAT_VERSION, raw.length, compressed);
    }

    public static BatchBody decode(DataOrientation orientation, BatchBody body) {
        if (!(body instanceof CompressedBatch payload)) return body;
        if (payload.formatVersion() != FORMAT_VERSION) throw new IllegalArgumentException("Unsupported batch format");
        if (payload.uncompressedSize() > MAX_UNCOMPRESSED_BYTES) throw new IllegalArgumentException("Decoded batch too large");
        byte[] raw = new byte[payload.uncompressedSize()];
        final long size;
        try {
            size = Zstd.decompress(raw, payload.bytes());
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Invalid Zstandard batch", exception);
        }
        if (Zstd.isError(size) || size != raw.length) throw new IllegalArgumentException("Invalid Zstandard batch");
        return decodeBinary(orientation, raw);
    }

    private static byte[] encodeBinary(DataOrientation orientation, BatchBody body) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                if (orientation == DataOrientation.VALUE_MAJOR && body instanceof ValueMajorBatch batch) {
                    out.writeInt(batch.values().size());
                    for (ValueData value : batch.values()) {
                        writeString(out, value.value()); out.writeInt(value.columns().size());
                        for (ColumnRows col : value.columns()) { out.writeInt(col.columnId()); out.writeInt(col.count()); }
                    }
                } else if (orientation == DataOrientation.COLUMN_MAJOR && body instanceof ColumnMajorBatch batch) {
                    out.writeInt(batch.columns().size());
                    for (ColumnValues col : batch.columns()) {
                        out.writeInt(col.colId()); out.writeInt(col.values().size());
                        for (ValueRows value : col.values()) {
                            writeString(out, value.value()); out.writeInt(value.rowIds().length);
                            for (long rowId : value.rowIds()) out.writeLong(rowId);
                        }
                    }
                } else throw new IllegalArgumentException("Batch body does not match " + orientation);
            }
            byte[] result = bytes.toByteArray();
            if (result.length > MAX_UNCOMPRESSED_BYTES) throw new IllegalArgumentException("Batch too large");
            return result;
        } catch (IOException e) { throw new IllegalStateException("Cannot encode batch", e); }
    }

    private static BatchBody decodeBinary(DataOrientation orientation, byte[] raw) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw))) {
            int outer = readCount(in, "outer count"); BatchBody result;
            if (orientation == DataOrientation.VALUE_MAJOR) {
                List<ValueData> values = new ArrayList<>(outer);
                for (int i = 0; i < outer; i++) {
                    String value = readString(in); int count = readCount(in, "column count");
                    List<ColumnRows> columns = new ArrayList<>(count);
                    for (int j = 0; j < count; j++) columns.add(new ColumnRows(in.readInt(), in.readInt()));
                    values.add(new ValueData(value, List.copyOf(columns)));
                }
                result = new ValueMajorBatch(List.copyOf(values));
            } else {
                List<ColumnValues> columns = new ArrayList<>(outer);
                for (int i = 0; i < outer; i++) {
                    int colId = in.readInt(), count = readCount(in, "value count");
                    List<ValueRows> values = new ArrayList<>(count);
                    for (int j = 0; j < count; j++) {
                        String value = readString(in); int rows = readCount(in, "row count");
                        if ((long) rows * Long.BYTES > in.available()) throw new EOFException("Invalid row count");
                        long[] ids = new long[rows]; for (int k = 0; k < rows; k++) ids[k] = in.readLong();
                        values.add(new ValueRows(value, ids));
                    }
                    columns.add(new ColumnValues(colId, List.copyOf(values)));
                }
                result = new ColumnMajorBatch(List.copyOf(columns));
            }
            if (in.available() != 0) throw new IllegalArgumentException("Trailing batch bytes");
            return result;
        } catch (IOException e) { throw new IllegalArgumentException("Cannot decode batch", e); }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] data = Objects.requireNonNull(value).getBytes(StandardCharsets.UTF_8); out.writeInt(data.length); out.write(data);
    }
    private static String readString(DataInputStream in) throws IOException {
        int length = readCount(in, "string length");
        if (length > in.available()) throw new EOFException("Invalid string length");
        return new String(in.readNBytes(length), StandardCharsets.UTF_8);
    }
    private static int readCount(DataInputStream in, String name) throws IOException {
        int value = in.readInt();
        if (value < 0 || value > MAX_COLLECTION_ENTRIES) throw new IllegalArgumentException("Invalid " + name + ": " + value);
        return value;
    }
}
