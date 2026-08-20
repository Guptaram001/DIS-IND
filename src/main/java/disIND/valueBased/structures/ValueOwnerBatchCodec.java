package disIND.valueBased.structures;

//import com.github.luben.zstd.Zstd;
import disIND.valueBased.model.AkkaSerializable;
import disIND.valueBased.protocol.ValueOwnerProtocol.ColumnValues;
import disIND.valueBased.protocol.ValueOwnerProtocol.ValueRows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Compact, Zstandard-compressed wire representation of a value-owner batch. */
public final class ValueOwnerBatchCodec {
    // private static final int FORMAT_VERSION = 1;
    // private static final int COMPRESSION_LEVEL = 3;
    // private static final int COMPRESSION_THRESHOLD_BYTES = 1_024;
    // private static final int MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024;
    // private static final int MAX_COLLECTION_ENTRIES = 50_000_000;

    // private ValueOwnerBatchCodec() {}

    // public record Payload(int formatVersion, boolean compressed, int uncompressedSize, byte[] bytes)
    //         implements AkkaSerializable {
    //     public Payload {
    //         if (formatVersion <= 0)
    //             throw new IllegalArgumentException("formatVersion must be positive");
    //         if (uncompressedSize < 0 || uncompressedSize > MAX_UNCOMPRESSED_BYTES)
    //             throw new IllegalArgumentException("Invalid uncompressed batch size: " + uncompressedSize);
    //         bytes = Objects.requireNonNull(bytes, "bytes").clone();
    //         if (!compressed && bytes.length != uncompressedSize)
    //             throw new IllegalArgumentException("Uncompressed payload length does not match its declared size");
    //     }
    // }

    // public static Payload encode(List<ColumnValues> columns) {
    //     Objects.requireNonNull(columns, "columns");
    //     byte[] encoded = encodeColumns(columns);
    //     if (encoded.length < COMPRESSION_THRESHOLD_BYTES)
    //         return new Payload(FORMAT_VERSION, false, encoded.length, encoded);

    //     byte[] compressed = Zstd.compress(encoded, COMPRESSION_LEVEL);
    //     if (compressed.length >= encoded.length)
    //         return new Payload(FORMAT_VERSION, false, encoded.length, encoded);
    //     return new Payload(FORMAT_VERSION, true, encoded.length, compressed);
    // }

    // public static List<ColumnValues> decode(Payload payload) {
    //     Objects.requireNonNull(payload, "payload");
    //     if (payload.formatVersion() != FORMAT_VERSION)
    //         throw new IllegalArgumentException("Unsupported value-owner batch format: " + payload.formatVersion());

    //     byte[] encoded = payload.compressed()
    //             ? Zstd.decompress(payload.bytes(), payload.uncompressedSize())
    //             : payload.bytes();
    //     if (encoded.length != payload.uncompressedSize())
    //         throw new IllegalArgumentException("Decoded value-owner batch has an unexpected size");
    //     return decodeColumns(encoded);
    // }

    // private static byte[] encodeColumns(List<ColumnValues> columns) {
    //     try {
    //         ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    //         try (DataOutputStream output = new DataOutputStream(bytes)) {
    //             output.writeInt(columns.size());
    //             for (ColumnValues column : columns) {
    //                 output.writeInt(column.colId());
    //                 output.writeInt(column.values().size());
    //                 for (ValueRows valueRows : column.values()) {
    //                     byte[] value = valueRows.value().getBytes(StandardCharsets.UTF_8);
    //                     output.writeInt(value.length);
    //                     output.write(value);
    //                     output.writeInt(valueRows.rowIds().length);
    //                     for (long rowId : valueRows.rowIds())
    //                         output.writeLong(rowId);
    //                 }
    //             }
    //         }
    //         byte[] encoded = bytes.toByteArray();
    //         if (encoded.length > MAX_UNCOMPRESSED_BYTES)
    //             throw new IllegalArgumentException("Value-owner batch exceeds the maximum encoded size: "
    //                     + encoded.length);
    //         return encoded;
    //     } catch (IOException exception) {
    //         throw new IllegalStateException("Cannot encode value-owner batch", exception);
    //     }
    // }

    // private static List<ColumnValues> decodeColumns(byte[] encoded) {
    //     try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
    //         int columnCount = readCount(input, "column count");
    //         List<ColumnValues> columns = new ArrayList<>(columnCount);
    //         for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
    //             int columnId = input.readInt();
    //             int valueCount = readCount(input, "value count");
    //             List<ValueRows> values = new ArrayList<>(valueCount);
    //             for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
    //                 int valueLength = readCount(input, "value length");
    //                 if (valueLength > input.available())
    //                     throw new EOFException("Value length exceeds the remaining batch bytes");
    //                 String value = new String(input.readNBytes(valueLength), StandardCharsets.UTF_8);
    //                 int rowCount = readCount(input, "row count");
    //                 if ((long) rowCount * Long.BYTES > input.available())
    //                     throw new EOFException("Row count exceeds the remaining batch bytes");
    //                 long[] rowIds = new long[rowCount];
    //                 for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
    //                     rowIds[rowIndex] = input.readLong();
    //                 values.add(new ValueRows(value, rowIds));
    //             }
    //             columns.add(new ColumnValues(columnId, values));
    //         }
    //         if (input.available() != 0)
    //             throw new IllegalArgumentException("Value-owner batch contains trailing bytes");
    //         return List.copyOf(columns);
    //     } catch (IOException exception) {
    //         throw new IllegalArgumentException("Cannot decode value-owner batch", exception);
    //     }
    // }

    // private static int readCount(DataInputStream input, String label) throws IOException {
    //     int count = input.readInt();
    //     if (count < 0 || count > MAX_COLLECTION_ENTRIES)
    //         throw new IllegalArgumentException("Invalid " + label + ": " + count);
    //     return count;
    // }
}
