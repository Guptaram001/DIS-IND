package disIND.streamBasedShardedDispatcher.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Encodes RoaringBitmap with its stable binary format.
 */
public final class RoaringBitmapJacksonModule extends SimpleModule {

    public RoaringBitmapJacksonModule() {
        super("dis-ind-roaring-bitmap");
        addSerializer(RoaringBitmap.class, new BitmapSerializer());
        addDeserializer(RoaringBitmap.class, new BitmapDeserializer());
    }

    private static final class BitmapSerializer extends JsonSerializer<RoaringBitmap> {
        @Override
        public void serialize(
                RoaringBitmap bitmap, JsonGenerator generator, SerializerProvider serializers)
                throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(bitmap.serializedSizeInBytes());
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                bitmap.serialize(output);
            }
            generator.writeBinary(bytes.toByteArray());
        }
    }

    private static final class BitmapDeserializer extends JsonDeserializer<RoaringBitmap> {
        @Override
        public RoaringBitmap deserialize(JsonParser parser, DeserializationContext context)
                throws IOException {
            RoaringBitmap bitmap = new RoaringBitmap();
            try (DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(parser.getBinaryValue()))) {
                bitmap.deserialize(input);
            }
            return bitmap;
        }
    }
}
