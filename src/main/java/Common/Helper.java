package Common;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Helper {
    public static String readFile(String filePath) throws IOException {
        InputStream is = new FileInputStream(filePath);

        String data = new String(is.readAllBytes());
        is.close();

        return data;
    }

    public static Schema getSchema(String schemaPath) throws IOException {
        MessageType mt = MessageTypeParser.parseMessageType(readFile(schemaPath));
        return new AvroSchemaConverter().convert(mt);
    }

    public static Schema getSchemaFromUnion(String fieldName, Schema union) {
        // Input schema must have type UNION
        if (!union.getType().equals(Schema.Type.UNION)) {
            return null;
        }

        for (Schema schema: union.getTypes()) {
            if (schema != null && schema.getName().equals(fieldName)) {
                return schema;
            }
        }

        return null;
    }
}
