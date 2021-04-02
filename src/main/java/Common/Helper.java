package Common;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Helper {
    private static final Integer BUFFER_SIZE = 1048576; // 1MB Buffer

    public static String readFile(String filePath) throws IOException {
        FSDataInputStream stream = FileSystem.get(new Configuration()).open(new Path(filePath));
        byte[] buffer = new byte[BUFFER_SIZE];
        StringBuilder dataFile = new StringBuilder();
        int readValue;

        readValue = stream.read(buffer);
        while (readValue != -1) {
            String dataRead = (readValue == BUFFER_SIZE) ?
                    new String(buffer, StandardCharsets.UTF_8) :
                    new String(Arrays.copyOf(buffer, readValue));
            dataFile.append(dataRead);

            readValue = stream.read(buffer);
        }

        stream.close();

        return dataFile.toString();
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
