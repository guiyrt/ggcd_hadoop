import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.io.Text;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

    public static byte[] serializeRecord(Schema schema, GenericRecord gr) throws IOException {
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, byteArray);
        dataFileWriter.append(gr);
        dataFileWriter.close();

        return byteArray.toByteArray();
    }

    public static GenericRecord deserializeRecord(Schema schema, Text data) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(data.getBytes()), datumReader);

        GenericRecord gr = null;
        if (dataFileReader.hasNext()) {
            gr = dataFileReader.next(gr);
        }

        return gr;
    }
}
