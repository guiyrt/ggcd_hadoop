package Common;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Definition of IO related methods
 */
public class IO {
    private static final Integer BUFFER_SIZE = 1048576; // 1MB Buffer

    /**
     * Given a file path, returns a String with content
     * @param filePath Path to file
     * @return File in String format
     * @throws IOException Read operations might throw this exception
     */
    public static String readFile(String filePath) throws IOException {
        FSDataInputStream stream = getFileInputStream(filePath);
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

    /**
     * Given a path to a schema, returns the associated Schema instance
     * @param schemaPath Path to schema file
     * @return Correspondent schema instance
     * @throws IOException Read operations might throw this exception
     */
    public static Schema readSchema(String schemaPath) throws IOException {
        MessageType mt = MessageTypeParser.parseMessageType(readFile(schemaPath));
        return new AvroSchemaConverter().convert(mt);
    }

    /**
     * Gets the filesystem related to a given path
     * @param path Path to a Google Storage bucket
     * @return Filesystem to interact with bucket
     * @throws IOException Read operations might throw this exception
     */
    public static GoogleHadoopFileSystem getGS(String path) throws IOException {
        Pattern bucketPattern = Pattern.compile("(?<=//)(.*?)(?=/)");
        Matcher bucketMatcher = bucketPattern.matcher(path);
        GoogleHadoopFileSystem gs = new GoogleHadoopFileSystem();

        if (bucketMatcher.find()) {
            String bucket = bucketMatcher.group();

            try {
                gs.initialize(new URI("gs://" + bucket), new Configuration());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        return gs;

    }

    /**
     * Given a folder in a bucket, deletes it
     * @param path Path to folder
     * @throws IOException Read operations might throw this exception
     */
    public static void deleteFolder(String path) throws IOException {
        if (path.contains("gs://")) {
            GoogleHadoopFileSystem gs = getGS(path);
            gs.delete(new Path(path), true);
        }

        else {
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(new Path(path), true);
        }
    }

    /**
     * Given a file path, returns the correct inputStream to interact with
     * @param path Path to file
     * @return InputStream to interact with file
     * @throws IOException Read operations might throw this exception
     */
    public static FSDataInputStream getFileInputStream(String path) throws IOException {
        if (path.contains("gs://")) {
            GoogleHadoopFileSystem gs = getGS(path);
            return gs.open(new Path(path));
        }

        else {
            FileSystem fs = FileSystem.get(new Configuration());
            return fs.open(new Path(path));
        }
    }
}
