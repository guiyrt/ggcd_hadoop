package Common;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Definition of IO related methods
 */
public class IO {
    private static final Integer BUFFER_SIZE = 1048576; // 1MB Buffer

    /**
     * Given an InputStream, reads the entire file
     * @param in InputStream instance to read from
     * @return Content in String format
     * @throws IOException Related to read operations
     */
    public static String readFromInputStream(InputStream in) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        StringBuilder dataFile = new StringBuilder();
        int readValue;

        readValue = in.read(buffer);
        while (readValue != -1) {
            String dataRead = (readValue == BUFFER_SIZE) ?
                    new String(buffer, StandardCharsets.UTF_8) :
                    new String(Arrays.copyOf(buffer, readValue));
            dataFile.append(dataRead);

            readValue = in.read(buffer);
        }

        in.close();

        return dataFile.toString();
    }

    /**
     * Given a file and a configuration, reads file, even if compressed
     * @param filePath Path to file
     * @param conf Configuration instance
     * @return Content of file, in String format
     * @throws IOException Related to read operations
     */
    public static String readCachedFile(String filePath, Configuration conf) throws IOException {
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path(filePath));
        InputStream dataInputStream = getFileInputStream(filePath);

        // In case file is compressed
        if (Objects.nonNull(codec)) {
            CompressionInputStream compressionInputStream = codec.createInputStream(dataInputStream);
            return readFromInputStream(compressionInputStream);
        }

        // In case file is not compressed or no compatible codec was found
        return readFromInputStream(dataInputStream);
    }

    /**
     * Given a path to a schema, returns the associated Schema instance
     * @param schemaPath Path to schema file
     * @return Correspondent schema instance
     * @throws IOException Read operations might throw this exception
     */
    public static Schema readSchema(String schemaPath) throws IOException {
        MessageType mt = MessageTypeParser.parseMessageType(readFromInputStream(getFileInputStream(schemaPath)));
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
