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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Helper {
    private static final Integer BUFFER_SIZE = 1048576; // 1MB Buffer

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

    public static String glueDirWithFile(String dir, String file) {
        String glue = dir.matches(".*/$") ? "" : "/";
        return dir + glue + file;
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

    public static Map<String, String> getInputData(String[] args) {
        Map<String, String> options = new HashMap<>();
        Pattern optionPattern = Pattern.compile("(?<=--)(.*?)(?==)");
        Pattern valuePattern = Pattern.compile("(?<==)(.*)");

        for(String arg: args) {
            Matcher optionMatcher = optionPattern.matcher(arg);
            Matcher valueMatcher = valuePattern.matcher(arg);

            if (optionMatcher.find() && valueMatcher.find()) {
                options.put(optionMatcher.group(), valueMatcher.group());
            }
        }

        return options;
    }

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

    public static List<String> missingOptions(Map<String, String> options, List<String> required) {
        return required.stream().filter(a ->  !options.containsKey(a)).collect(Collectors.toList());
    }

    public static String missingOptionsString(List<String> missingOptions) {
        StringBuilder sb = new StringBuilder();

        for (String option: missingOptions) {
            sb.append("--").append(option).append(" ");
        }

        return sb.toString();
    }
}
