package FileOutputFormatters;

import RecordWriters.JsonRecordWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * FileOutputFormat implementation to write to JSON files
 */
public class JsonOutputFormat extends FileOutputFormat<Text, JsonNode> {
    private static final String filename = "part";
    private static final String extension = ".json";

    private final ObjectMapper mapper;

    /**
     * Default empty constructor
     */
    public JsonOutputFormat() {
        mapper = new ObjectMapper();
    }

    /**
     * Creates and returns a record writer that outputs to a JSON file
     * @param job Job context instance
     * @return JsonRecordWriter in job context
     * @throws IOException Related to file creation and write operations
     */
    @Override
    public RecordWriter<Text, JsonNode> getRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration conf = job.getConfiguration();

        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(job);
        Path file = new Path(committer.getWorkPath(), getUniqueFile(job, filename, extension));
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream out = fs.create(file, false);

        return new JsonRecordWriter(out, mapper);
    }
}
