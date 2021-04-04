package FileOutputFormatters;

import RecordWriters.JsonRecordWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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


public class JsonOutputFormat extends FileOutputFormat<Text, ObjectNode> {
    private static final String filename = "output";
    private static final String extension = ".json";

    private final ObjectMapper mapper;

    public JsonOutputFormat() {
        mapper = new ObjectMapper();
    }

    @Override
    public RecordWriter<Text, ObjectNode> getRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration conf = job.getConfiguration();

        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(job);
        Path file = new Path(committer.getWorkPath(), getUniqueFile(job, filename, extension));
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream out = fs.create(file, false);

        return new JsonRecordWriter(out, mapper);
    }
}
