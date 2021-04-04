package RecordWriters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

public class JsonRecordWriter extends RecordWriter<Text, ObjectNode> {
    private final ObjectMapper mapper;
    private final ObjectNode node;
    private final DataOutputStream dataOutputStream;

    public JsonRecordWriter(DataOutputStream out, ObjectMapper mapper) {
        this.mapper = mapper;
        this.node = mapper.createObjectNode();
        this.dataOutputStream = out;
    }

    @Override
    public void write(Text key, ObjectNode value) throws IOException, InterruptedException {
        node.set(key.toString(), value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        dataOutputStream.write(mapper.writeValueAsBytes(node));
        dataOutputStream.close();
    }
}
