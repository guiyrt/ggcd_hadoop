package RecordWriters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

public class JsonRecordWriter extends RecordWriter<Text, JsonNode> {
    private final ObjectMapper mapper;
    private final ObjectNode node;
    private final DataOutputStream dataOutputStream;

    public JsonRecordWriter(DataOutputStream out, ObjectMapper mapper) {
        this.mapper = mapper;
        this.node = mapper.createObjectNode();
        this.dataOutputStream = out;
    }

    private JsonNode merge(JsonNode current, JsonNode fresh) {
        // If both are arrays, merge their content
        if (current.getNodeType() == JsonNodeType.ARRAY && fresh.getNodeType() == JsonNodeType.ARRAY) {
            ArrayNode currentArray = (ArrayNode) current;
            ArrayNode freshArray = (ArrayNode) fresh;
            currentArray.addAll(freshArray);

            return currentArray;
        }

        // If both are objects, merge their content
        else if (current.getNodeType() == JsonNodeType.OBJECT && fresh.getNodeType() == JsonNodeType.OBJECT) {
            ObjectNode currentObject = (ObjectNode) current;
            ObjectNode freshObject = (ObjectNode) fresh;
            currentObject.setAll(freshObject);

            return currentObject;
        }

        // Otherwise, return the fresh JsonNode
        else {
            return fresh;
        }
    }

    @Override
    public void write(Text key, JsonNode value) throws IOException, InterruptedException {
        JsonNode currentValue = node.path(key.toString());
        JsonNode freshValue = currentValue.isMissingNode() ? value : merge(currentValue, value);

        node.set(key.toString(), freshValue);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        dataOutputStream.write(mapper.writeValueAsBytes(node));
        dataOutputStream.close();
    }
}
