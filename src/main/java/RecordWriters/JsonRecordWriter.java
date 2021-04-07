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

/**
 * RecordWriter implementation to enable writing JSON files
 */
public class JsonRecordWriter extends RecordWriter<Text, JsonNode> {
    private final ObjectMapper mapper;
    private final ObjectNode node;
    private final DataOutputStream dataOutputStream;

    /**
     * Default constructor
     * @param out DataOutputStream instance
     * @param mapper ObjectMapper instance
     */
    public JsonRecordWriter(DataOutputStream out, ObjectMapper mapper) {
        this.mapper = mapper;
        this.node = mapper.createObjectNode();
        this.dataOutputStream = out;
    }

    /**
     * Given two JsonNodes, merges one if another, if possible. Otherwise chooses the latest
     * @param current Current JsonNode value
     * @param fresh More recent JsonNode value
     * @return New JsonNode value
     */
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

    /**
     * Override of write method, that updates the json ObjectNode
     * @param key Key to update
     * @param value Value to update
     */
    @Override
    public void write(Text key, JsonNode value) {
        JsonNode currentValue = node.path(key.toString());
        JsonNode freshValue = currentValue.isMissingNode() ? value : merge(currentValue, value);

        node.set(key.toString(), freshValue);
    }

    /**
     * Writes the final JSON content to the output file
     * @param context TaskAttemptContext instance
     * @throws IOException Related with write operation
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException {
        dataOutputStream.write(mapper.writeValueAsBytes(node));
        dataOutputStream.close();
    }
}
