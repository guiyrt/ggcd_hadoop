package Mappers;

import Common.Job;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ParquetToJsonEntriesMapper extends Mapper<Void, GenericRecord, Text, JsonNode> {

    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        JsonNode convertedRecord = Common.AvroSchemas.fieldToNode(value, value.getSchema());
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        arrayNode.add(convertedRecord);

        context.write(new Text("entries"), arrayNode);
    }
}
