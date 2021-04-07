package Mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper related to ParquetToJson job
 * In this mapper, all the data is associated in the same JSON array, with key "entries"
 */
public class ParquetToJsonEntriesMapper extends Mapper<Void, GenericRecord, Text, JsonNode> {

    /**
     * Map method definition
     * @param key Always null, as input data is from parquet file
     * @param value GenericRecord instance
     * @param context Mapper context
     * @throws IOException Associated with write context call
     * @throws InterruptedException Associated with write context call
     */
    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        JsonNode convertedRecord = Common.AvroSchemas.fieldToNode(value, value.getSchema());
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        arrayNode.add(convertedRecord);

        context.write(new Text("entries"), arrayNode);
    }
}
