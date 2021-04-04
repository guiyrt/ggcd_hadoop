package Mappers;

import Common.Helper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.Schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class ParquetToJsonIDsMapper extends Mapper<Void, GenericRecord, Text, JsonNode> {

    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        ObjectNode nodeValues = JsonNodeFactory.instance.objectNode();

        Schema master = value.getSchema();
        List<Field> fields = master.getFields();

        // Sublist starting in 1 because 0 is used as identifier
        fields.subList(1, fields.size()).forEach(field -> nodeValues.set(field.name(), Helper.fieldToNode(value.get(field.name()), field.schema())));

        context.write(new Text(Helper.fieldToNode(value.get(0), fields.get(0).schema()).asText()), nodeValues);
    }
}
