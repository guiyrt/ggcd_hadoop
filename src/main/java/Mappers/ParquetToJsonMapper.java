package Mappers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.Schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ParquetToJsonMapper extends Mapper<Void, GenericRecord, Text, ObjectNode> {

    private JsonNode fieldToNode(Object fieldValue, Schema fieldSchema) throws ClassCastException, NullPointerException {

        switch (fieldSchema.getType()) {
            case NULL:
                if (!Objects.isNull(fieldValue)) throw new ClassCastException();
                return JsonNodeFactory.instance.nullNode();

            case INT:
                return JsonNodeFactory.instance.numberNode((int) fieldValue);

            case LONG:
                return JsonNodeFactory.instance.numberNode((long) fieldValue);

            case FLOAT:
                return JsonNodeFactory.instance.numberNode((float) fieldValue);

            case DOUBLE:
                return JsonNodeFactory.instance.numberNode((double) fieldValue);

            case STRING:
                return JsonNodeFactory.instance.textNode((String) fieldValue);

            case BOOLEAN:
                return JsonNodeFactory.instance.booleanNode((boolean) fieldValue);

            case ARRAY:
                List<Object> array = (List<Object>) fieldValue;
                Schema itemSchema = fieldSchema.getElementType();
                ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
                array.forEach(item -> arrayNode.add(fieldToNode(item, itemSchema)));

                return arrayNode;

            case MAP:
                Map<String, Object> map = (Map<String, Object>) fieldValue;
                Schema entrySchema = fieldSchema.getValueType();
                ObjectNode mapNode = JsonNodeFactory.instance.objectNode();
                map.forEach((id, value) -> mapNode.set(id, fieldToNode(value, entrySchema)));

                return mapNode;

            case UNION:
                List<Schema> union = fieldSchema.getTypes();
                JsonNode node = JsonNodeFactory.instance.objectNode();

                for(Schema schema: union) {

                    // Since schema type is union, object class has multiple possibilities
                    try {
                        node = fieldToNode(fieldValue, schema);
                    } catch (NullPointerException | ClassCastException ignored) {}
                }

                return node;

            case RECORD:
                GenericRecord record = (GenericRecord) fieldValue;
                ObjectNode recordNode = JsonNodeFactory.instance.objectNode();
                List<Field> fields = record.getSchema().getFields();
                fields.forEach(field -> recordNode.set(field.name(), fieldToNode(record.get(field.name()), field.schema())));

                return  recordNode;
        }

        return JsonNodeFactory.instance.nullNode();
    }


    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode nodeValues = mapper.createObjectNode();

        Schema master = value.getSchema();
        List<Field> fields = master.getFields();

        // Sublist starting in 1 because 0 is used as identifier
        fields.subList(1, fields.size()).forEach(field -> nodeValues.set(field.name(), fieldToNode(value.get(field.name()), field.schema())));

        context.write(new Text(fieldToNode(value.get(0), fields.get(0).schema()).asText()), nodeValues);
    }
}
