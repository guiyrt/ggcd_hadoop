package Common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class that handles interactions with schemas
 */
public class AvroSchemas {

    /**
     * Finds a schema of a field in a union, if exists
     * @param fieldName Field to retrieve
     * @param union Union to search in
     * @return Schema of wanted union, or null if not there
     */
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

    /**
     * Given a schema and a object with content, converts the object to JSON equivalent structure
     * @param fieldValue Data that follows the given schema
     * @param fieldSchema Schema of input data object
     * @return Converted JsonNode
     * @throws ClassCastException If schema has type UNION, tries to cast object to all hypothesis until success
     * @throws NullPointerException If schema has type UNION, tries to cast object to all hypothesis until success
     */
    public static JsonNode fieldToNode(Object fieldValue, Schema fieldSchema) throws ClassCastException, NullPointerException {
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
            case ENUM:
                return JsonNodeFactory.instance.textNode((String) fieldValue);

            case BOOLEAN:
                return JsonNodeFactory.instance.booleanNode((boolean) fieldValue);

            case ARRAY:
                // Array type is converted to List, so we can suppress the unchecked cast
                @SuppressWarnings("unchecked")
                List<Object> array = (List<Object>) fieldValue;
                Schema itemSchema = fieldSchema.getElementType();
                ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
                array.forEach(item -> arrayNode.add(fieldToNode(item, itemSchema)));

                return arrayNode;

            case MAP:
                // Map type is converted to Map with String keys, so we can suppress the unchecked cast
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) fieldValue;
                Schema entrySchema = fieldSchema.getValueType();
                ObjectNode mapNode = JsonNodeFactory.instance.objectNode();
                map.forEach((id, value) -> mapNode.set(id, fieldToNode(value, entrySchema)));

                return mapNode;

            case UNION:
                List<Schema> union = fieldSchema.getTypes();
                JsonNode node = JsonNodeFactory.instance.objectNode();

                for(Schema schema: union) {

                    // Since schema type is union, the class of fieldValue has multiple possibilities
                    try {
                        node = fieldToNode(fieldValue, schema);
                    } catch (NullPointerException | ClassCastException ignored) {}
                }

                return node;

            case RECORD:
                GenericRecord record = (GenericRecord) fieldValue;
                ObjectNode recordNode = JsonNodeFactory.instance.objectNode();
                List<Schema.Field> fields = record.getSchema().getFields();
                fields.forEach(field -> recordNode.set(field.name(), fieldToNode(record.get(field.name()), field.schema())));

                return  recordNode;

            case FIXED:
            case BYTES:
                byte[] fixed = (byte[]) fieldValue;
                return JsonNodeFactory.instance.textNode(new String(fixed));

            default:
                return JsonNodeFactory.instance.nullNode();
        }
    }
}
