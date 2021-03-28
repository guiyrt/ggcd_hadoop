import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class YearMovieMapper extends Mapper<Void, GenericRecord, IntWritable, BytesWritable> {
    private static final String TYPE_MOVIE = "movie";
    private Schema outputSchema;

    @Override
    protected void setup(Context context) throws IOException {
        outputSchema = Helper.getSchema("src/main/schemas/yearMovieReducerInput.parquet");
    }

    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        String titleType = (String) value.get("titleType");

        // Should be "movie" type and have a stated release year
        if (titleType.equals(TYPE_MOVIE) && value.get("startYear") != null) {
            GenericRecord record = new GenericData.Record(outputSchema);

            record.put("ttconst", value.get("ttconst"));
            record.put("primaryTitle", value.get("primaryTitle"));
            record.put("avgRating", value.get("avgRating"));
            record.put("numVotes", value.get("numVotes"));

            context.write(new IntWritable((int) value.get("startYear")), new BytesWritable(Helper.serializeRecord(outputSchema, record)));
        }
    }
}
