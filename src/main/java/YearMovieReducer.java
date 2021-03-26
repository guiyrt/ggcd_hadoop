import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class YearMovieReducer extends Reducer<IntWritable, GenericRecord, Void, GenericRecord> {
    private Schema outputSchema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputSchema = Helper.getSchema("src/main/schemas/yearMovie.parquet");
    }

    @Override
    protected void reduce(IntWritable key, Iterable<GenericRecord> values, Context context) throws IOException, InterruptedException {
        List<GenericRecord> valuesList = new ArrayList<>();
        values.forEach(valuesList::add);

        GenericRecord record = new GenericData.Record(outputSchema);

    }
}
