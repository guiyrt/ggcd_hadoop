import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class MovieSuggestionMapper extends Mapper<Void, GenericRecord, GenreRatingPair, Text> {
    private static final String TYPE_MOVIE = "movie";
    private Schema outputSchema;

    @Override
    protected void setup(Context context) throws IOException {
        outputSchema = Helper.getSchema("src/main/schemas/movieSuggestionReducerInput.parquet");
    }

    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        if (value.get("titleType").equals(TYPE_MOVIE)) {
            GenericRecord record = new GenericData.Record(outputSchema);

            record.put("ttconst", value.get("ttconst"));
            record.put("titleType", value.get("titleType"));
            record.put("primaryTitle", value.get("primaryTitle"));
            record.put("avgRating", value.get("avgRating"));

            // If movie has no associated genres, define main genre as "None"
            String mainGenre = value.get("genres") == null ? "None" : ((List<String>) value.get("genres")).get(0);
            float rating = value.get("avgRating") == null ? 0 : (float) value.get("avgRating");

            context.write(new GenreRatingPair(mainGenre, rating), new Text(Helper.serializeRecord(outputSchema, record)));
        }
    }
}
