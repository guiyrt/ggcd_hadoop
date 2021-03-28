import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MovieSuggestionReducer extends Reducer<GenreRatingPair, Text, Void, Text> {
    private Schema inputSchema;
    private static final String headers = "ttconst\tprimaryTitle\tmainGenre\tsuggestedTtconst\tsuggestedPrimaryTitle\tsuggestedAvgRating";


    @Override
    protected void setup(Context context) throws IOException {
        inputSchema = Helper.getSchema("src/main/schemas/movieSuggestionReducerInput.parquet");
    }

    private Text buildEntry(GenericRecord gr, GenericRecord bestRanked, String mainGenre) {
        StringBuilder sb = new StringBuilder();

        sb.append(gr.get("ttconst")).append("\t");
        sb.append(gr.get("primaryTitle")).append("\t");
        sb.append(mainGenre).append("\t");

        if (bestRanked != null) {
            sb.append(bestRanked.get("ttconst")).append("\t");
            sb.append(bestRanked.get("primaryTitle")).append("\t");
            sb.append(bestRanked.get("avgRating") == null ? "\\N" : bestRanked.get("avgRating")).append("\t");
        }

        // Keep same formatting as imdb provided databases
        else {
            sb.append("\\N\t\\N\t\\N\t");
        }

        return new Text(sb.toString());
    }

    @Override
    protected void reduce(GenreRatingPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Pass iterable to list
        List<GenericRecord> valuesList = new ArrayList<>();
        for (Text data : values) {
            valuesList.add(Helper.deserializeRecord(inputSchema, data));
        }

        if (valuesList.size() == 1) {
            context.write(null, buildEntry(valuesList.get(0), null, key.getGenre().toString()));
        }

        else {
            // Secondary sorting leaves the highest avgRating movie in last position
            GenericRecord suggestedMovie = valuesList.get(valuesList.size()-1);

            for (GenericRecord gr: valuesList.subList(0, valuesList.size()-1)) {
                context.write(null, buildEntry(gr, suggestedMovie, key.getGenre().toString()));
            }

            // Movie with highest avgRating should suggest the second best rated movie
            context.write(null, buildEntry(suggestedMovie, valuesList.get(valuesList.size()-2), key.getGenre().toString()));
        }
    }
}
