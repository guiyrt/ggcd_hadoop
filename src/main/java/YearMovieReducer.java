import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class YearMovieReducer extends Reducer<IntWritable, BytesWritable, Void, GenericRecord> {
    private Schema inputSchema;
    private Schema outputSchema;
    private Schema mostVotedMovieSchema;
    private Schema movieRatingInfo;

    @Override
    protected void setup(Context context) throws IOException {
        inputSchema = Helper.getSchema("src/main/schemas/yearMovieReducerInput.parquet");
        outputSchema = Helper.getSchema("src/main/schemas/yearMovie.parquet");
        mostVotedMovieSchema = Helper.getSchemaFromUnion("mostVotedMovie", outputSchema.getField("mostVotedMovie").schema());
        movieRatingInfo =  Helper.getSchemaFromUnion("movieRatingInfo", outputSchema.getField("top10RatedMovies").schema().getValueType());
    }

    @Override
    protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        GenericRecord record = new GenericData.Record(outputSchema);

        // Pass iterable to list
        List<GenericRecord> valuesList = new ArrayList<>();
        for (BytesWritable data : values) {
            valuesList.add(Helper.deserializeRecord(inputSchema, data));
        }

        // Year and total movies released in that year
        record.put("startYear", key.get());
        record.put("totalMovies", valuesList.size());

        // Most votes
        GenericRecord mostVotes = valuesList.stream()
                .max(Comparator.comparingInt((GenericRecord gr) -> gr.get("numVotes") == null ? 0 : (int) gr.get("numVotes")))
                .orElse(null);

        // Verify if is valid
        if (mostVotes != null && mostVotes.get("numVotes") != null) {
            GenericRecord mostVotesMovie = new GenericData.Record(mostVotedMovieSchema);
            mostVotesMovie.put("ttconst", mostVotes.get("ttconst"));
            mostVotesMovie.put("primaryTitle", mostVotes.get("primaryTitle"));
            mostVotesMovie.put("numVotes", mostVotes.get("numVotes"));

            record.put("mostVotedMovie", mostVotesMovie);
        }

        else {
            record.put("mostVotedMovie", null);
        }


        // Top 10 ranked movies
        Map<String, GenericRecord> top10movies = new HashMap<>();
        valuesList.sort(Comparator.comparingDouble((GenericRecord gr) -> gr.get("avgRating") == null ? 0.0 : (float) gr.get("avgRating")).reversed());

        for (int i=0; i<10; i++) {
            if (i < valuesList.size() && valuesList.get(i).get("avgRating") != null) {
                GenericRecord movieRatingInfo = new GenericData.Record(this.movieRatingInfo);
                GenericRecord movieInRankI = valuesList.get(i);

                movieRatingInfo.put("ttconst", movieInRankI.get("ttconst"));
                movieRatingInfo.put("primaryTitle", movieInRankI.get("primaryTitle"));
                movieRatingInfo.put("avgRating", movieInRankI.get("avgRating"));

                top10movies.put(Integer.toString(i+1), movieRatingInfo);
            }

            else {
                top10movies.put(Integer.toString(i+1), null);
            }
        }

        // Add calculated list
        record.put("top10RatedMovies", top10movies);

        // Add record
        context.write(null, record);
    }
}