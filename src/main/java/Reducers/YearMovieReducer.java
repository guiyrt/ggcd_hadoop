package Reducers;

import WritableComparable.YearRatingPair;
import Writables.YearMovieData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reducer definition for YearMovie Job
 */
public class YearMovieReducer extends Reducer<YearRatingPair, YearMovieData, Void, GenericRecord> {
    private Schema outputSchema;
    private Schema mostVotedMovieSchema;
    private Schema movieRatingInfo;

    /**
     * Overwrite setup method to loads schemas
     * @param context Reducer context
     * @throws IOException Related with read operations
     */
    @Override
    protected void setup(Context context) throws IOException {
        List<String> cachedURIs = Arrays.stream(context.getCacheFiles()).map(URI::toString).collect(Collectors.toList());

        outputSchema = Common.IO.readSchema(cachedURIs.get(0));
        mostVotedMovieSchema = Common.AvroSchemas.getSchemaFromUnion("mostVotedMovie",
                outputSchema.getField("mostVotedMovie").schema());
        movieRatingInfo =  Common.AvroSchemas.getSchemaFromUnion("movieRatingInfo",
                outputSchema.getField("top10RatedMovies").schema().getValueType());
    }

    /**
     * Reducer definition
     * @param key Contains start year (useful for primary sort) and rating (useful for secondary sort)
     * @param values Contains data associated with a given movie
     * @param context Reducer context instance
     * @throws IOException Associated with write context call
     * @throws InterruptedException Associated with write context call
     */
    @Override
    protected void reduce(YearRatingPair key, Iterable<YearMovieData> values, Context context) throws IOException, InterruptedException {
        // Definition for necessary data structures
        GenericRecord yearRecord = new GenericData.Record(outputSchema);
        Iterator<YearMovieData> iterator = values.iterator();
        Map<String, GenericRecord> top10movies = new HashMap<>();
        GenericRecord mostVotedMovie = new GenericData.Record(mostVotedMovieSchema);

        // Definitions of necessary local variables
        int totalMovies = 0;
        int ratedMovies = 0;
        int maxVotes = Integer.MIN_VALUE;

        // Iterate trough movies to find total number of movies and the one with more votes
        // Keep in mind that the movies are already in descending order by their ratings
        while(iterator.hasNext()) {
            YearMovieData movie = iterator.next();

            // Secondary sort by avgRating orders movies by descending avgRating values
            if (ratedMovies < 10 && !Float.isNaN(movie.getAvgRating().get())) {
                GenericRecord movieRatingInfo = new GenericData.Record(this.movieRatingInfo);

                movieRatingInfo.put("ttconst", movie.getTtconst().toString());
                movieRatingInfo.put("primaryTitle", movie.getPrimaryTitle().toString());
                movieRatingInfo.put("avgRating",  movie.getAvgRating().get());

                top10movies.put(Integer.toString(++ratedMovies), movieRatingInfo);
            }

            // Checks if movie has more votes than the current most voted movie
            if (movie.getNumVotes().get() > maxVotes) {
                maxVotes = movie.getNumVotes().get();

                mostVotedMovie.put("ttconst", movie.getTtconst());
                mostVotedMovie.put("primaryTitle", movie.getPrimaryTitle());
                mostVotedMovie.put("numVotes", movie.getNumVotes().get());
            }

            // Increments movie count
            totalMovies++;
        }

        // Fill the empty space if there are less than 10 ranked movies in a given year
        if (ratedMovies < 10) {
            for (int i = ratedMovies + 1; i <= 10; i++) {
                top10movies.put(Integer.toString(i), null);
            }
        }

        // Add top 10 ranked movies map
        yearRecord.put("top10RatedMovies", top10movies);

        // Add most voted movie, if any
        yearRecord.put("mostVotedMovie", maxVotes == Integer.MIN_VALUE ? null : mostVotedMovie);

        // Year and total movies released in that year
        yearRecord.put("startYear", key.getStartYear().get());
        yearRecord.put("totalMovies", totalMovies);

        // Add record
        context.write(null, yearRecord);
    }
}