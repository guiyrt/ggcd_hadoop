package Reducers;

import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer definition for MovieSuggestion Job
 */
public class MovieSuggestionReducer extends Reducer<GenreRatingPair, MovieSuggestionData, Void, Text> {

    /**
     * Builds a entry to the output file, given parameters
     * @param msd MovieSuggestionData instance (some movie)
     * @param topMovie Best ranked movie
     * @param mainGenre Associated genre
     * @return String associating all the input values
     */
    private Text buildEntry(MovieSuggestionData msd, MovieSuggestionData topMovie, String mainGenre) {
        StringBuilder sb = new StringBuilder();

        // Write ttconst, primaryTitle and mainGenre
        sb.append(msd.getTtconst().toString()).append("\t");
        sb.append(msd.getPrimaryTitle().toString()).append("\t");
        sb.append(mainGenre).append("\t");

        // Top movie cannot be null, and if not null, must be rated
        if (topMovie != null && !Float.isNaN(topMovie.getAvgRating().get())) {
            sb.append(topMovie.getTtconst().toString()).append("\t");
            sb.append(topMovie.getPrimaryTitle().toString()).append("\t");
            sb.append(topMovie.getAvgRating().get()).append("\t");
        }

        // Keep same formatting as imdb provided databases
        else {
            sb.append("\\N\t\\N\t\\N\t");
        }

        return new Text(sb.toString());
    }

    /**
     * Reducer definition
     * @param key Contains genre (useful for primary sort) and rating (useful for secondary sort)
     * @param values Contains data associated with a given movie
     * @param context Reducer context instance
     * @throws IOException Associated with write context call
     * @throws InterruptedException Associated with write context call
     */
    @Override
    protected void reduce(GenreRatingPair key, Iterable<MovieSuggestionData> values, Context context) throws IOException, InterruptedException {
        Iterator<MovieSuggestionData> iterData = values.iterator();

        MovieSuggestionData topMovie = iterData.next().clone();
        MovieSuggestionData secondTopMovie = iterData.hasNext() ? iterData.next() : null;

        // Add top movie suggestion
        context.write(null, buildEntry(topMovie, secondTopMovie, key.getGenre().toString()));

        // Add following movies
        if (secondTopMovie != null) {
            context.write(null, buildEntry(secondTopMovie, topMovie, key.getGenre().toString()));

            while (iterData.hasNext()) {
                MovieSuggestionData msd = iterData.next();
                context.write(null, buildEntry(msd, topMovie, key.getGenre().toString()));
            }
        }
    }
}
