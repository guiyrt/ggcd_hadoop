package Reducers;

import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MovieSuggestionReducer extends Reducer<GenreRatingPair, MovieSuggestionData, Void, Text> {
    private static final String headers = "ttconst\tprimaryTitle\tmainGenre\tsuggestedTtconst\tsuggestedPrimaryTitle\tsuggestedAvgRating";


    private Text buildEntry(MovieSuggestionData msd, MovieSuggestionData topMovie, String mainGenre) {
        StringBuilder sb = new StringBuilder();

        sb.append(msd.getTtconst().toString()).append("\t");
        sb.append(msd.getPrimaryTitle().toString()).append("\t");
        sb.append(mainGenre).append("\t");

        if (topMovie != null) {
            sb.append(topMovie.getTtconst().toString()).append("\t");
            sb.append(topMovie.getPrimaryTitle().toString()).append("\t");
            sb.append(Float.isNaN(topMovie.getAvgRating().get()) ? "\\N" : topMovie.getAvgRating().get()).append("\t");
        }

        // Keep same formatting as imdb provided databases
        else {
            sb.append("\\N\t\\N\t\\N\t");
        }

        return new Text(sb.toString());
    }

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
