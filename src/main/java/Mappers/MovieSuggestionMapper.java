package Mappers;

import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Mapper definition for MovieSuggestion Job
 */
public class MovieSuggestionMapper extends Mapper<Void, GenericRecord, GenreRatingPair, MovieSuggestionData> {
    private static final String TYPE_MOVIE = "movie";

    /**
     * Map method definition
     * @param key Always null, as input data is from parquet file
     * @param value GenericRecord instance
     * @param context Mapper context
     * @throws IOException Associated with write context call
     * @throws InterruptedException Associated with write context call
     */
    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        if (value.get("titleType").equals(TYPE_MOVIE)) {
            String ttconst = (String) value.get("ttconst");
            String titleType = (String) value.get("titleType");
            String primaryTitle = (String) value.get("primaryTitle");
            Float avgRating = (Float) value.get("avgRating");

            // If movie has no associated genres, define main genre as "None"
            // From knowing the input schema, we are certain that "genre" is a Avro List, go we can suppress the unchecked cast warning
            @SuppressWarnings("unchecked")
            String mainGenre = value.get("genres") == null ? "None" : ((List<String>) value.get("genres")).get(0);

            context.write(new GenreRatingPair(mainGenre, avgRating), new MovieSuggestionData(ttconst, titleType, primaryTitle, avgRating));
        }
    }
}
