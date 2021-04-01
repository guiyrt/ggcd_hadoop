package Mappers;

import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class MovieSuggestionMapper extends Mapper<Void, GenericRecord, GenreRatingPair, MovieSuggestionData> {
    private static final String TYPE_MOVIE = "movie";

    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        if (value.get("titleType").equals(TYPE_MOVIE)) {
            String ttconst = (String) value.get("ttconst");
            String titleType = (String) value.get("titleType");
            String primaryTitle = (String) value.get("primaryTitle");
            Float avgRating = (Float) value.get("avgRating");

            // If movie has no associated genres, define main genre as "None"
            String mainGenre = value.get("genres") == null ? "None" : ((List<String>) value.get("genres")).get(0);

            context.write(new GenreRatingPair(mainGenre, avgRating == null ? 0 : avgRating), new MovieSuggestionData(ttconst, titleType, primaryTitle, avgRating));
        }
    }
}
