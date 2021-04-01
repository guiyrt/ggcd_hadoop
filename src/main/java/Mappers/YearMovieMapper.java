package Mappers;

import WritableComparable.YearRatingPair;
import Writables.YearMovieData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class YearMovieMapper extends Mapper<Void, GenericRecord, YearRatingPair, YearMovieData> {
    private static final String TYPE_MOVIE = "movie";

    @Override
    protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        String titleType = (String) value.get("titleType");

        // Should be "movie" type and have a stated release year
        if (titleType.equals(TYPE_MOVIE) && value.get("startYear") != null) {
            String ttconst = (String) value.get("ttconst");
            String primaryTitle = (String) value.get("primaryTitle");
            Float avgRating = (Float) value.get("avgRating");
            Integer numVotes = (Integer) value.get("numVotes");
            Integer year = (Integer) value.get("startYear");

            context.write(new YearRatingPair(year, avgRating), new YearMovieData(ttconst, primaryTitle, avgRating, numVotes));
        }
    }
}
