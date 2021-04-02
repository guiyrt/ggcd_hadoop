package Partitioners;

import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.hadoop.mapred.lib.BinaryPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenreRatingPartitioner extends Partitioner<GenreRatingPair, MovieSuggestionData> {
    @Override
    public int getPartition(GenreRatingPair genreRatingPair, MovieSuggestionData movieSuggestionData, int i) {
       return new BinaryPartitioner<MovieSuggestionData>().getPartition(genreRatingPair.getGenre(), movieSuggestionData, i);
    }
}