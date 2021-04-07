package Partitioners;

import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner associating GenreRatingPair with MovieSuggestionData
 */
public class GenreRatingPartitioner extends Partitioner<GenreRatingPair, MovieSuggestionData> {

    /**
     * This Custom Partitioner calls the KeyFieldBasedPartitioner Partitioner
     * @param genreRatingPair Key
     * @param movieSuggestionData Value
     * @param numPartitions number of partitions
     * @return Associated partition for input key and value
     */
    @Override
    public int getPartition(GenreRatingPair genreRatingPair, MovieSuggestionData movieSuggestionData, int numPartitions) {
       return new KeyFieldBasedPartitioner<Text, MovieSuggestionData>().getPartition(genreRatingPair.getGenre(), movieSuggestionData, numPartitions);
    }
}