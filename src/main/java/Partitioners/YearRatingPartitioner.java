package Partitioners;

import WritableComparable.YearRatingPair;
import Writables.YearMovieData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.RehashPartitioner;

/**
 * Partitioner associating YearRatingPair with YearMovieData
 */
public class YearRatingPartitioner extends Partitioner<YearRatingPair, YearMovieData> {

    /**
     * Following documentation indications, this Custom Partitioner calls the Rehash Partitioner, which is suggested for Integer keys
     * with simple patterns in their distribution
     * @param yearRatingPair Key
     * @param yearMovieData Value
     * @param numPartitions number of partitions
     * @return Associated partition for input key and value
     */
    @Override
    public int getPartition(YearRatingPair yearRatingPair, YearMovieData yearMovieData, int numPartitions) {
        return new RehashPartitioner<IntWritable, YearMovieData>().getPartition(yearRatingPair.getStartYear(), yearMovieData, numPartitions);
    }
}