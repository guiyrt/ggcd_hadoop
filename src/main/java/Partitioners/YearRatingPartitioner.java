package Partitioners;

import WritableComparable.YearRatingPair;
import Writables.YearMovieData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.RehashPartitioner;

public class YearRatingPartitioner extends Partitioner<YearRatingPair, YearMovieData> {
    @Override
    public int getPartition(YearRatingPair yearRatingPair, YearMovieData yearMovieData, int i) {
        return new RehashPartitioner<IntWritable, YearMovieData>().getPartition(yearRatingPair.getYear(), yearMovieData, i);
    }
}