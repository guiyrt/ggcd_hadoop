package Partitioners;

import WritableComparable.YearRatingPair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearRatingPartitioner extends Partitioner<YearRatingPair, NullWritable> {
    @Override
    public int getPartition(YearRatingPair yearRatingPair, NullWritable nullWritable, int i) {
        return yearRatingPair.getYear().hashCode() % i;
    }
}