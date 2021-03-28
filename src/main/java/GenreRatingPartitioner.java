import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenreRatingPartitioner extends Partitioner<GenreRatingPair, NullWritable> {
    @Override
    public int getPartition(GenreRatingPair genreRatingPair, NullWritable nullWritable, int i) {
        return genreRatingPair.getGenre().hashCode() % i;
    }
}