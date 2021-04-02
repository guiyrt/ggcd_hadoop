package Jobs;

import Common.Helper;
import GroupringComparators.GenreRatingGroupingComparator;
import Mappers.MovieSuggestionMapper;
import Partitioners.GenreRatingPartitioner;
import Reducers.MovieSuggestionReducer;
import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

public class MovieSuggestions {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("movieSuggestionJobOutput"), true);

        Job job = Job.getInstance(new Configuration(), "movieSuggestionOutput");
        job.setJarByClass(MovieSuggestions.class);
        job.setMapperClass(MovieSuggestionMapper.class);
        job.setReducerClass(MovieSuggestionReducer.class);

        job.setMapOutputKeyClass(GenreRatingPair.class);
        job.setMapOutputValueClass(MovieSuggestionData.class);
        job.setInputFormatClass(AvroParquetInputFormat.class);

        job.setPartitionerClass(GenreRatingPartitioner.class);
        job.setGroupingComparatorClass(GenreRatingGroupingComparator.class);

        AvroParquetInputFormat.addInputPath(job, new Path(args[0]));
        AvroParquetInputFormat.setRequestedProjection(job, Helper.getSchema("hdfs:////schemas/basicsRatingsProjectionForMovieSuggestion.parquet"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}