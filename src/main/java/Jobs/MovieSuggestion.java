package Jobs;

import Common.Helper;
import GroupringComparators.GenreRatingGroupingComparator;
import Mappers.MovieSuggestionMapper;
import Partitioners.GenreRatingPartitioner;
import Reducers.MovieSuggestionReducer;
import WritableComparable.GenreRatingPair;
import Writables.MovieSuggestionData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

import java.util.Map;

public class MovieSuggestion {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Map<String, String> options = Helper.getInputData(args);

        if (options.containsKey("overwrite") && Boolean.parseBoolean(options.get("overwrite"))) {
            Helper.deleteFolder(options.get("output"));
        }

        Job job = Job.getInstance(new Configuration(), "movieSuggestionOutput");
        job.setJarByClass(MovieSuggestion.class);
        job.setMapperClass(MovieSuggestionMapper.class);
        job.setReducerClass(MovieSuggestionReducer.class);

        job.setNumReduceTasks(Integer.parseInt(options.get("workers")));

        job.setMapOutputKeyClass(GenreRatingPair.class);
        job.setMapOutputValueClass(MovieSuggestionData.class);
        job.setInputFormatClass(AvroParquetInputFormat.class);

        job.setPartitionerClass(GenreRatingPartitioner.class);
        job.setGroupingComparatorClass(GenreRatingGroupingComparator.class);

        AvroParquetInputFormat.addInputPath(job, new Path(options.get("input")));
        AvroParquetInputFormat.setRequestedProjection(job, Helper.getSchema(Helper.glueDirWithFile(options.get("schemas"), "basicsRatingsProjectionForMovieSuggestion.parquet")));
        FileOutputFormat.setOutputPath(job, new Path(options.get("output")));

        job.waitForCompletion(true);
    }
}