package Jobs;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Definition of job that for each movie, suggests the best of the same genre
 */
public class MovieSuggestion {
    // These options must be declared in input, otherwise the job exists unsuccessfully
    private static final List<String> requiredOptions = Arrays.asList("input", "output", "schemas");

    /**
     * Job declaration
     * @param args Input arguments
     * @throws IOException Associated with file reading
     * @throws ClassNotFoundException Associated with declaration of used classes in job execution
     * @throws InterruptedException Associated with waitForCompletion call
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "movieSuggestionOutput");
        Map<String, String> options = Common.Job.getInputOptions(args);
        List<String> missingOptions = Common.Job.missingOptions(options, requiredOptions);

        // Must contain required options
        if (missingOptions.size() > 0) {
            System.err.println("\u001B[31mMISSING OPTIONS:\u001B[0m Job not submitted, the following required options are missing: \u001B[33m"
                    + Common.Job.missingOptionsString(missingOptions) + "\u001B[0m");
            System.exit(1);
        }

        // If "overwrite" option is true, delete output folder before execution
        if (options.containsKey("overwrite") && Boolean.parseBoolean(options.get("overwrite"))) {
            Common.IO.deleteFolder(options.get("output"));
        }

        // Define number of reducers if specified
        if (options.containsKey("reducers") && Integer.parseInt(options.get("reducers")) > 0) {
            job.setNumReduceTasks(Integer.parseInt(options.get("reducers")));
        }

        job.setJarByClass(MovieSuggestion.class);
        job.setMapperClass(MovieSuggestionMapper.class);
        job.setReducerClass(MovieSuggestionReducer.class);

        job.setMapOutputKeyClass(GenreRatingPair.class);
        job.setMapOutputValueClass(MovieSuggestionData.class);
        job.setInputFormatClass(AvroParquetInputFormat.class);

        job.setPartitionerClass(GenreRatingPartitioner.class);
        job.setGroupingComparatorClass(GenreRatingGroupingComparator.class);

        AvroParquetInputFormat.addInputPath(job, new Path(options.get("input")));
        AvroParquetInputFormat.setRequestedProjection(job, Common.IO.readSchema(Common.Job.glueDirWithFile(options.get("schemas"),
                "basicsRatingsProjectionForMovieSuggestion.parquet")));
        FileOutputFormat.setOutputPath(job, new Path(options.get("output")));

        job.waitForCompletion(true);
    }
}