package Jobs;

import Common.Helper;
import GroupringComparators.YearRatingGroupingComparator;
import Mappers.YearMovieMapper;
import Partitioners.YearRatingPartitioner;
import Reducers.YearMovieReducer;
import WritableComparable.YearRatingPair;
import Writables.YearMovieData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static Common.Helper.missingOptionsString;

public class YearMovie {
    private static final List<String> requiredOptions = Arrays.asList("input", "output", "schemas", "workers");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job =  Job.getInstance(new Configuration(), "yearMovieJob");
        Map<String, String> options = Helper.getInputData(args);
        List<String> missingOptions = Helper.missingOptions(options, requiredOptions);

        // Must contain required options
        if (missingOptions.size() > 0) {
            System.err.println("\u001B[31mMISSING OPTIONS: \u001B[0m Job not submitted, the following required options are missing: \u001B[33m" + missingOptionsString(missingOptions) + "\u001B[0m");
            System.exit(1);
        }

        // If "overwrite" option is true, delete output folder before execution
        if (options.containsKey("overwrite") && Boolean.parseBoolean(options.get("overwrite"))) {
            Helper.deleteFolder(options.get("output"));
        }

        // Define number of reducers if specified
        if (options.containsKey("reducers") && Integer.parseInt(options.get("reducers")) > 0) {
            job.setNumReduceTasks(Integer.parseInt(options.get("reducers")));
        }

        job.setJarByClass(YearMovie.class);
        job.setMapperClass(YearMovieMapper.class);
        job.setReducerClass(YearMovieReducer.class);

        job.setNumReduceTasks(Integer.parseInt(options.get("workers")));
        job.addCacheFile(URI.create(Helper.glueDirWithFile(options.get("schemas"), "yearMovie.parquet")));

        job.setMapOutputKeyClass(YearRatingPair.class);
        job.setMapOutputValueClass(YearMovieData.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        job.setPartitionerClass(YearRatingPartitioner.class);
        job.setGroupingComparatorClass(YearRatingGroupingComparator.class);

        AvroParquetInputFormat.addInputPath(job, new Path(options.get("input")));
        AvroParquetInputFormat.setRequestedProjection(job, Helper.getSchema(Helper.glueDirWithFile(options.get("schemas"), "basicsRatingsProjectionForYearMovie.parquet")));
        AvroParquetOutputFormat.setSchema(job, Helper.getSchema(Helper.glueDirWithFile(options.get("schemas"), "yearMovie.parquet")));
        FileOutputFormat.setOutputPath(job, new Path(options.get("output")));

        job.waitForCompletion(true);
    }
}
