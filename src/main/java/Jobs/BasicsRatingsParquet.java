package Jobs;

import Mappers.BasicsRatingsParquetMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Definition of job that converts the imdb text files to parquet
 */
public class BasicsRatingsParquet {
    // These options must be declared in input, otherwise the job exists unsuccessfully
    private static final List<String> requiredOptions = Arrays.asList("input", "output", "ratings", "schemas");

    /**
     * Job declaration
     * @param args Input arguments
     * @throws IOException Associated with file reading
     * @throws ClassNotFoundException Associated with declaration of used classes in job execution
     * @throws InterruptedException Associated with waitForCompletion call
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(),
                "basicsRatingsParquetJob");
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

        job.setJarByClass(BasicsRatingsParquet.class);
        job.setMapperClass(BasicsRatingsParquetMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        AvroParquetOutputFormat.setSchema(job, Common.IO.readSchema(Common.Job.glueDirWithFile(options.get("schemas"),
                "basicsRatings.parquet")));
        FileInputFormat.addInputPath(job, new Path(options.get("input")));
        job.addCacheFile(URI.create(options.get("ratings")));
        job.addCacheFile(URI.create(Common.Job.glueDirWithFile(options.get("schemas"), "basicsRatings.parquet")));
        FileOutputFormat.setOutputPath(job, new Path(options.get("output")));

        job.waitForCompletion(true);
    }
}