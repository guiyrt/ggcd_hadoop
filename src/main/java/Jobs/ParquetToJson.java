package Jobs;

import FileOutputFormatters.JsonOutputFormat;
import Mappers.ParquetToJsonEntriesMapper;
import Mappers.ParquetToJsonIDsMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Definition of job that converts parquet files to JSON equivalent
 */
public class ParquetToJson {
    // These options must be declared in input, otherwise the job exists unsuccessfully
    private static final List<String> requiredOptions = Arrays.asList("input", "output");

    /**
     * Job declaration
     * @param args Input arguments
     * @throws IOException Associated with file reading
     * @throws ClassNotFoundException Associated with declaration of used classes in job execution
     * @throws InterruptedException Associated with waitForCompletion call
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "basicsRatingsJsonJob");
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

        // If specified, use mapper that uses the first attribute as ID
        if (options.containsKey("firstAsId") && Boolean.parseBoolean(options.get("firstAsId"))) {
            job.setMapperClass(ParquetToJsonIDsMapper.class);
        }

        else {
            job.setMapperClass(ParquetToJsonEntriesMapper.class);
        }

        job.setJarByClass(ParquetToJson.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(JsonNode.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(JsonOutputFormat.class);

        AvroParquetInputFormat.addInputPath(job, new Path(options.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(options.get("output")));

        job.waitForCompletion(true);
    }

}
