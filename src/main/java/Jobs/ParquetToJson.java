package Jobs;

import Common.Helper;
import FileOutputFormatters.JsonOutputFormat;
import Mappers.ParquetToJsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import static Common.Helper.missingOptionsString;

public class ParquetToJson {
    private static final List<String> requiredOptions = Arrays.asList("input", "output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
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

        Job job = Job.getInstance(new Configuration(), "basicsRatingsJsonJob");
        job.setJarByClass(ParquetToJson.class);
        job.setMapperClass(ParquetToJsonMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ObjectNode.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(JsonOutputFormat.class);

        AvroParquetInputFormat.addInputPath(job, new Path(options.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(options.get("output")));

        job.waitForCompletion(true);
    }

}
