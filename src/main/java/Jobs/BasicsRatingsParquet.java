package Jobs;

import Common.Helper;
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
import java.util.Map;

import static Common.Helper.glueDirWithFile;

public class BasicsRatingsParquet {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Map<String, String> options = Helper.getInputData(args);
        Helper.deleteFolder(options.get("output"));

        Job job =  Job.getInstance(new Configuration(), "basicsRatingsParquetJob");
        job.setJarByClass(BasicsRatingsParquet.class);
        job.setMapperClass(BasicsRatingsParquetMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        AvroParquetOutputFormat.setSchema(job, Helper.getSchema(Helper.glueDirWithFile(options.get("schemas"), "basicsRatings.parquet")));
        FileInputFormat.addInputPath(job, new Path(options.get("input")));
        job.addCacheFile(URI.create(options.get("ratings")));
        job.addCacheFile(URI.create(glueDirWithFile(options.get("schemas"), "basicsRatings.parquet")));
        FileOutputFormat.setOutputPath(job, new Path(options.get("output")));

        job.waitForCompletion(true);
    }
}