package Jobs;

import Common.Helper;
import Mappers.BasicsRatingsParquetMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.net.URI;

public class BasicsRatingsParquet {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[2]), true);

        Job job =  Job.getInstance(new Configuration(), "basicsRatingsParquetJob");
        job.setJarByClass(BasicsRatingsParquet.class);
        job.setMapperClass(BasicsRatingsParquetMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        AvroParquetOutputFormat.setSchema(job, Helper.getSchema("hdfs:/schemas/basicsRatings.parquet"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.addCacheFile(URI.create(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
