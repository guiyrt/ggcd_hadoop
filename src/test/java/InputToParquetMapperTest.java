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

public class InputToParquetMapperTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("inputToParquetJobOutput"), true);

        Job job =  Job.getInstance(new Configuration(), "inputToParquetJob");
        job.setJarByClass(InputToParquetMapper.class);
        job.setMapperClass(InputToParquetMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        AvroParquetOutputFormat.setSchema(job, Helper.getSchema("src/main/schemas/basicsRatings.parquet"));
        FileInputFormat.addInputPath(job, new Path("src/main/resources/title.basics.mini.tsv.bz2"));
        FileOutputFormat.setOutputPath(job, new Path("inputToParquetJobOutput"));

        job.waitForCompletion(true);
    }
}