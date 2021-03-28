import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

public class YearMovieJobTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("yearMovieJobOutput"), true);

        Job job =  Job.getInstance(new Configuration(), "yearMovieJob");
        job.setJarByClass(YearMovieJobTest.class);
        job.setMapperClass(YearMovieMapper.class);
        job.setReducerClass(YearMovieReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        AvroParquetInputFormat.addInputPath(job, new Path("basicsRatingsParquetJobOutput"));
        AvroParquetInputFormat.setRequestedProjection(job, Helper.getSchema("src/main/schemas/basicsRatingsProjectionForYearMovie.parquet"));
        AvroParquetOutputFormat.setSchema(job, Helper.getSchema("src/main/schemas/yearMovie.parquet"));
        FileOutputFormat.setOutputPath(job, new Path("yearMovieJobOutput"));

        job.waitForCompletion(true);
    }
}
