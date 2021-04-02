package Jobs;

import Common.Helper;
import GroupringComparators.GenreRatingGroupingComparator;
import GroupringComparators.YearRatingGroupingComparator;
import Mappers.YearMovieMapper;
import Partitioners.GenreRatingPartitioner;
import Partitioners.YearRatingPartitioner;
import Reducers.YearMovieReducer;
import WritableComparable.YearRatingPair;
import Writables.YearMovieData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

public class YearMovie {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[1]), true);

        Job job =  Job.getInstance(new Configuration(), "yearMovieJob");
        job.setJarByClass(YearMovie.class);
        job.setMapperClass(YearMovieMapper.class);
        job.setReducerClass(YearMovieReducer.class);

        job.setMapOutputKeyClass(YearRatingPair.class);
        job.setMapOutputValueClass(YearMovieData.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        job.setPartitionerClass(YearRatingPartitioner.class);
        job.setGroupingComparatorClass(YearRatingGroupingComparator.class);

        AvroParquetInputFormat.addInputPath(job, new Path(args[0]));
        AvroParquetInputFormat.setRequestedProjection(job, Helper.getSchema("hdfs:////schemas/basicsRatingsProjectionForYearMovie.parquet"));
        AvroParquetOutputFormat.setSchema(job, Helper.getSchema("hdfs:////schemas/yearMovie.parquet"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
