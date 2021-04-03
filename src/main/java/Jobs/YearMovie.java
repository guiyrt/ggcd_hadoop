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
import java.util.Map;

public class YearMovie {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Map<String, String> options = Helper.getInputData(args);

        if (options.containsKey("overwrite") && Boolean.parseBoolean(options.get("overwrite"))) {
            Helper.deleteFolder(options.get("output"));
        }

        Job job =  Job.getInstance(new Configuration(), "yearMovieJob");
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
