import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;


// TODO: Get URIs from MAIN
// TODO: Read ratings compressed
// TODO: Shorter float than 32bit to avgRatings [PARQUET]
// TODO: investigate DECIMAL to avgRating [PARQUET]


public class InputToParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
    private Schema schema;
    HashMap<String, Rating> ratings = new HashMap<>();

    private void populateRatings() throws IOException {
        String ratingsData = Helper.readFile("src/main/resources/title.ratings.full.tsv");
        boolean headerGone = false;

        // Get rows
        String[] values = ratingsData.split("\n");

        for (String row: values) {
            String[] fields = row.split("\t");

            try {
                ratings.put(fields[0], new Rating(Double.parseDouble(fields[1]), Integer.parseInt(fields[2])));
            }
            catch (NumberFormatException e) {
                // First exception is caused by trying to parse header, so ignore that line
                if (!headerGone) {
                    headerGone = true;
                }
                // Following exceptions are not expected
                else {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException {
        schema = Helper.getSchema("src/main/schemas/basicsRatings.parquet");
        populateRatings();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore header
        if (key.get() != 0) {
            GenericRecord record = new GenericData.Record(schema);

            String[] values = value.toString().split("\t");

            record.put("ttconst", values[0]);
            record.put("titleType", values[1]);
            record.put("primaryTitle", values[2]);
            record.put("originalTitle", values[3]);
            record.put("isAdult", Integer.parseInt(values[4]) == 1);
            record.put("startYear", values[5].equals("\\N") ? null : Integer.parseInt(values[5]));
            record.put("endYear", values[6].equals("\\N") ? null : Integer.parseInt(values[6]));
            record.put("runtimeMinutes", values[7].equals("\\N") ? null : Integer.parseInt(values[7]));
            record.put("genres", values[8].equals("\\N") ? null : Arrays.asList(values[8].split(",")));
            record.put("avgRating", ratings.containsKey(values[0]) ? ratings.get(values[0]).getAvgRating() : null);
            record.put("numVotes", ratings.containsKey(values[0]) ? ratings.get(values[0]).getNumVotes() : null);

            context.write(null, record);
        }
    }
}
