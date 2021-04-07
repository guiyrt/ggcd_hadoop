package Mappers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mapper definition for BasicsRatings Job
 */
public class BasicsRatingsParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
    private Schema schema;
    HashMap<String, Rating> ratings = new HashMap<>();

    /**
     * Class definition to store data related with imdb ratings file
     * Used to associate ttconst with numVotes and avgRating
     */
    private static class Rating {
        private final double avgRating;
        private final int numVotes;

        public Rating(double avgRating, int numVotes) {
            this.avgRating = avgRating;
            this.numVotes = numVotes;
        }

        /**
         * Getter for avgRating
         * @return avgRating
         */
        public double getAvgRating() {
            return avgRating;
        }

        /**
         * Getter for numVotes
         * @return numVotes
         */
        public int getNumVotes() {
            return numVotes;
        }
    }

    /**
     * Given the imdb file regarding ratings, fills the "ratings" hashmap, associating ttconst with Rating instance
     * @param ratingsFile Input text file
     * @throws IOException Related with file reading operation
     */
    private void populateRatings(String ratingsFile) throws IOException {
        String ratingsData = Common.IO.readFile(ratingsFile);
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

    /**
     * Prepares the data to mapper execution
     * @param context Mapper context
     * @throws IOException Associated with read operations
     */
    @Override
    protected void setup(Context context) throws IOException {
        List<String> cachedURIs = Arrays.stream(context.getCacheFiles()).map(URI::toString).collect(Collectors.toList());

        populateRatings(cachedURIs.get(0));
        schema = Common.IO.readSchema(cachedURIs.get(1));
    }

    /**
     * Map method definition
     * @param key Line from input text file
     * @param value Content of line
     * @param context Mapper context
     * @throws IOException Associated with write context call
     * @throws InterruptedException Associated with write context call
     */
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