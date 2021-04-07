package WritableComparable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Implementation of WritableComparable to enable secondary sort in YearMovie Job
 */
public class YearRatingPair implements Writable, WritableComparable<YearRatingPair> {
    private final IntWritable startYear;
    private final FloatWritable avgRating;

    /**
     * Empty constructor (used by Hadoop)
     */
    public YearRatingPair() {
        startYear = new IntWritable();
        avgRating = new FloatWritable();
    }

    /**
     * Default constructor
     * @param startYear Start year of a movie
     * @param avgRating Rating of a movie
     */
    public YearRatingPair(Integer startYear, Float avgRating) {
        this.startYear = new IntWritable(startYear);
        this.avgRating = avgRating == null ? new FloatWritable(Float.NaN) : new FloatWritable(avgRating);
    }

    /**
     * Method that compares instance with another input YearRatingPair
     * If both have the same year, then sort by descending rating
     * @param yrp Other YearRatingPair instance
     * @return Comparison result
     */
    @Override
    public int compareTo(YearRatingPair yrp) {
        int compareYRP = startYear.compareTo(yrp.getStartYear());

        if (compareYRP == 0) {
            // If any of the avgRating is Nan, use impossible rating value of -1
            // Multiply by -1 to get reverse order
            Float avgRating1 = Float.isNaN(avgRating.get()) ? -1 : avgRating.get();
            Float avgRating2 = Float.isNaN(yrp.getAvgRating().get()) ? -1 : yrp.getAvgRating().get();

            compareYRP = avgRating1.compareTo(avgRating2) * -1;
        }

        return compareYRP;
    }

    /**
     * Override of write method to write class attributes
     * @param dataOutput DataOutput instance
     * @throws IOException Related with write operations
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        startYear.write(dataOutput);
        avgRating.write(dataOutput);
    }

    /**
     * Override of write method to read values to class attributes
     * @param dataInput DataInput instance
     * @throws IOException Related with read operations
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        startYear.readFields(dataInput);
        avgRating.readFields(dataInput);
    }

    /**
     * Getter for startYear
     * @return startYear
     */
    public IntWritable getStartYear() {
        return startYear;
    }

    /**
     * Getter for avgRating
     * @return avgRating
     */
    public FloatWritable getAvgRating() {
        return avgRating;
    }
}
