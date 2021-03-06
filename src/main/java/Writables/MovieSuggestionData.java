package Writables;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Definition of class that contains necessary information to describe a movie for the MovieSuggestion Job
 */
public class MovieSuggestionData implements Writable {
    private final Text ttconst;
    private final Text titleType;
    private final Text primaryTitle;
    private final FloatWritable avgRating;

    /**
     * Empty constructor (used by Hadoop)
     */
    public MovieSuggestionData() {
        ttconst = new Text();
        titleType = new Text();
        primaryTitle = new Text();
        avgRating = new FloatWritable();
    }

    /**
     * Default constructor
     * @param ttconst ttconst of a movie
     * @param titleType titleType of a movie
     * @param primaryTitle primaryTitle of a movie
     * @param avgRating avgRating of a movie
     */
    public MovieSuggestionData(String ttconst, String titleType, String primaryTitle, Float avgRating) {
        this.ttconst = new Text(ttconst);
        this.titleType = new Text(titleType);
        this.primaryTitle = new Text(primaryTitle);
        this.avgRating = avgRating == null ? new FloatWritable(Float.NaN) : new FloatWritable(avgRating);
    }

    /**
     * Override of write method to write class attributes
     * @param dataOutput DataOutput instance
     * @throws IOException Related with write operations
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ttconst.write(dataOutput);
        titleType.write(dataOutput);
        primaryTitle.write(dataOutput);
        avgRating.write(dataOutput);
    }

    /**
     * Override of write method to read values to class attributes
     * @param dataInput DataInput instance
     * @throws IOException Related with read operations
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ttconst.readFields(dataInput);
        titleType.readFields(dataInput);
        primaryTitle.readFields(dataInput);
        avgRating.readFields(dataInput);
    }

    /**
     * Cloner for MovieSuggestionDate
     * @return Clone of instance
     */
    @Override
    public MovieSuggestionData clone() {
        return new MovieSuggestionData(getTtconst().toString(), getTitleType().toString(), getPrimaryTitle().toString(), getAvgRating().get());
    }

    /**
     * Getter for ttconst
     * @return ttconst
     */
    public Text getTtconst() {
        return ttconst;
    }

    /**
     * Getter for titleType
     * @return titleType
     */
    public Text getTitleType() {
        return titleType;
    }

    /**
     * Getter for primaryTitle
     * @return primaryTitle
     */
    public Text getPrimaryTitle() {
        return primaryTitle;
    }

    /**
     * Getter for avgRating
     * @return avgRating
     */
    public FloatWritable getAvgRating() {
        return avgRating;
    }
}
