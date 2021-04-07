package Writables;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Definition of class that contains necessary information to describe a movie for the YearMovie Job
 */
public class YearMovieData implements Writable {
    private final Text ttconst;
    private final Text primaryTitle;
    private final FloatWritable avgRating;
    private final IntWritable numVotes;

    /**
     * Empty constructor (used by Hadoop)
     */
    public YearMovieData() {
        ttconst = new Text();
        primaryTitle = new Text();
        avgRating = new FloatWritable();
        numVotes = new IntWritable();
    }

    /**
     * Default constructor
     * @param ttconst ttconst of a movie
     * @param primaryTitle primaryTitle of a movie
     * @param avgRating avgRating of a movie
     * @param numVotes numVotes of a movie
     */
    public YearMovieData(String ttconst, String primaryTitle, Float avgRating, Integer numVotes) {
        this.ttconst = new Text(ttconst);
        this.primaryTitle = new Text(primaryTitle);
        this.avgRating = avgRating == null ? new FloatWritable(Float.NaN) :new FloatWritable(avgRating);
        this.numVotes = numVotes == null ? new IntWritable(Integer.MIN_VALUE) : new IntWritable(numVotes);
    }

    /**
     * Overwrite of write method to write class attributes
     * @param dataOutput DataOutput instance
     * @throws IOException Related with write operations
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ttconst.write(dataOutput);
        primaryTitle.write(dataOutput);
        avgRating.write(dataOutput);
        numVotes.write(dataOutput);
    }

    /**
     * Overwrite of write method to read values to class attributes
     * @param dataInput DataInput instance
     * @throws IOException Related with read operations
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ttconst.readFields(dataInput);
        primaryTitle.readFields(dataInput);
        avgRating.readFields(dataInput);
        numVotes.readFields(dataInput);
    }

    /**
     * Getter for ttconst
     * @return ttconst
     */
    public Text getTtconst() {
        return ttconst;
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

    /**
     * Getter for numVotes
     * @return numVotes
     */
    public IntWritable getNumVotes() {
        return numVotes;
    }
}
