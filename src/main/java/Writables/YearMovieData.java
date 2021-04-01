package Writables;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearMovieData implements Writable {
    private final Text ttconst;
    private final Text primaryTitle;
    private final FloatWritable avgRating;
    private final IntWritable numVotes;

    public YearMovieData() {
        ttconst = new Text();
        primaryTitle = new Text();
        avgRating = new FloatWritable();
        numVotes = new IntWritable();
    }

    public YearMovieData(String ttconst, String primaryTitle, Float avgRating, Integer numVotes) {
        this.ttconst = new Text(ttconst);
        this.primaryTitle = new Text(primaryTitle);
        this.avgRating = avgRating == null ? new FloatWritable(Float.NaN) :new FloatWritable(avgRating);
        this.numVotes = numVotes == null ? new IntWritable(Integer.MIN_VALUE) : new IntWritable(numVotes);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ttconst.write(dataOutput);
        primaryTitle.write(dataOutput);
        avgRating.write(dataOutput);
        numVotes.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ttconst.readFields(dataInput);
        primaryTitle.readFields(dataInput);
        avgRating.readFields(dataInput);
        numVotes.readFields(dataInput);
    }

    public Text getTtconst() {
        return ttconst;
    }

    public Text getPrimaryTitle() {
        return primaryTitle;
    }

    public FloatWritable getAvgRating() {
        return avgRating;
    }

    public IntWritable getNumVotes() {
        return numVotes;
    }
}
