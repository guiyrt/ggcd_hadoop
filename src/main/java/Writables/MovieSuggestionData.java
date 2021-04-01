package Writables;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieSuggestionData implements Writable {
    private final Text ttconst;
    private final Text titleType;
    private final Text primaryTitle;
    private final FloatWritable avgRating;


    public MovieSuggestionData() {
        ttconst = new Text();
        titleType = new Text();
        primaryTitle = new Text();
        avgRating = new FloatWritable();
    }

    public MovieSuggestionData(String ttconst, String titleType, String primaryTitle, Float avgRating) {
        this.ttconst = new Text(ttconst);
        this.titleType = new Text(titleType);
        this.primaryTitle = new Text(primaryTitle);
        this.avgRating = avgRating == null ? new FloatWritable(Float.NaN) : new FloatWritable(avgRating);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ttconst.write(dataOutput);
        titleType.write(dataOutput);
        primaryTitle.write(dataOutput);
        avgRating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ttconst.readFields(dataInput);
        titleType.readFields(dataInput);
        primaryTitle.readFields(dataInput);
        avgRating.readFields(dataInput);
    }

    public MovieSuggestionData clone() {
        return new MovieSuggestionData(getTtconst().toString(), getTitleType().toString(), getPrimaryTitle().toString(), getAvgRating().get());
    }

    public Text getTtconst() {
        return ttconst;
    }

    public Text getTitleType() {
        return titleType;
    }

    public Text getPrimaryTitle() {
        return primaryTitle;
    }

    public FloatWritable getAvgRating() {
        return avgRating;
    }
}
