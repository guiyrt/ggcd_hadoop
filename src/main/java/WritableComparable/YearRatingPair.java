package WritableComparable;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearRatingPair implements Writable, WritableComparable<YearRatingPair> {
    private final IntWritable year;
    private final FloatWritable avgRating;

    public YearRatingPair() {
        year = new IntWritable();
        avgRating = new FloatWritable();
    }

    public YearRatingPair(Integer year, Float avgRating) {
        this.year = new IntWritable(year);
        this.avgRating = avgRating == null ? new FloatWritable(Float.NaN) : new FloatWritable(avgRating);
    }

    @Override
    public int compareTo(YearRatingPair yrp) {
        int compareYRP = year.compareTo(yrp.getYear());

        if (compareYRP == 0) {
            // Multiply by -1 to get reverse order
            Float avgRating1 = Float.isNaN(avgRating.get()) ? 0 : avgRating.get();
            Float avgRating2 = Float.isNaN(yrp.getAvgRating().get()) ? 0 : yrp.getAvgRating().get();

            compareYRP = avgRating1.compareTo(avgRating2) * -1;
        }

        return compareYRP;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        year.write(dataOutput);
        avgRating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year.readFields(dataInput);
        avgRating.readFields(dataInput);
    }

    public IntWritable getYear() {
        return year;
    }

    public FloatWritable getAvgRating() {
        return avgRating;
    }
}
