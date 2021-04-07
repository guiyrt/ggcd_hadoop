package WritableComparable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of WritableComparable to enable secondary sort in MovieSuggestion Job
 */
public class GenreRatingPair implements Writable, WritableComparable<GenreRatingPair> {
    private final Text genre;
    private final FloatWritable avgRating;

    /**
     * Empty constructor (used by Hadoop)
     */
    public GenreRatingPair() {
        genre = new Text();
        avgRating = new FloatWritable();
    }

    /**
     * Default constructor
     * @param genre Genre of a movie
     * @param avgRating Rating of a movie
     */
    public GenreRatingPair(String genre, Float avgRating) {
        this.genre = new Text(genre);
        this.avgRating = avgRating == null ? new FloatWritable(Float.NaN) : new FloatWritable(avgRating);
    }

    /**
     * Method that compares instance with another input GenreRatingPair
     * If both have the same genre, then sort by descending rating
     * @param grp Other GenreRatingPair instance
     * @return Comparison result
     */
    @Override
    public int compareTo(GenreRatingPair grp) {
        int compareGRP = genre.compareTo(grp.getGenre());

        if (compareGRP == 0) {
            // If any of the avgRating is Nan, use impossible rating value of -1
            // Multiply by -1 to get reverse order
            Float avgRating1 = Float.isNaN(avgRating.get()) ? -1 : avgRating.get();
            Float avgRating2 = Float.isNaN(grp.getAvgRating().get()) ? -1 : grp.getAvgRating().get();

            compareGRP = avgRating1.compareTo(avgRating2) * -1;
        }

        return compareGRP;
    }

    /**
     * Overwrite of write method to write class attributes
     * @param dataOutput DataOutput instance
     * @throws IOException Related with write operations
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        genre.write(dataOutput);
        avgRating.write(dataOutput);
    }

    /**
     * Overwrite of write method to read values to class attributes
     * @param dataInput DataInput instance
     * @throws IOException Related with read operations
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        genre.readFields(dataInput);
        avgRating.readFields(dataInput);
    }

    /**
     * Getter for genre
     * @return genre
     */
    public Text getGenre() {
        return genre;
    }

    /**
     * Getter for avgRating
     * @return avgRating
     */
    public FloatWritable getAvgRating() {
        return avgRating;
    }
}
