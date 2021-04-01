package WritableComparable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenreRatingPair implements Writable, WritableComparable<GenreRatingPair> {
    private final Text genre;
    private final FloatWritable rating;

    public  GenreRatingPair() {
        genre = new Text();
        rating = new FloatWritable();
    }

    public GenreRatingPair(String genre, float rating) {
        this.genre = new Text(genre);
        this.rating = new FloatWritable(rating);
    }

    @Override
    public int compareTo(GenreRatingPair grp) {
        int compareGRP = genre.compareTo(grp.getGenre());

        if (compareGRP == 0) {
            // Multiply by -1 to get reverse order
            compareGRP = rating.compareTo(grp.getRating()) * -1;
        }

        return compareGRP;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        genre.write(dataOutput);
        rating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        genre.readFields(dataInput);
        rating.readFields(dataInput);
    }

    public Text getGenre() {
        return genre;
    }

    public FloatWritable getRating() {
        return rating;
    }
}
