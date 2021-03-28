import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GenreRatingGroupingComparator extends WritableComparator {
    public GenreRatingGroupingComparator() {
        super(GenreRatingPair.class, true);
    }

    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
        GenreRatingPair temperaturePair = (GenreRatingPair) tp1;
        GenreRatingPair temperaturePair2 = (GenreRatingPair) tp2;
        return temperaturePair.getGenre().compareTo(temperaturePair2.getGenre());
    }
}
