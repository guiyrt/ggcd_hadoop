package GroupringComparators;

import WritableComparable.GenreRatingPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  Grouping Comparator implementation, so that comparison only compares by natural key "genre"
 */
public class GenreRatingGroupingComparator extends WritableComparator {
    public GenreRatingGroupingComparator() {
        super(GenreRatingPair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        GenreRatingPair grp1 = (GenreRatingPair) wc1;
        GenreRatingPair grp2 = (GenreRatingPair) wc2;
        return grp1.getGenre().compareTo(grp2.getGenre());
    }
}
