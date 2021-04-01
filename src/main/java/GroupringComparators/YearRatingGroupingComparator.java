package GroupringComparators;

import WritableComparable.YearRatingPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearRatingGroupingComparator extends WritableComparator {
    public YearRatingGroupingComparator() {
        super(YearRatingPair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        YearRatingPair yrp1 = (YearRatingPair) wc1;
        YearRatingPair yrp2 = (YearRatingPair) wc2;
        return yrp1.getYear().compareTo(yrp2.getYear());
    }
}