public class Rating {
    private final double avgRating;
    private final int numVotes;

    public Rating(double avgRating, int numVotes) {
        this.avgRating = avgRating;
        this.numVotes = numVotes;
    }

    public double getAvgRating() {
        return avgRating;
    }

    public int getNumVotes() {
        return numVotes;
    }
}
