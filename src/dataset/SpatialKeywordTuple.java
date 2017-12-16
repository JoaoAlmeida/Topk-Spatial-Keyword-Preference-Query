package dataset;

import util.tuples.Tuple;

public class SpatialKeywordTuple extends Tuple {

    private int id;
    private String text;
    private double score;
    

    public SpatialKeywordTuple(double[] values, String text, int id) {
        super(values);
        this.text = text;
        this.id = id;
    }

    public SpatialKeywordTuple(double[] values, String text, int id, double score) {
        super(values);
        this.text = text;
        this.id = id;
        this.score = score;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String toString() {
        //return String.format("Tuple:%-5d  [%4.2f %5.2f] %5s ",id,this.getValue(0),this.getValue(1),text);
        return String.format("%d %.8e %.8e %s ", id, this.getValue(0), this.getValue(1), text);
    }

    /*ACRESCENTADO POR MIM! */
    @Override
    public int compareTo(Object o) {

        if (o instanceof SpatialKeywordTuple) {

            SpatialKeywordTuple other = (SpatialKeywordTuple) o;

            if (this.getScore() == other.getScore()) {
                return this.getId() - other.getId();
            } else if (this.getScore() > other.getScore()) {
                return 1;
            } else if (this.getScore() < other.getScore()) {
                return -1;
            }
        } else {
            throw new RuntimeException("It must be a DataObject!");
        }
        return 0;
    }
}
