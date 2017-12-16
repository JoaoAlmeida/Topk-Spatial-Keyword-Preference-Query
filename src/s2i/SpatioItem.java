/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package s2i;

import util.SpatioTextualObject;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import util.MathUtils;
import util.file.PersistentEntry;
import util.file.PersistentEntryFactory;
import util.nra.NRAItem;

/**
 *
 * @author joao
 */
public class SpatioItem extends NRAItem implements PersistentEntry, SpatioTextualObject, Serializable, Comparable{

    public static int SIZE = (Integer.SIZE + 3 * Double.SIZE) / Byte.SIZE;
    private double latitude;
    private double longitude;
    private double remainingScore;
    private final double lowerBoundSpatialScore;
    private double distancia; //acrescentado por mim

    public SpatioItem() {
        this(-1, -1.0, -1.0, -1.0);
    }

    public SpatioItem(int id, double latitude, double longitude, double score) {
        this(id, latitude, longitude, score, 0);
    }

    SpatioItem(int id, double latitude, double longitude, double score, double spatioPartialScore) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.id = id;
        this.score = score;
        this.lowerBoundSpatialScore = spatioPartialScore;
        this.remainingScore = 0;        
    }

    public double getDistancia() {
        return distancia;
    }

    public void setDistancia(double distancia) {
        this.distancia = distancia;
    }
   
    public double getLatitude() {
        return this.latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    @Override
    protected void updateSource(int sourceIndex, int sourceSize) {
        super.updateSource(sourceIndex, sourceSize);
        remainingScore = 0;
        for (int i = 0; i < sourceSize; i++) {
            if ((bitmap & (1 << i)) == 0) { //The object was not found in the i source
                remainingScore += lowerBoundSpatialScore;
            }
        }
    }

    @Override
    public double updateUpperBoundScore(double[] upperBound) {
        upperBoundScore = 0;
        for (int i = 0; i < upperBound.length; i++) {
            if ((bitmap & (1 << i)) == 0) { //The object was not found in the i source
                upperBoundScore += Math.max(lowerBoundSpatialScore, upperBound[i]);
            }
        }
        upperBoundScore += score;
        return upperBoundScore;
    }

    @Override
    public double getScore() {
        return score + remainingScore;
        //return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public boolean equals(Object o) {
        SpatioTextualObject other = (SpatioTextualObject) o;
        return this.getId() == other.getId()
                && this.getLatitude() == other.getLatitude()
                && this.getLongitude() == other.getLongitude()
                && MathUtils.isEqual(this.getScore(), other.getScore());
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeInt(id);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
        out.writeDouble(score);
    }

    public void read(DataInputStream in) throws IOException {
        id = in.readInt();
        latitude = in.readDouble();
        longitude = in.readDouble();
        score = in.readDouble();
    }

    @Override
    public String toString() {
        return String.format("[id=%d,(%.4f,%.4f),%g]", getId(),
                getLatitude(), getLongitude(), getScore());
    }
    public static PersistentEntryFactory<SpatioItem> FACTORY = new PersistentEntryFactory<SpatioItem>() {
        public SpatioItem produce(DataInputStream input) throws IOException {
            SpatioItem entry = new SpatioItem();
            entry.read(input);
            return entry;
        }
    };

    public String getMessage() {
        return toString();
    }
    
    @Override
    public int compareTo(Object o) {

        if (o instanceof SpatioItem) {

            SpatioItem other = (SpatioItem) o;

            if (this.getScore()== other.getScore()) {
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
