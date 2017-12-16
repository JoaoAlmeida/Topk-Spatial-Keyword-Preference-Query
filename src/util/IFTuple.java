package util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import util.file.PersistentEntry;
import util.file.PersistentEntryFactory;

public class IFTuple implements PersistentEntry {
 
    //id lat lgt termImpact
    public static int SIZE = (Long.SIZE + 3* Double.SIZE) / Byte.SIZE;
    private long id;
    private double termImpact;
    private double latitude;
    private double longitude;

    public IFTuple(){}
    
    public IFTuple(long id, double termImpact, double latitude, double longitude) {
        this.id = id;
        this.termImpact = termImpact;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public long getID() {
        return this.id;
    }

    public double getTermImpact() {
        return this.termImpact;
    }
    
    public void setTermImpact(double termImpact) {
        this.termImpact = termImpact;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeLong(id);
        out.writeDouble(termImpact);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
    }

    public void read(DataInputStream in) throws IOException {
        id = in.readLong();
        termImpact = in.readDouble();
        latitude = in.readDouble();
        longitude = in.readDouble();
    }
    
     public static PersistentEntryFactory<IFTuple> FACTORY = new PersistentEntryFactory<IFTuple>() {
        public IFTuple produce(DataInputStream input) throws IOException {
            IFTuple entry = new IFTuple();
            entry.read(input);
            return entry;
        }
    };


}
