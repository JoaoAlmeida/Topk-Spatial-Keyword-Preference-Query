package util;

public class ScoredObject implements Comparable, SpatioTextualObject{
    private int id;
    private double score;
    private double distance;
    private double latitude;
    private double longitude;
    private String msg;    
    private SpatioItemCollection nn;
   
    public ScoredObject(int id, double lat, double lgt){
        this(id, lat, lgt, null);
    }
    
    public ScoredObject(int id, double lat, double lgt, String msg){
        this.id = id;
        this.latitude = lat;
        this.longitude = lgt;
        this.msg = msg;
    }

    public SpatioItemCollection getNn() {
        return nn;    
    }

    public void nnMode(){
        nn = new SpatioItemCollection();
    }
    
      @Override
    public int getId() {
        return id;
    }

    @Override
    public double getLatitude() {
        return latitude;
    }

    @Override
    public double getLongitude() {
        return longitude;
    }

    public void setMessage(String msg){
        this.msg =msg;
    }

    @Override
    public String getMessage() {
        return msg;
    }
    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public double getScore() {
        return this.score;
    }

    public void setDistancia(double distance) {
        this.distance = distance;
    }

    @Override
    public double getDistancia() {
        return this.distance;
    }

    @Override
    public int compareTo(Object other) {
        if(other instanceof ScoredObject){
            ScoredObject otherDocument = (ScoredObject) other;
            double thisScore = this.getScore();
            double otherScore = otherDocument.getScore();
            if(thisScore > otherScore){
                return 1;
            }else if(thisScore < otherScore){
                return -1;
            }else{//They are equals
                ScoredObject outro = (ScoredObject) other;
                return this.getId() -outro.getId();
                
            }
        }
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    @Override
    public String toString(){
        return "[id="+id+", score="+score+", lat="+latitude+", lgt="+longitude+", dist="+distance+", msg="+msg+"]";
    }
  
}
