/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package s2i;

import xxl.util.TreeHeapEntry;

/**
 *
 * @author joao
 */
public class SpatioTreeHeapEntry extends TreeHeapEntry{
    private double lowerBoundSpatialScore;    

    public SpatioTreeHeapEntry(Object entry){
        super(entry);
    }

    public void setLowerBoundSpatialScore(double spatioPartialScore){
        this.lowerBoundSpatialScore=spatioPartialScore;
    }
    
    public double getLowerBoundSpatialScore(){
        return this.lowerBoundSpatialScore;
    }

}
