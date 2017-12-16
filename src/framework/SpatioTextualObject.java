/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package framework;

import util.experiment.ExperimentResult;

public interface SpatioTextualObject extends ExperimentResult{

    public int getId();

    public double getLatitude();

    public double getLongitude();

    public double getScore();
    
    public double getDistancia();
}
