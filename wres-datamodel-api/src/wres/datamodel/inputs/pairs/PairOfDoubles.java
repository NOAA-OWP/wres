package wres.datamodel.inputs.pairs;

/**
 * <p>Pair of primitive double values.</p>
 * 
 * <p>An example might be the simplest forecast/observation timeseries data, but stripped of 
 * any/all time information, and containing only the values, i.e. {obs, fcst}.</p>
 *
 * @author jesse
 *
 */
public interface PairOfDoubles
extends Comparable<PairOfDoubles>
{
    /**
     * Returns the first value, i.e. the obs in the above example.
     * 
     * @return the first value
     */
    
    double getItemOne();
    
    /**
     * Returns the second value, i.e. the fcst in the above example.
     * 
     * @return the second value
     */
    
    double getItemTwo();
}
