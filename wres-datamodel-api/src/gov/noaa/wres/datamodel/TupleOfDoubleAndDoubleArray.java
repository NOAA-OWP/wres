package gov.noaa.wres.datamodel;

/**
 * Attempt to model the  case of tuple of data looking like:
 * 
 *   [obs_val1, [fc1_val1, fc1_val2 ... fc1_valn]
 *   
 * Length is unnecessary because we have a tuple, the second element
 * has its own length in it.
 * 
 * Use getKey method to retrieve the first value of the tuple,
 * the method to retrieve the second value of the tuple is from ByIndex.
 *
 * @author jesse
 * 
 */
public interface TupleOfDoubleAndDoubleArray extends DoubleBrick
{
    /** Get the "obs_val1" value at a position */
    double getKey();
    /** value is simply getDoubles from DoubleBrick */
}
