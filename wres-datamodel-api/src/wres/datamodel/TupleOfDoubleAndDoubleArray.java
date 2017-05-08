package wres.datamodel;

/**
 * Tuple of primitive double and primitive double[]
 * 
 * Attempt to model the  case of tuple of data looking like:
 * 
 *   [obs_val1, [fc1_val1, fc1_val2 ... fc1_valn]
 *   
 * Length of second element can be found from the second element.
 * It is just an array of primitive doubles.
 * 
 * Use getKey method to retrieve the first value of the tuple,
 * the method to retrieve the second value of the tuple is from DoubleBrick.
 *
 * @author jesse
 * 
 */
public interface TupleOfDoubleAndDoubleArray extends DoubleArray
{
    /** Get the "obs_val1" value at a position */
    double getKey();
    /** value is simply getDoubles from DoubleBrick */
}
