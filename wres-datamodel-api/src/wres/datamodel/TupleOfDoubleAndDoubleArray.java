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
 * @author jesse
 *
 */
public interface TupleOfDoubleAndDoubleArray
{
    /** Get the "obs_val1" value at a position */
    double getItemOne();
    double[] getItemTwo();
}
