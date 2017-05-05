package wres.datamodel;

import java.util.List;

/**
 * Attempt to model the outer case of data looking like:
 * 
 * [
 *   [obs_val1, [fc1_val1, fc1_val2 ... fc1_valn]
 *   [obs_val2, [fc2_val1, fc2_val2 ... fc2_valn]
 *   ...
 *   [obs_valn, [fcn_val1, fcn_val2 ... fcn_valn]
 * ]
 *
 * @author jesse
 *
 */
public interface TuplesOfDoubleAndDoubleArray
{
    List<TupleOfDoubleAndDoubleArray> getTuplesOfDoubleAndDoubleArray();
}
