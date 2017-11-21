package wres.datamodel.inputs.pairs;

/**
 * Pair of primitive double and primitive double[]. Models the case of a pair that comprises: {obs_val1,
 * [fc1_val1, fc1_val2, ..., fc1_valn]}.
 *
 * @author jesse
 */
public interface PairOfDoubleAndVectorOfDoubles
extends Comparable<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * Return the first value.
     * 
     * @return in the above example, the "obs_val1" value
     */

    double getItemOne();

    /**
     * Return the second value.
     * 
     * @return in the above example, [fc_val1, fc1_val2, ..., fc1_valn] value
     */

    double[] getItemTwo();
}
