package wres.datamodel;

/**
 * Tuple of primitive double values as a primitive double[] with two values.
 * 
 * An example might be the simplest forecast/observation timeseries data,
 * but stripped of any/all time information. Only the values.
 *
 * @author jesse
 *
 */
public interface TupleOfDoubles
{
    double[] getTupleOfDoubles();
}
