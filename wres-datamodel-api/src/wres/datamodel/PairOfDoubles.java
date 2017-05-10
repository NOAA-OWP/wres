package wres.datamodel;

/**
 * Pair of primitive double values.
 * 
 * An example might be the simplest forecast/observation timeseries data,
 * but stripped of any/all time information. Only the values.
 *
 * @author jesse
 *
 */
public interface PairOfDoubles
{
    double getItemOne();
    double getItemTwo();
}
