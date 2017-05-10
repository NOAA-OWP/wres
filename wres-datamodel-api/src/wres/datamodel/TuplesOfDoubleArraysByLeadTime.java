package wres.datamodel;

/**
 * To use in the case of ensemble forecast vs ensemble forecast
 * 
 * @author jesse
 *
 */
public interface TuplesOfDoubleArraysByLeadTime
extends ByLeadTime<Pair<DoubleArray,DoubleArray>>, TuplesOfDoubleArrays
{
}
