package gov.noaa.wres.datamodel;

/**
 * Type allowing retrieval of a pair by by lead time.
 * 
 * Can get the whole array of pairs independent of lead time, or an 
 * individual pair by from a lead time.
 * 
 * Maybe meaningful at a high level while allowing extraction of the lower.
 * 
 * @author jesse
 *
 */
public interface TuplesOfDoublesByLeadTime
extends TuplesOfDoubles, ByLeadTime<TupleOfDoubles>
{

}
