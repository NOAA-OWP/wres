package gov.noaa.wres.datamodel;

/**
 * To use in the case of ensemble forecast vs ensemble forecast
 * 
 * @author jesse
 *
 */
public interface TuplesOfDoubleBricksByLeadTime
extends ByLeadTime<Tuple<DoubleBrick,DoubleBrick>>, TuplesOfDoubleBricks
{
}
