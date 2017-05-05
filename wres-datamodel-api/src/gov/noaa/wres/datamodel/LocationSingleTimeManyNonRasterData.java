package gov.noaa.wres.datamodel;

/**
 * Part of an attempt at a more specific/meaningful "spacetimeobject"
 *
 * The idea being there were only 8 total kinds of objects,
 * so implement each (useful) one in a concrete type.
 *
 * Location could be single or many
 * Time could be single or many
 * Data underlying organization could be vector or raster
 *
 * 2 * 2 * 2 (except many of these permutations are unlikely to be used)
 *
 * This may be too meaningful for the system without lower-level types to
 * pass around underneath. For example, metrics may want double tuples
 * with no regard to times or spaces. Also, metrics may not care whether
 * things are observations or forecasts, metrics seem to care about
 * "first value" and "second value" with no extra knowledge.
 *
 * @author jesse
 *
 */
public interface LocationSingleTimeManyNonRasterData
extends LocationSingle, TimeMany, NonRaster
{
    /** Get only the values of the observations 
     * Should we even have this?
     */
    public double[] getObservationValues();

    /** Get the values for a forecast
     *  Should we even have this?
     */
    public double[] getForecastValues(String ensembleId);
    
    /** Get pairs of observation/forecast for a forecast
     *  Should we even have this? This would be getValues
     */
    public double[][] getObservationAndForecastValues(String ensembleId);
}
