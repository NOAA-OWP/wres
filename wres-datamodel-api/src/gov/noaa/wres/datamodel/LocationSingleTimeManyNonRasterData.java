package gov.noaa.wres.datamodel;

public interface LocationSingleTimeManyNonRasterData
extends LocationSingle, TimeMany, NonRaster, Dataset<double[][]>
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
