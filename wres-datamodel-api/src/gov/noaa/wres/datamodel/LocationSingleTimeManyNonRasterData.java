package gov.noaa.wres.datamodel;

public interface LocationSingleTimeManyNonRasterData
extends LocationSingle, TimeMany, NonRaster
{
    /** Get the values of the observations */
    public double[] getObservationValues();

    /** Get the values for a forecast */
    public double[] getForecastValues(String ensembleId);
}
