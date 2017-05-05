package wres.datamodel;

/**
 * Returns data in ready-for-metrics-computation form.
 * @author jesse
 *
 */
public class PairFactory
{
    /**
     * A common case: a timeseries for a fixed location, such as a hydrograph.
     * @return
     */    
    public static LocationSingleTimeManyNonRasterData getLocationSingleTimeManyNonRasterData()
    {
        return PairFactoryImpl.getLocationSingleTimeManyNonRasterData();
    }

    /**
     * A common case: a single raster for a single time, like a picture.
     * @return
     */
    public static LocationSingleTimeSingleRasterData getLocationSingleTimeSingleRasterData()
    {
        return PairFactoryImpl.getLocationSingleTimeSingleRasterData();
    }

}
