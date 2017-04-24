package gov.noaa.wres.datamodel;

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

    /**
     * A common case: several rasters over the same space, like a video.
     * @return
     */
    public static LocationSingleTimeManyRasterData getLocationSingleTimeManyRasterData()
    {
        return PairFactoryImpl.getLocationSingleTimeManyRasterData();
    }

    /**
     * A common case: a single value for point locations, like an nwm channel timestep file.
     * @return
     */
    public static LocationManyTimeSingleNonRasterData getLocationManyTimeSingleNonRasterData()
    {
        return PairFactoryImpl.getLocationManyTimeSingleNonRasterData();
    }
}
