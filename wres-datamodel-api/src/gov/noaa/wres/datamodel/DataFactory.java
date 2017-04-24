package gov.noaa.wres.datamodel;

public class DataFactory
{
    /**
     * A common case: a timeseries for a fixed location, such as a hydrograph.
     * @return
     */    
    public static LocationSingleTimeManyNonRasterData getLocationSingleTimeManyNonRasterData()
    {
        return DataFactoryImpl.getLocationSingleTimeManyNonRasterData();
    }

    /**
     * A common case: a single raster for a single time, like a picture.
     * @return
     */
    public static LocationSingleTimeSingleRasterData getLocationSingleTimeSingleRasterData()
    {
        return DataFactoryImpl.getLocationSingleTimeSingleRasterData();
    }

    /**
     * A common case: several rasters over the same space, like a video.
     * @return
     */
    public static LocationSingleTimeManyRasterData getLocationSingleTimeManyRasterData()
    {
        return DataFactoryImpl.getLocationSingleTimeManyRasterData();
    }

    /**
     * A common case: a single value for point locations, like an nwm channel timestep file.
     * @return
     */
    public static LocationManyTimeSingleNonRasterData getLocationManyTimeSingleNonRasterData()
    {
        return DataFactoryImpl.getLocationManyTimeSingleNonRasterData();
    }
}
