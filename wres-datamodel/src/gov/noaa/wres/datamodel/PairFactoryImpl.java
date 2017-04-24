package gov.noaa.wres.datamodel;

import gov.noaa.wres.datamodel.LocationSingleTimeManyNonRasterData;

public class PairFactoryImpl
{
    static LocationSingleTimeManyNonRasterData getLocationSingleTimeManyNonRasterData()
    {
        return LocationSingleTimeManyNonRasterDataImpl.of("SQIN",
                                                          "QINE",
                                                          null);
    }

    static LocationSingleTimeSingleRasterData getLocationSingleTimeSingleRasterData()
    {
        return (LocationSingleTimeSingleRasterData) get(false, false, true);
    }

    static LocationSingleTimeManyRasterData getLocationSingleTimeManyRasterData()
    {
        return (LocationSingleTimeManyRasterData) get(false, true, true);
    }

    static LocationManyTimeSingleNonRasterData getLocationManyTimeSingleNonRasterData()
    {
        return (LocationManyTimeSingleNonRasterData) get(true, false, false);
    }

    private static Object get(boolean spaceVaries,
                       boolean timeVaries,
                       boolean isRaster)
    {
        return new Object();
    }
}
