package wres.datamodel;

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


    private static Object get(boolean spaceVaries,
                       boolean timeVaries,
                       boolean isRaster)
    {
        return new Object();
    }
}
