package wres.datamodel;

/** This is nonsense - by definition raster data has multiple locations */
public interface LocationSingleTimeSingleRasterData 
extends LocationSingle, TimeSingle, Raster
{
    public boolean shouldThisEvenExist();
}
