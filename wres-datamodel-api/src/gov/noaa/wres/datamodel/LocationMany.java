package gov.noaa.wres.datamodel;

/**
 * There are many locations in this data.
 * @author jesse
 *
 */
public interface LocationMany
{
    /**
     * Reports whether there are different locations for each time step
     * 
     * @return true if locations are potentially different per time step,
     *         false otherwise
     */
    public boolean locationVariesInTime();

    /** Get all the locations */
    public WresPoint[] getAllWresPoints();
    
    /**
     * Get the locations at a particular time - NOT an index, a TIME
     * @see TimeConversion
     **/
    public WresPoint[] getWresPointsAtInternalTime(int internalTime);

}
