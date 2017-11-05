package wres.datamodel;

/**
 * An enumeration of reference time systems.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public enum ReferenceTime
{

    /**
     * Valid time.
     */

    VALID_TIME,

    /**
     * Forecast issue time or basis time.
     */

    ISSUE_TIME;


    @Override
    public String toString()
    {
        return name().replaceAll( "_", " " );
    }

}
