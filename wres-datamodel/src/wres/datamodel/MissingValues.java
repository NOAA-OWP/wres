package wres.datamodel;

import java.time.Duration;

/**
 * Class for representing missing values associated with different data types.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MissingValues
{

    /**
     * Default output for {@link Duration} when missing.
     */

    public static final Duration DURATION = null;

    /**
     * Default output for {@link Double} when missing.
     */

    public static final double DOUBLE = Double.NaN;
    
    /**
     * Default missing string.
     */
    
    public static final String STRING = "NA";

    /**
     * Do not construct.
     */

    private MissingValues()
    {
    }

}
