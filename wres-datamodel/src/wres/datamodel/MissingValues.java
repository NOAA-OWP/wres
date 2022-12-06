package wres.datamodel;

import java.time.Duration;
import org.apache.commons.math3.util.Precision;

/**
 * Class for representing missing values associated with different data types.
 * 
 * @author James Brown
 */

public class MissingValues
{
    /** Default output for {@link Duration} when missing. */
    public static final Duration DURATION = null;

    /** Default output for {@link Double} when missing. */
    public static final double DOUBLE = Double.NaN;

    /** Default missing string. */
    public static final String STRING = "NA";

    /**
     * @param value the value to test
     * @return true if the value is missing or non-finite
     */

    public static boolean isMissingValue( double value )
    {
        return Precision.equals( value, MissingValues.DOUBLE, Precision.EPSILON ) || !Double.isFinite( value );
    }

    /**
     * Opposite of {@link #isMissingValue(double)} to allow a method reference.
     * @param value the value to test
     * @return true if the value is not missing
     */

    public static boolean isNotMissingValue( double value )
    {
        return !MissingValues.isMissingValue( value );
    }

    /**
     * Do not construct.
     */

    private MissingValues()
    {
    }
}
