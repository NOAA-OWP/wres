package wres.datamodel;

import java.time.ZoneOffset;
import java.time.LocalDateTime;

/**
 * Uses internal time in number of minutes since unix epoch.
 * Convert to and from the common date formats.
 * @author jesse
 *
 */
public class TimeConversionImpl
{
    private static final int FACTOR = 60;
    
    public static LocalDateTime localDateTimeOf(int slimTime)
    {
        return LocalDateTime.ofEpochSecond(slimTime * FACTOR, 0, ZoneOffset.UTC);
    }

    public static int internalTimeOf(LocalDateTime fatTime)
    {
        return (int) (fatTime.toEpochSecond(ZoneOffset.UTC) / FACTOR);
    }

    public static int internalTimeOf(java.util.Date fatUtilTime)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public static int internalTimeOf(java.sql.Date fatSqlTime)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
