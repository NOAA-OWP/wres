package wres.datamodel;

/**
 * Transform to/from internal slimmer time representation.
 * 
 * @author jesse
 *
 */
public class TimeConversion
{
    /**
     * Convert from internal time to LocalDateTime in UTC
     * @param slimTime the internal integer time
     * @return the time as LocalDateTime in UTC
     */
    public static java.time.LocalDateTime localDateTimeOf(int slimTime)
    {
        return TimeConversionImpl.localDateTimeOf(slimTime);
    }

    /**
     * Convert from LocalDateTime in UTC to internal time
     * @param fatTime the external LocalDateTime
     * @return the internal integer time
     */
    public static int internalTimeOf(java.time.LocalDateTime fatTime)
    {
        return TimeConversionImpl.internalTimeOf(fatTime);
    }

    /**
     * Convert from legacy Java Date to internal time
     * @param fatUtilTime the external Date
     * @return the internal integer time
     */
    public static int internalDateOf(java.util.Date fatUtilTime)
    {
        return TimeConversionImpl.internalTimeOf(fatUtilTime);
    }

    /**
     * Convert from SQL Java Date to internal time
     * @param fatSqlTime the external Date
     * @return the internal integer time
     */
    public static int internalDateOf(java.sql.Date fatSqlTime)
    {
        return TimeConversionImpl.internalTimeOf(fatSqlTime);
    }
}
