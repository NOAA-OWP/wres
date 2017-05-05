package wres.datamodel;

/**
 * A placeholder class. Does not arrive in the API jar.
 * An implementation jar must implement this class.
 * 
 * @author jesse
 *
 */
public class TimeConversionImpl
{
    public static java.time.LocalDateTime localDateTimeOf(int slimTime)
    {
        throw new UnsupportedOperationException("This code should not make it into the api jar file");
    }

    public static int internalTimeOf(java.time.LocalDateTime fatTime)
    {
        throw new UnsupportedOperationException("This code should not make it into the api jar file");
    }

    public static int internalTimeOf(java.util.Date fatUtilTime)
    {
        throw new UnsupportedOperationException("This code should not make it into the api jar file");
    }

    public static int internalTimeOf(java.sql.Date fatSqlTime)
    {
        throw new UnsupportedOperationException("This code should not make it into the api jar file");
    }
}
