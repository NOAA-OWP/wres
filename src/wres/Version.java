package wres;

import java.util.StringJoiner;
import wres.system.SystemSettings;

/**
 * Used to retrieve the version of the WRES software (top-level, wres jar)
 */
public class Version
{
    private static final String UNKNOWN_VERSION = "unknown";
    private final Package rawPackage;

    Version()
    {
        this.rawPackage = this.getClass().getPackage();
    }

    /**
     * @return the version of the enclosing jar (the WRES version), or
     * "unknown" if unknown.
     */
    @Override
    public String toString()
    {
        Package toGetVersion = this.getRawPackage();

        if ( toGetVersion != null && toGetVersion.getImplementationVersion() != null )
        {
            // When running from a released zip, the version should show up.
            return toGetVersion.getImplementationVersion();
        }
        else
        {
            // When running from source, this will be the expected outcome.
            return UNKNOWN_VERSION;
        }
    }

    /**
     * Get a more lengthy description of the wres version
     * @return the lengthier description of the wres version
     */
    public String getDescription()
    {
        String shortVersion = this.toString();

        if ( !shortVersion.equals( UNKNOWN_VERSION ) )
        {
            return "WRES version " + shortVersion;
        }
        else
        {
            return "WRES version is " + UNKNOWN_VERSION
                   + ", probably developer version.";
        }
    }

    private Package getRawPackage()
    {
        return this.rawPackage;
    }


    /**
     * Get a lot of information about the system, including processors, jvm ram,
     * jvm system properties. Omit passphrases and classpaths.
     * @param systemSettings the system settings
     * @return lots of runtime information
     */

    public String getVerboseRuntimeDescription( SystemSettings systemSettings )
    {
        StringJoiner s = new StringJoiner( "; " );

        Runtime runtime = Runtime.getRuntime();

        final String MIB = "MiB";
        final long MEGABYTE = 1024L * 1024L;
        s.add( "Processors: " + runtime.availableProcessors() );
        s.add( "Max Memory: " + ( runtime.maxMemory() / MEGABYTE ) + MIB );
        s.add( "Free Memory: " + ( runtime.freeMemory() / MEGABYTE ) + MIB );
        s.add( "Total Memory: " + ( runtime.totalMemory() / MEGABYTE ) + MIB );
        s.add( "WRES System Settings: " + systemSettings.redacted());  // Important to redact these
        s.add( "Java System Properties: " + systemSettings.redactedSystemProperties() );  // Important to redact these

        return s.toString();
    }
}
