package wres;

import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;

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

        if (toGetVersion != null && toGetVersion.getImplementationVersion() != null)
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
     * @return lots of runtime information
     */

    public String getVerboseRuntimeDescription()
    {
        StringJoiner s = new StringJoiner( "; " );

        Runtime runtime = Runtime.getRuntime();

        final String MIB = "MiB";
        final long MEGABYTE = 1024 * 1024;
        s.add( "Processors: " + runtime.availableProcessors() );
        s.add( "Max Memory: " + ( runtime.maxMemory() / MEGABYTE ) + MIB );
        s.add( "Free Memory: " + ( runtime.freeMemory() / MEGABYTE ) + MIB );
        s.add( "Total Memory: " +  ( runtime.totalMemory() / MEGABYTE ) + MIB );

        // Order the property names for consistency.
        SortedSet<String> sortedPropertyNames =
                new TreeSet<>( System.getProperties().stringPropertyNames() );

        for ( String propertyName : sortedPropertyNames )
        {
            String lowerCaseName = propertyName.toLowerCase();

            // Avoid printing passphrases or passwords or passes of any kind,
            // avoid printing full classpath, ignore separators, ignore printers
            // and ignore some extraneous sun/oracle directories
            if ( !lowerCaseName.contains( "pass" )
                 && !lowerCaseName.contains( "class.path" )
                 && !lowerCaseName.contains( "separator" )
                 && !lowerCaseName.startsWith( "sun" )
                 && !lowerCaseName.contains( "user.country" )
                 && !lowerCaseName.startsWith( "java.vendor" )
                 && !lowerCaseName.startsWith( "java.e" )
                 && !lowerCaseName.contains( "java.vm.specification" )
                 && !lowerCaseName.contains( "printer" ) )
            {
                s.add( propertyName + ": " + System.getProperty( propertyName ) );
            }
        }

        return s.toString();
    }
}
