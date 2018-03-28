package wres;

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
}
