package wres;

/**
 * Used to retrieve the version of the WRES software (top-level, wres jar)
 */
public class Version
{
    private final Package rawPackage;

    Version()
    {
        this.rawPackage = this.getClass().getPackage();
    }

    @Override
    public String toString()
    {
        Package toGetVersion = this.getRawPackage();

        if (toGetVersion != null && toGetVersion.getImplementationVersion() != null)
        {
            // When running from a released zip, the version should show up.
            return "WRES version " + toGetVersion.getImplementationVersion();
        }
        else
        {
            // When running from source, this will be the expected outcome.
            return "WRES version is unknown, probably developer version.";
        }
    }

    private Package getRawPackage()
    {
        return this.rawPackage;
    }
}
