package wres.tasker;

/**
 * Helper to discover environment information, specifically information present
 * from environment variables, and more specifically to answer the question
 * "am I running in -dev, -ti, or production?"
 */
class Environment
{
    static final String SYSTEM_PROPERTY_NAMING_WRES_ENVIRONMENT = "wres.environment";

    /**
     * Returns a string that can be appended to a base hostname in order to
     * vary the hostname used per-environent. If there is a production string
     * it will be blank. Uses the $WRES_ENV_SUFFIX environment variable which
     * contains the dash in it already.
     * @return an always-appendable-to-hostname, non-null string for use in
     * reading a server private key file or server x509 certificate file.
     */

    static String getEnvironmentSuffix()
    {
        String descriptor = System.getProperty(
                SYSTEM_PROPERTY_NAMING_WRES_ENVIRONMENT );

        if ( descriptor == null || descriptor.isEmpty() )
        {
            return "";
        }
        else
        {
            return descriptor;
        }
    }
}
