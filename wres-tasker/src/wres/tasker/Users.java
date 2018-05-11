package wres.tasker;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Helper to get properties from on-classpath users.properties file specifying
 * mappings between databases and users. Properties are database.name.[user],
 * database.host.[user], database.user.[user] and values are the postgres
 * database name, database host, and database user, respectively.
 */
class Users
{
    private static final String PROPERTIES_FILE = "users.properties";
    private static final Properties USER_PROPERTIES = Users.readProperties( PROPERTIES_FILE );

    private Users()
    {
        // Helper class with static methods has no constructor
    }

    /**
     *
     * @param user
     * @return the database name that user should use, null if not found
     */
    static String getDatabaseName( String user )
    {
        return USER_PROPERTIES.getProperty( "database.name." + user );
    }

    /**
     *
     * @param user the user to look for
     * @return the database host that user should use, null if not found
     */
    static String getDatabaseHost( String user )
    {
        return USER_PROPERTIES.getProperty( "database.host." + user );
    }

    /**
     *
     * @param user the user to look for
     * @return the database host that user should use, null if not found
     */

    static String getDatabaseUser( String user )
    {
        return USER_PROPERTIES.getProperty( "database.user." + user );
    }


    /**
     *
     * @param fileNameOnClasspath the filename to load
     * @return the properties read from the fileNameOnClasspath
     * @throws IllegalStateException when file is not found on classpath or
     * cannot be loaded.
     */

    static Properties readProperties( String fileNameOnClasspath )
    {
        Properties properties = new Properties();
        InputStream propertiesOnClasspath =
                Users.class.getClassLoader()
                           .getResourceAsStream( fileNameOnClasspath );

        if ( propertiesOnClasspath == null )
        {
            throw new IllegalStateException( "Failed to find user-to-database mappings on classpath at '"
                                             + fileNameOnClasspath + "'" );
        }

        try
        {
            properties.load( propertiesOnClasspath );
            return properties;
        }
        catch ( IOException ioe )
        {
            String message = "Failed to load user-to-database mappings from classpath at '"
                             + fileNameOnClasspath + "'";
            throw new IllegalStateException( message, ioe );
        }
    }
}
