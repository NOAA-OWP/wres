package wres.events.broker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;

/**
 * Utilities to help in finding broker information.
 * 
 * @author James Brown
 */

public class BrokerUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( BrokerUtilities.class );

    /** String denoting a broker on TCP reserved port 0 of IP address 127.0.0.1, aka localhost. */
    private static final String LOCALHOST_127_0_0_1_0 = "127.0.0.1:0";

    /** String denoting a broker on TCP reserved port 0 of localhost. */
    private static final String LOCALHOST_0 = "localhost:0";

    /**
     * <p>Attempts to read the broker connection properties from a JNDI resource on the class path. After reading the 
     * properties, applies any overrides from the system properties, specifically:</p>
     * 
     * <ol>
     * <li>wres.eventsBrokerAddress: the address of the host that contains the events broker</li>
     * <li>wres.eventsBrokerPort: the port on the host to which the broker is bound and listening</li>
     * </ol>
     * 
     * @param jndiProperties name the name of the JNDI properties resource on the class path
     * @return the named properties from the class path
     * @throws NullPointerException if the jndiProperties is null
     * @throws CouldNotLoadBrokerConfigurationException if the properties could not be found or loaded
     */

    public static Properties getBrokerConnectionProperties( String jndiProperties )
    {
        // Load the jndi.properties
        URL config = BrokerConnectionFactory.class.getClassLoader().getResource( jndiProperties );

        if ( Objects.isNull( config ) )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Could not find the " + jndiProperties
                                                                + " file on the class path." );
        }

        Properties properties = new Properties();
        try ( InputStream stream = config.openStream() )
        {
            properties.load( stream );

            LOGGER.debug( "Upon reading {}, discovered the following broker connection properties: {}",
                          jndiProperties,
                          properties );
        }
        catch ( IOException e )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Unable to load the expected broker configuration from "
                                                                + "properties file "
                                                                + jndiProperties
                                                                + " on the application "
                                                                + "class path.",
                                                                e );
        }

        // Update any property overrides
        LOGGER.debug( "Updating the broker connection properties with any system property overrides." );

        String connectionPropertyName = BrokerUtilities.getConnectionPropertyName( properties );
        BrokerUtilities.updateConnectionStringWithSystemPropertiesIfConfigured( connectionPropertyName, properties );

        return properties;
    }

    /**
     * Returns the connection property name from the map of properties.
     * 
     * @param properties the properties
     * @return the connection property name or null if none could be found
     * @throws NullPointerException if the properties is null
     */

    public static String getConnectionPropertyName( Properties properties )
    {
        Objects.requireNonNull( properties );

        for ( Entry<Object, Object> nextEntry : properties.entrySet() )
        {
            Object key = nextEntry.getKey();

            if ( Objects.nonNull( key ) && key.toString().contains( "connectionFactory" ) )
            {
                String name = key.toString();

                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Discovered a connection property name of {} with value {} among the broker "
                                  + "connection properties.",
                                  name,
                                  properties.get( key ) );
                }

                return name;
            }
        }

        LOGGER.warn( "Unable to locate a connection property name among the properties: {}.", properties );

        // None found
        return null;
    }

    /**
     * <p>Tests to establish whether an embedded broker is required. Begins by looking for a system property 
     * {@code wres.startBroker}. If {@code wres.startBroker=true}, then an embedded broker is required. If 
     * {@code wres.startBroker=false}, then an embedded broker is not required. If the system property is missing, then 
     * inspects the connection properties. If the connection properties contains a connection string that points to 
     * localhost, then a connection is verified to an active broker. If a connection does not succeed, an embedded 
     * broker is required.
     *
     * @param properties the broker connection properties
     * @return true if an embedded broker is required, otherwise false
     * @throws NullPointerException if the properties is null
     * @throws IllegalArgumentException if the connection property cannot be found
     */

    public static boolean isEmbeddedBrokerRequired( Properties properties )
    {
        Objects.requireNonNull( properties );

        String connectionPropertyName = BrokerUtilities.getConnectionPropertyName( properties );
        BrokerUtilities.testConnectionProperty( connectionPropertyName, properties );

        String url = properties.getProperty( connectionPropertyName );

        // Look for a system property that definitively says whether an embedded broker should be started
        String startBroker = System.getProperty( "wres.startBroker" );
        if ( "true".equalsIgnoreCase( startBroker ) )
        {
            LOGGER.info( "Discovered the WRES system property wres.startBroker=true, indicating that an embedded "
                         + "broker should be created and bound to {}...",
                         url );

            return true;
        }
        else if ( "false".equalsIgnoreCase( startBroker ) )
        {
            LOGGER.warn( "Discovered the WRES system property wres.startBroker=false. The evaluation will fail if no "
                         + "active broker is discovered at the binding URL (after exhausting any failover options): "
                         + "{}.",
                         url );

            return false;
        }

        boolean returnMe = false;

        // Loopback interface or all local interfaces? If so, an embedded broker may be required.
        if ( url.contains( "localhost" ) || url.contains( "127.0.0.1" ) || url.contains( "0.0.0.0" ) )
        {
            LOGGER.debug( "Discovered the connection property {} with value {}, which "
                          + "indicates that a broker should be listening on localhost.",
                          connectionPropertyName,
                          url );

            // Does the url contain the tcp reserved port 0, i.e. dynamic binding required?
            if ( url.contains( LOCALHOST_0 ) || url.contains( LOCALHOST_127_0_0_1_0 ) )
            {
                LOGGER.info( "Discovered a binding URL of {}, which includes the reserved TCP port of 0. An "
                             + "embedded broker will be created at this URL and the broker allowed to assign a port "
                             + "dynamically.",
                             url );

                returnMe = true;
            }
            // Look for an active broker, fall back on an embedded one
            else
            {
                // If retries are configured, then expect retries here, even if the connection ultimately fails
                LOGGER.debug( "Probing to establish whether an active broker is accepting connections at {}. This "
                              + "may fail!",
                              url );
                try
                {
                    LOGGER.warn( "Probing for an active AMQP broker at the binding URL {}. This may take some time if "
                                 + "no active broker exists and retries are configured. If no active broker is "
                                 + "discovered, an embedded broker will be required.",
                                 url );

                    BrokerConnectionFactory.testConnection( properties, 0 );

                    LOGGER.info( "Discovered an active AMQP broker at {}", url );
                }
                catch ( BrokerConnectionException e )
                {
                    LOGGER.info( "Could not connect to an active AMQP broker at {}. An embedded broker is required.",
                                 url );

                    returnMe = true;
                }
            }
        }

        return returnMe;
    }

    /**
     * Tests the connection property name and throws an exception if the name is not found in the properties
     * @param connectionPropertyName the connection property name
     * @param properties the properties
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the name cannot be found
     */

    static void testConnectionProperty( String connectionPropertyName, Properties properties )
    {
        Objects.requireNonNull( connectionPropertyName );
        Objects.requireNonNull( properties );

        if ( !properties.containsKey( connectionPropertyName ) )
        {
            throw new IllegalArgumentException( "Could not locate a connection property '"
                                                + connectionPropertyName
                                                + "' in the map of properties: "
                                                + properties
                                                + "." );
        }
    }

    /**
     * Checks for a <code>wres.eventsBrokerPort</code> and/or <code>wres.eventsBrokerAddress</code> system property and 
     * substitutes these entries into the binding url contained in the supplied properties.
     * 
     * @param connectionPropertyName the connection property name
     * @param properties the properties to update
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the connection property cannot be found
     */

    private static void updateConnectionStringWithSystemPropertiesIfConfigured( String connectionPropertyName,
                                                                                Properties properties )
    {
        Objects.requireNonNull( connectionPropertyName );
        Objects.requireNonNull( properties );

        if ( !properties.containsKey( connectionPropertyName ) )
        {
            throw new IllegalArgumentException( "Could not locate a connection property '"
                                                + connectionPropertyName
                                                + "' in the map of properties: "
                                                + properties
                                                + "." );
        }

        String oldConnectionProperty = properties.getProperty( connectionPropertyName );
        String connectionProperty = oldConnectionProperty;

        Properties systemProperties = System.getProperties();

        boolean overriden = false;

        if ( systemProperties.containsKey( "wres.eventsBrokerAddress" ) )
        {
            Object address = systemProperties.get( "wres.eventsBrokerAddress" );
            connectionProperty = connectionProperty.replaceAll( "amqp://+[a-zA-Z0-9.]+",
                                                                "amqp://" + address );
            overriden = true;
        }

        if ( systemProperties.containsKey( "wres.eventsBrokerPort" ) )
        {
            Object port = systemProperties.get( "wres.eventsBrokerPort" );

            String innerConnectionProperty = connectionProperty;

            connectionProperty = connectionProperty.replaceAll( ":(?<port>\\d+)", ":" + port );

            LOGGER.debug( "Updated the port in the binding URL. The old binding URL was: {}. The new binding URL is: "
                          + "{}.",
                          innerConnectionProperty,
                          connectionProperty );

            overriden = true;
        }

        // Update the properties
        properties.setProperty( connectionPropertyName, connectionProperty );

        if ( LOGGER.isDebugEnabled() && overriden )
        {
            LOGGER.debug( "Updated the binding URL (BURL) using system properties discovered at runtime. The old "
                          + "BURL was {}. The new BURL is {}.",
                          oldConnectionProperty,
                          connectionProperty );
        }
    }

    /**
     * Do not construct.
     */

    private BrokerUtilities()
    {
    }

}
