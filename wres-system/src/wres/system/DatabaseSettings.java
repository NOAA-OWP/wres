package wres.system;

import java.sql.DriverManager;
import java.time.temporal.ChronoUnit;
import javax.sql.DataSource;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Contains access to configured settings and objects for accessing the database
 *
 * <p>Connections should mostly be acquired from a common pool
 * (HikariCP), which uses the {@link DataSource} route, not the {@link DriverManager} route. The latter should not be
 * used. When acquiring connections for a database lock manager, these connections should not be from the same pool,
 * but they should be obtained by creating a {@link DataSource}, not by using the {@link DriverManager}. See #103431
 * for further discussion. Once this work is completed, there should be no need to skip the parsing of database
 * settings when using in-memory mode, only the connection verification and database interaction. See the relevant
 * block of instantiation in {@link SettingsFactory} for this class.
 *
 */

@XmlAccessorType( XmlAccessType.NONE )
@Value
@NoArgsConstructor
@AllArgsConstructor
@Builder( toBuilder = true )
public class DatabaseSettings
{
    /** The precision of the lead duration units in the database. Until the database has been refactored to represent
     * time-series values at valid times (i.e., timestamps; see GitHub #630), rather than lead durations, the precision
     * of the lead duration units is centralized here for consistency across various database operations, such as ingest
     * and retrieval. All lead durations in the database schema are expressed in these whole units. See GitHub #719
     * also. */
    public static final ChronoUnit LEAD_DURATION_UNIT = ChronoUnit.SECONDS;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseSettings.class );

    // Initialize all available driver classes for known databases. Strictly, this should not be necessary for JDBC 4.0+
    // drivers, which should be loaded automatically. However, #103770 suggests otherwise in some environments or class
    // loading contexts.

    static
    {
        for ( DatabaseType nextType : DatabaseType.values() )
        {
            try
            {
                Class.forName( nextType.getDriverClassName() );
            }
            catch ( ClassNotFoundException classError )
            {
                LOGGER.debug( "Failed to load the database driver class {}.", nextType.getDriverClassName() );
            }
        }
    }

    /**
     * When the jdbcUrl is specified, it takes precedence over the fields used
     * to programmatically generate a jdbcUrl. The alternative to specifying a
     * jdbcUrl is to specify a type, host, port, and database name.
     * If username and password are specified or password is found in
     * pgpass file they will be added to the properties sent to the DataSource.
     * Furthermore, the jdbcUrl will override the specified database type.
     */

    @Builder.Default
    @XmlElement( name = "jdbcUrl" )
    String jdbcUrl = null;
    @Builder.Default
    @XmlElement( name = "host" )
    String host = "localhost";
    @Builder.Default
    @XmlElement( name = "username" )
    String username = "wres";
    @Builder.Default
    @XmlElement( name = "password" )
    String password = null;
    @Builder.Default
    @XmlElement( name = "port" )
    int port = 5432;
    @Builder.Default
    @XmlElement( name = "name" )
    String databaseName = "wres";
    @Builder.Default
    @NonFinal
    DatabaseType databaseType = DatabaseType.POSTGRESQL;
    @Builder.Default
    @XmlElement( name = "certificate_file_to_trust" )
    String certificateFileToTrust = null;
    @Builder.Default
    @XmlElement( name = "max_pool_size" )
    int maxPoolSize = 10;
    @Builder.Default
    @XmlElement( name = "max_high_priority_pool_size" )
    int maxHighPriorityPoolSize = 5;
    @Builder.Default
    @XmlElement( name = "max-idle_time" )
    int maxIdleTime = 30;
    @Builder.Default
    @XmlElement( name = "attempt_to_migrate" )
    boolean attemptToMigrate = true;

    @Builder.Default
    @XmlElement( name = "use_ssl" )
    boolean useSSL = false;
    @Builder.Default
    @XmlElement( name = "validate_ssl" )
    boolean validateSSL = true;

    // The query timeout needs to be in seconds, and we're setting the default for 5 hours (arbitrarily large)
    @Builder.Default
    @XmlElement( name = "query_timeout" )
    int queryTimeout = 18000;

    // Get-connection timeout is in milliseconds. Default to default query timeout.
    @Builder.Default
    @XmlElement( name = "connectionTimeoutMs" )
    int connectionTimeoutMs = 18000000;

    /**
     * Dummy class to allow javadoc task to find the builder created by lombok
     */
    public static class DatabaseSettingsBuilder {}  // NOSONAR
}
