package wres.system;

import java.sql.DriverManager;
import javax.sql.DataSource;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

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

    // The query timeout needs to be in seconds and we're setting the default for 5 hours (arbitrarily large)
    @Builder.Default
    @XmlElement( name = "query_timeout" )
    int queryTimeout = 18000;

    // Get-connection timeout is in milliseconds. Default to default query timeout.
    @Builder.Default
    @XmlElement( name = "connectionTimeoutMs" )
    int connectionTimeoutMs = 18000000;

    @XmlElement( name = "database_type" )
    private void setDatabaseType( String databaseType )
    {
        this.databaseType = DatabaseType.valueOf( databaseType );
    }

    // Dummy class to allow javadoc task to find the builder created by lombok
    public static class DatabaseSettingsBuilder {}  // NOSONAR
}
