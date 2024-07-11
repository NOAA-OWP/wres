package wres.io.database;

import java.util.Properties;
import javax.sql.DataSource;

import com.github.marschall.jfr.jdbc.JfrDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
import wres.system.SettingsHelper;
import wres.system.SystemSettings;

/**
 * Class that supplies database connections.
 */
public class ConnectionSupplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ConnectionSupplier.class );

    private final SystemSettings systemSettings;

    /**
     * The standard priority set of connections to the database
     */
    private final DataSource connectionPool;


    /**
     * A higher priority set of connections to the database used for operations
     * that absolutely need to operate within the database with little to no
     * competition for resources. Should be used sparingly
     */
    private final DataSource highPriorityConnectionPool;

    /**
     * Create an instance with the supplied settings.
     * @param systemSettings the settings
     */
    public ConnectionSupplier( SystemSettings systemSettings )
    {
        this.systemSettings = systemSettings;
        this.connectionPool = this.makeConnectionPool();
        this.highPriorityConnectionPool = this.makeHighPriorityConnectionPool();
    }

    /**
     * @return the system settings
     */
    public SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    /**
     * @return the ordinary priority database data source
     */
    public DataSource getConnectionPool()
    {
        return this.connectionPool;
    }

    /**
     * @return the high priority database data source
     */
    public DataSource getHighPriorityConnectionPool()
    {
        return this.highPriorityConnectionPool;
    }

    /**
     * @return A new instance of a connection pool that is built for the system wide configuration
     */
    private DataSource makeConnectionPool()
    {
        DatabaseSettings databaseConfiguration = this.systemSettings.getDatabaseConfiguration();
        int maxPoolSize = databaseConfiguration.getMaxPoolSize();
        LOGGER.info( "Creating a database connection pool with {} connections...", maxPoolSize );
        long connectionTimeoutMs = databaseConfiguration.getConnectionTimeoutMs();
        DataSource inner;
        try
        {
            inner = createDataSource( maxPoolSize, connectionTimeoutMs );
        }
        catch ( HikariPool.PoolInitializationException e )
        {
            throw new IllegalStateException( "Unable to create pool", e );
        }
        return new JfrDataSource( inner ); // Monitor JDBC traffic with JFR: #61680
    }

    /**
     * @return a high-priority connection pool
     */
    private DataSource makeHighPriorityConnectionPool()
    {
        DatabaseSettings databaseConfiguration = this.systemSettings.getDatabaseConfiguration();
        int maxPoolSize = databaseConfiguration.getMaxHighPriorityPoolSize();
        LOGGER.info( "Creating a high-priority database connection pool with {} connections...", maxPoolSize );
        long connectionTimeoutMs = databaseConfiguration.getConnectionTimeoutMs();
        DataSource inner = createDataSource( maxPoolSize, connectionTimeoutMs );
        return new JfrDataSource( inner ); // Monitor JDBC traffic with JFR: #61680
    }

    /**
     * @param maxPoolSize the maximum pool size
     * @param connectionTimeOutMs the maximum connection timeout in milliseconds
     * @return the data source
     */
    private DataSource createDataSource( int maxPoolSize, long connectionTimeOutMs )
    {
        DatabaseSettings databaseConfiguration = this.systemSettings.getDatabaseConfiguration();
        HikariConfig poolConfig = new HikariConfig();
        Properties properties = getConnectionProperties();
        poolConfig.setDataSourceProperties( properties );
        DatabaseType type = databaseConfiguration.getDatabaseType();
        String className = type.getDataSourceClassName();
        poolConfig.setDataSourceClassName( className );
        poolConfig.setMaximumPoolSize( maxPoolSize );
        poolConfig.setConnectionTimeout( connectionTimeOutMs );
        return new HikariDataSource( poolConfig );
    }

    /**
     * @return the database connection properties
     */

    private Properties getConnectionProperties()
    {
        DatabaseSettings databaseConfiguration = this.systemSettings.getDatabaseConfiguration();
        return SettingsHelper.getDatasourceProperties( databaseConfiguration );
    }
}
