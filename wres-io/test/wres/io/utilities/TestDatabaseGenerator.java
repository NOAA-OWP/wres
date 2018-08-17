package wres.io.utilities;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.V9_6;

import wres.system.DatabaseSchema;
import wres.system.SystemSettings;

@RunWith( PowerMockRunner.class)
@PrepareForTest({DatabaseSchema.class})
public abstract class TestDatabaseGenerator
{
    private static final Logger LOGGER = LoggerFactory.getLogger( TestDatabaseGenerator.class );
    private static final AtomicInteger PORT = new AtomicInteger( 5555 );
    private static final AtomicInteger DATABASE_COUNTER = new AtomicInteger(  );
    private static final String USERNAME = "wres_user";
    private static final String PASSWORD = "test";
    private static final String HOST = "localhost";
    private static final String CONFIG_PATH = ".postgres_testinstance";

    public static class DatabaseAndConnections
    {
        private DatabaseAndConnections(
                final String name,
                final EmbeddedPostgres database,
                final ComboPooledDataSource dataSource
        )
        {
            this.database = database;
            this.dataSource = dataSource;
            this.name = name;
        }

        public Connection getConnection() throws SQLException
        {
            return this.dataSource.getConnection();
        }

        public void close()
        {
            LOGGER.trace( "Closing the database named {}.", this.name );
            this.dataSource.close();
            this.database.stop();
            LOGGER.trace("The database named {} has been closed.", this.name);
        }

        private final EmbeddedPostgres database;
        private final ComboPooledDataSource dataSource;
        private final String name;
    }

    public static DatabaseAndConnections createDatabase() throws IOException, PropertyVetoException, SQLException
    {
        final String name = "wrestest" + DATABASE_COUNTER.incrementAndGet();
        LOGGER.trace("A database named {} is being created...", name);

        EmbeddedPostgres instance = new EmbeddedPostgres( V9_6 );
        int portNumber = PORT.getAndIncrement();

        String jdbcURL = instance.start(
                EmbeddedPostgres.cachedRuntimeConfig( Paths.get( CONFIG_PATH) ),
                HOST,
                portNumber,
                name,
                USERNAME,
                PASSWORD,
                new ArrayList<>(  )
        );

        ComboPooledDataSource dataSource = new ComboPooledDataSource(  );
        dataSource.setDriverClass("org.postgresql.Driver");
        dataSource.setAutoCommitOnClose(true);
        dataSource.setInitialPoolSize( 10 );
        dataSource.setMaxIdleTime( 30 );
        dataSource.setMaxPoolSize( 20 );
        dataSource.setUser( USERNAME );
        dataSource.setPassword( PASSWORD );
        dataSource.setJdbcUrl(jdbcURL);

        // Create the tables needed for this test
        try ( Connection con = dataSource.getConnection())
        {
            DatabaseSchema schema = PowerMockito.spy( new DatabaseSchema(name) );
            //DatabaseSchema schema = new DatabaseSchema( name );
            PowerMockito.doReturn( "../dist/lib/conf/database/db.changelog-master.xml" ).when( schema, "getChangelogURL" );
            //PowerMockito.when( schema.getChangelogURL() ).thenReturn( "../dist/lib/conf/database/db.changelog-master.xml" );
            schema.applySchema( con );
        }
        catch ( Exception e )
        {
            throw new SQLException("The function used to find liquibase scripts could not be mocked.", e);
        }

        // Because SystemSettings is static, and parses XML in constructor,
        // and referred to elsewhere, need to use powermock to replace it
        // instead of a simpler mock:
        PowerMockito.mockStatic( SystemSettings.class);

        // TODO: Evaluate if this is actually mocking 'getConnectionPool' properly
        PowerMockito.when(SystemSettings.getConnectionPool())
                    .thenReturn(dataSource);

        // Need Database behavior to be actual methods except for getPool()
        PowerMockito.spy(Database.class);

        // TODO: Evaluate if this is actually mocking 'getPool' properly
        PowerMockito.when(Database.getPool()).thenReturn(dataSource);
        Whitebox.setInternalState( Database.class, "CONNECTION_POOL", dataSource );

        LOGGER.trace("The database named {} has been created.", name);

        return new DatabaseAndConnections( name, instance, dataSource );
    }
}
