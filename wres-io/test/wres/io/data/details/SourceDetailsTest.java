package wres.io.data.details;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import wres.system.SystemSettings;

@Ignore
@RunWith( PowerMockRunner.class)
@PrepareForTest( SystemSettings.class)
@PowerMockIgnore( {"javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class SourceDetailsTest
{
    private ComboPooledDataSource connectionPoolDataSource;

    @Before
    public void setup() throws SQLException
    {
        // create our own data source connecting to in-memory H2 database
        connectionPoolDataSource = new ComboPooledDataSource();
        //connectionPoolDataSource.setJdbcUrl("jdbc:h2:mem:wres;DB_CLOSE_DELAY=-1");
        // helps h2 use a subset of postgres' syntax or features:
        //connectionPoolDataSource.setJdbcUrl("jdbc:h2:mem:wres;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        // Use this verbose one to figure out issues with queries/files/h2/etc:
        connectionPoolDataSource.setJdbcUrl("jdbc:h2:mem:wres;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;TRACE_LEVEL_SYSTEM_OUT=3");
        connectionPoolDataSource.setUser("SA");
        connectionPoolDataSource.setPassword("");

        try ( Connection connection = connectionPoolDataSource.getConnection();
              Statement statement = connection.createStatement() )
        {
            statement.execute( "CREATE SCHEMA wres" );
            statement.execute( "CREATE TABLE wres.source ( source_id INTEGER PRIMARY KEY AUTO_INCREMENT, path TEXT, output_time TIMESTAMP, is_point_data BOOLEAN, lead SMALLINT, hash BYTEA(16) UNIQUE );" );
        }
    }

    @Test
    public void saveSourceDetails() throws SQLException
    {
        // The sticky situation is in SystemSettings, static method getConnectionPool():
        PowerMockito.mockStatic( SystemSettings.class);
        // Substitute our H2 connection pool for both pools:
        when( SystemSettings.getConnectionPool() ).thenReturn( this.connectionPoolDataSource );
        when( SystemSettings.getHighPriorityConnectionPool() ).thenReturn( this.connectionPoolDataSource );

        SourceDetails.SourceKey sourceKey = SourceDetails.createKey( URI.create( "/this/is/just/a/test" ),
                                                                     "2017-06-16 11:13:00",
                                                                     null,
                                                                     "abc123" );
        SourceDetails sourceDetails = new SourceDetails( sourceKey );
        sourceDetails.save();
        assertTrue( "Expected source details to have performed insert.",
                    sourceDetails.performedInsert() );
        assertNotNull( "Expected the id of the source to be non-null",
                       sourceDetails.getId() );
    }

    @After
    public void tearDown() throws SQLException
    {
        try ( Connection connection = connectionPoolDataSource.getConnection();
              Statement statement = connection.createStatement() )
        {
            statement.execute( "DROP SCHEMA wres CASCADE" );
        }

        connectionPoolDataSource.close();
    }
}
