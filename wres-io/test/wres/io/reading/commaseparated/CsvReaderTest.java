package wres.io.reading.commaseparated;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link CsvReader}.
 * @author James Brown
 */

class CsvReaderTest
{
    private static final String TEST_CSV = "test.csv";
    private static final String TEST = "test";
    private static final String DRRC2 = "DRRC2";
    private static final String DRRC3 = "DRRC3";
    private static final String CFS = "CFS";
    private static final String QINE = "QINE";
    private static final Instant T1985_06_01T15_00_00Z = Instant.parse( "1985-06-01T15:00:00Z" );
    private static final Instant T1985_06_01T14_00_00Z = Instant.parse( "1985-06-01T14:00:00Z" );
    private static final Instant T1985_06_01T13_00_00Z = Instant.parse( "1985-06-01T13:00:00Z" );
    private static final Instant T1985_06_01T12_00_00Z = Instant.parse( "1985-06-01T12:00:00Z" );
    private static final Instant T1985_06_02T12_00_00Z = Instant.parse( "1985-06-02T12:00:00Z" );
    private static final Instant T1985_06_02T13_00_00Z = Instant.parse( "1985-06-02T13:00:00Z" );
    private static final Instant T1985_06_02T14_00_00Z = Instant.parse( "1985-06-02T14:00:00Z" );

    @Test
    void testReadObservationsResultsInTwoTimeSeries() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "value_date,variable_name,location,measurement_unit,value" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC3,CFS,4" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC3,CFS,5" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC3,CFS,6" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );

            CsvReader reader = CsvReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .collect( Collectors.toList() );

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 3.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataTwo =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) ),
                                               CFS );

                TimeSeries<Double> expectedTwo =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataTwo )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 4.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 5.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 6.0 ) )
                                                        .build();

                List<TimeSeries<Double>> expected = List.of( expectedOne, expectedTwo );

                assertEquals( expected, actual );
            }

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testReadForecastsResultsInThreeTimeSeriesAcrossTwoFeatures() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "value_date,variable_name,location,measurement_unit,value" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC3,CFS,4" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC3,CFS,5" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC3,CFS,6" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );

            CsvReader reader = CsvReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( InputStream inputStream = new BufferedInputStream( Files.newInputStream( csvPath ) );
                  Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource, inputStream ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .collect( Collectors.toList() );

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 3.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataTwo =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) ),
                                               CFS );

                TimeSeries<Double> expectedTwo =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataTwo )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 4.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 5.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 6.0 ) )
                                                        .build();

                List<TimeSeries<Double>> expected = List.of( expectedOne, expectedTwo );

                assertEquals( expected, actual );
            }

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

    @Test
    void testReadForecastsResultsInThreeTimeSeries() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( "start_date,value_date,variable_name,location,measurement_unit,value" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-02T12:00:00Z,1985-06-02T13:00:00Z,QINE,DRRC2,CFS,3" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-02T12:00:00Z,1985-06-02T14:00:00Z,QINE,DRRC2,CFS,4" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T13:00:00Z,QINE,DRRC3,CFS,5" )
                      .append( System.lineSeparator() )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T14:00:00Z,QINE,DRRC3,CFS,6" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );

            CsvReader reader = CsvReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .collect( Collectors.toList() );

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN, T1985_06_01T12_00_00Z ),
                                               null,
                                               QINE,
                                               FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataTwo =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN, T1985_06_02T12_00_00Z ),
                                               null,
                                               QINE,
                                               FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) ),
                                               CFS );

                TimeSeries<Double> expectedTwo =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataTwo )
                                                        .addEvent( Event.of( T1985_06_02T13_00_00Z, 3.0 ) )
                                                        .addEvent( Event.of( T1985_06_02T14_00_00Z, 4.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataThree =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN, T1985_06_01T12_00_00Z ),
                                               null,
                                               QINE,
                                               FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) ),
                                               CFS );

                TimeSeries<Double> expectedThree =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataThree )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 5.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 6.0 ) )
                                                        .build();

                List<TimeSeries<Double>> expected = List.of( expectedOne, expectedTwo, expectedThree );

                assertEquals( expected, actual );
            }

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

}
