package wres.reading.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.Variable;
import wres.datamodel.MissingValues;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.TimeSeriesTuple;
import wres.reading.DataSource.DataDisposition;
import wres.statistics.MessageUtilities;
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
    void testReadObservationsWithEmptyValueResultsInOneTimeSeries() throws IOException
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
                writer.append( "value_date,variable_name,location,measurement_unit,value\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null, Set.of() ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );

            CsvReader reader = CsvReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .toList();

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               Feature.of( MessageUtilities.getGeometry( DRRC2 ) ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z,
                                                                             MissingValues.DOUBLE ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 3.0 ) )
                                                        .build();

                List<TimeSeries<Double>> expected = List.of( expectedOne );

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
                writer.append( "value_date,variable_name,location,measurement_unit,value\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC3,CFS,4\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC3,CFS,5\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC3,CFS,6" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null, Set.of() ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );

            CsvReader reader = CsvReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .toList();

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               Feature.of( MessageUtilities.getGeometry( DRRC2 ) ),
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
                                               Feature.of( MessageUtilities.getGeometry( DRRC3 ) ),
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
    void testReadObservationsWithCommentLinesResultsInTwoTimeSeries() throws IOException
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
                writer.append( "# look, a header!\n" )
                      .append( "# look, another header!\n" )
                      .append( "# look, yet another header!\n" )
                      .append( "value_date,variable_name,location,measurement_unit,value\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3\n" )
                      .append( "# the values after this line are bigger than the earlier lines\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC3,CFS,4\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC3,CFS,5\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC3,CFS,6" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null, Set.of() ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );

            CsvReader reader = CsvReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .toList();

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               Feature.of( MessageUtilities.getGeometry( DRRC2 ) ),
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
                                               Feature.of( MessageUtilities.getGeometry( DRRC3 ) ),
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
                writer.append( "value_date,variable_name,location,measurement_unit,value\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC3,CFS,4\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC3,CFS,5\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC3,CFS,6\n" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null, Set.of() ) );
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
                                                             .toList();

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               null,
                                               QINE,
                                               Feature.of( MessageUtilities.getGeometry( DRRC2 ) ),
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
                                               Feature.of( MessageUtilities.getGeometry( DRRC3 ) ),
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
                writer.append( "start_date,value_date,variable_name,location,measurement_unit,value\n" )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1\n" )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2\n" )
                      .append( "1985-06-02T12:00:00Z,1985-06-02T13:00:00Z,QINE,DRRC2,CFS,3\n" )
                      .append( "1985-06-02T12:00:00Z,1985-06-02T14:00:00Z,QINE,DRRC2,CFS,4\n" )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T13:00:00Z,QINE,DRRC3,CFS,5\n" )
                      .append( "1985-06-01T12:00:00Z,1985-06-01T14:00:00Z,QINE,DRRC3,CFS,6\n" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null, Set.of() ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );

            CsvReader reader = CsvReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .toList();

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, T1985_06_01T12_00_00Z ),
                                               null,
                                               QINE,
                                               Feature.of( MessageUtilities.getGeometry( DRRC2 ) ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataTwo =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, T1985_06_02T12_00_00Z ),
                                               null,
                                               QINE,
                                               Feature.of( MessageUtilities.getGeometry( DRRC2 ) ),
                                               CFS );

                TimeSeries<Double> expectedTwo =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataTwo )
                                                        .addEvent( Event.of( T1985_06_02T13_00_00Z, 3.0 ) )
                                                        .addEvent( Event.of( T1985_06_02T14_00_00Z, 4.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataThree =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, T1985_06_01T12_00_00Z ),
                                               null,
                                               QINE,
                                               Feature.of( MessageUtilities.getGeometry( DRRC3 ) ),
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

    @Test
    void testReadObservationsWithInconsistentDeclaredTimeZoneProducesExpectedException() throws IOException
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
                writer.append( "value_date,variable_name,location,measurement_unit,value\n" )
                      .append( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1\n" )
                      .append( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,\n" )
                      .append( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null, Set.of() ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.CSV_WRES );
            Mockito.when( dataSource.getSource() )
                   .thenReturn( SourceBuilder.builder()
                                             .timeZoneOffset( ZoneOffset.of( "-06:00" ) )
                                             .build() );

            CsvReader reader = CsvReader.of();

            // Validation happens on stream construction, even if reading happens later
            ReadException actual = assertThrows( ReadException.class,
                                                 () -> reader.read( dataSource ) );
            assertTrue( actual.getMessage()
                              .contains( "which is inconsistent with the CSV format requirement that all times are "
                                         + "supplied in UTC" ) );

            // Clean up
            if ( Files.exists( csvPath ) )
            {
                Files.delete( csvPath );
            }
        }
    }

}
