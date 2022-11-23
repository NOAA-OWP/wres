package wres.io.reading.datacard;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;

/**
 * Tests the {@link DatacardReader}.
 * @author James Brown
 */

class DatacardReaderTest
{
    private static final String CFS = "CFS";
    private static final String QINE = "QINE";
    private static final Instant T1985_06_01T06_00_00Z = Instant.parse( "1985-06-01T06:00:00Z" );
    private static final Instant T1985_06_01T12_00_00Z = Instant.parse( "1985-06-01T12:00:00Z" );
    private static final Instant T1985_06_01T18_00_00Z = Instant.parse( "1985-06-01T18:00:00Z" );
    private static final Instant T1985_06_02T00_00_00Z = Instant.parse( "1985-06-02T00:00:00Z" );

    @Test
    void testReadObservationsResultsInTwoTimeSeries() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", "test.datacard" );
            Path cardPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( cardPath ) )
            {
                writer.append( "$OH datacard format:" )
                      .append( System.lineSeparator() )
                      .append( "$FromFile     Type Dim  Unit  Stp StationID  StationDesc (header card 1)" )
                      .append( System.lineSeparator() )
                      .append( "$m  yyyy mm   yyyy  col format (header card 2)" )
                      .append( System.lineSeparator() )
                      .append( "$StationID  mmyy day datavalue (values n cards)" )
                      .append( System.lineSeparator() )
                      .append( "ts740.2017090 QINE L3   CFS   6   DRRC2            DOLORES RIVER, CO" )
                      .append( System.lineSeparator() )
                      .append( "06  1985 06   1985  1   F9.1" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   1.0" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   2.0" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   3.0" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   4.0" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( cardPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.getSource() )
                   .thenReturn( new DataSourceConfig.Source( cardPath.toUri(), null, "UTC", null, null ) );
            Mockito.when( dataSource.hasSourcePath() )
                   .thenReturn( true );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.DATACARD );

            DatacardReader reader = DatacardReader.of();

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
                                               Feature.of( MessageFactory.getGeometry( "DRRC2",
                                                                                          "DOLORES RIVER, CO",
                                                                                          null,
                                                                                          null ) ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T06_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T12_00_00Z, 2.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T18_00_00Z, 3.0 ) )
                                                        .addEvent( Event.of( T1985_06_02T00_00_00Z, 4.0 ) )
                                                        .build();

                List<TimeSeries<Double>> expected = List.of( expectedOne );

                assertEquals( expected, actual );
            }

            // Clean up
            if ( Files.exists( cardPath ) )
            {
                Files.delete( cardPath );
            }
        }
    }

    @Test
    void testReadObservationsFromInputStreamResultsInTwoTimeSeries() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", "test.datacard" );
            Path cardPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( cardPath ) )
            {
                writer.append( "$OH datacard format:" )
                      .append( System.lineSeparator() )
                      .append( "$FromFile     Type Dim  Unit  Stp StationID  StationDesc (header card 1)" )
                      .append( System.lineSeparator() )
                      .append( "$m  yyyy mm   yyyy  col format (header card 2)" )
                      .append( System.lineSeparator() )
                      .append( "$StationID  mmyy day datavalue (values n cards)" )
                      .append( System.lineSeparator() )
                      .append( "ts740.2017090 QINE L3   CFS   6   DRRC2            DOLORES RIVER, CO" )
                      .append( System.lineSeparator() )
                      .append( "06  1985 06   1985  1   F9.1" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   1.0" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   2.0" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   3.0" )
                      .append( System.lineSeparator() )
                      .append( "DRRC2      8506  01   4.0" );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( cardPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.getSource() )
                   .thenReturn( new DataSourceConfig.Source( cardPath.toUri(), null, "UTC", null, null ) );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.DATACARD );

            DatacardReader reader = DatacardReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( InputStream inputStream = new BufferedInputStream( Files.newInputStream( cardPath ) );
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
                                               Feature.of( MessageFactory.getGeometry( "DRRC2",
                                                                                          "DOLORES RIVER, CO",
                                                                                          null,
                                                                                          null ) ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T06_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T12_00_00Z, 2.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T18_00_00Z, 3.0 ) )
                                                        .addEvent( Event.of( T1985_06_02T00_00_00Z, 4.0 ) )
                                                        .build();

                List<TimeSeries<Double>> expected = List.of( expectedOne );

                assertEquals( expected, actual );
            }

            // Clean up
            if ( Files.exists( cardPath ) )
            {
                Files.delete( cardPath );
            }
        }
    }

}
