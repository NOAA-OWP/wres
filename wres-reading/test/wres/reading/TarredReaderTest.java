package wres.reading;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource.DataDisposition;
import wres.statistics.MessageUtilities;
import wres.system.SystemSettings;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

/**
 * Tests the {@link TarredReader}.
 * @author James Brown
 */

class TarredReaderTest
{
    private static final String TEST = "test";
    private static final String DRRC2 = "DRRC2";
    private static final String DRRC3 = "DRRC3";
    private static final String CFS = "CFS";
    private static final String QINE = "QINE";
    private static final Instant T1985_06_01T15_00_00Z = Instant.parse( "1985-06-01T15:00:00Z" );
    private static final Instant T1985_06_01T14_00_00Z = Instant.parse( "1985-06-01T14:00:00Z" );
    private static final Instant T1985_06_01T13_00_00Z = Instant.parse( "1985-06-01T13:00:00Z" );

    private static final String PI_STRING_ONE = """
            <?xml version="1.0" encoding="UTF-8"?>\r
            <TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.2">\r
                <timeZone>0.0</timeZone>\r
                <series>\r
                    <header>\r
                        <type>instantaneous</type>\r
                        <locationId>DRRC2</locationId>\r
                        <parameterId>QINE</parameterId>\r
                        <timeStep unit="second" multiplier="3600"/>\r
                        <startDate date="1985-06-01" time="13:00:00"/>\r
                        <endDate date="1985-06-01" time="15:00:00"/>\r
                        <missVal>-999.0</missVal>\r
                        <stationName>DOLORES, CO</stationName>\r
                        <lat>37.4739</lat>\r
                        <lon>108.5045</lon>\r
                        <units>CFS</units>\r
                    </header>\r
                    <event date="1985-06-01" time="13:00:00" value="1" flag="0"/>\r
                    <event date="1985-06-01" time="14:00:00" value="2" flag="0"/>\r
                    <event date="1985-06-01" time="15:00:00" value="3" flag="0"/>\r
                </series>\r
            </TimeSeries>""";

    @Test
    void testReadObservationsFromTwoArchiveEntriesWithTwoDifferentFormatsResultsInThreeTimeSeries() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new tarred file containing one CSV file and one PI-XML file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, "test.tar" );
            Path tarPath = Files.createFile( pathToStore );

            // Create the CSV content for the first archive entry
            StringJoiner joiner = new StringJoiner( System.lineSeparator() );
            joiner.add( "value_date,variable_name,location,measurement_unit,value" )
                  .add( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1" )
                  .add( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2" )
                  .add( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3" )
                  .add( "1985-06-01T13:00:00Z,QINE,DRRC3,CFS,4" )
                  .add( "1985-06-01T14:00:00Z,QINE,DRRC3,CFS,5" )
                  .add( "1985-06-01T15:00:00Z,QINE,DRRC3,CFS,6" );

            byte[] csvContent = joiner.toString()
                                      .getBytes();

            // Create the PI-XML content for the second archive entry
            byte[] pixmlContent = PI_STRING_ONE.getBytes();

            // Write the content to each of two archive entries
            try ( TarArchiveOutputStream out =
                          new TarArchiveOutputStream( new BufferedOutputStream( Files.newOutputStream( tarPath ) ) ) )
            {
                // Create two entries, the first one a PI-XML file, the second a CSV file
                TarArchiveEntry archiveEntryOne = new TarArchiveEntry( "test/one.xml" );
                archiveEntryOne.setSize( pixmlContent.length );
                out.putArchiveEntry( archiveEntryOne );
                out.write( pixmlContent );
                out.closeArchiveEntry();

                TarArchiveEntry archiveEntryTwo = new TarArchiveEntry( "test/two.csv" );
                archiveEntryTwo.setSize( csvContent.length );
                out.putArchiveEntry( archiveEntryTwo );
                out.write( csvContent );
                out.closeArchiveEntry();
            }

            Source fakeDeclarationSource = SourceBuilder.builder()
                                                        .uri( tarPath.toUri() )
                                                        .build();

            Dataset dataset = DatasetBuilder.builder()
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .variable( VariableBuilder.builder()
                                                                      .name( QINE )
                                                                      .build() )
                                            .build();

            DataSource fakeSource = DataSource.of( DataDisposition.TARBALL,
                                                   fakeDeclarationSource,
                                                   dataset,
                                                   Collections.emptyList(),
                                                   tarPath.toUri(),
                                                   DatasetOrientation.RIGHT,
                                                   null );

            SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
            Mockito.when( systemSettings.getMaximumArchiveThreads() )
                   .thenReturn( 5 );
            Mockito.when( systemSettings.getPoolObjectLifespan() )
                   .thenReturn( 30_000 );

            // Generator of internal format readers
            TimeSeriesReaderFactory readerFactory = TimeSeriesReaderFactory.of( null, systemSettings, null );

            TarredReader reader = TarredReader.of( readerFactory, systemSettings );

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .collect( Collectors.toList() );

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

                Feature feature = Feature.of( MessageUtilities.getGeometry( DRRC2,
                                                                            "DOLORES, CO",
                                                                            0,
                                                                            "POINT ( 108.5045 37.4739 )" ) );
                TimeSeriesMetadata expectedMetadataThree =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               TimeScaleOuter.of(),
                                               QINE,
                                               feature,
                                               CFS );

                TimeSeries<Double> expectedThree =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataThree )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 3.0 ) )
                                                        .build();

                // Two time-series across two separate archive entries
                List<TimeSeries<Double>> expected = List.of( expectedThree, expectedOne, expectedTwo );

                assertEquals( expected, actual );
            }

            // Clean up
            if ( Files.exists( tarPath ) )
            {
                Files.delete( tarPath );
            }
        }
    }

}