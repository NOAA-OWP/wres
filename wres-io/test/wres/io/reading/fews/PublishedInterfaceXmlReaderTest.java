package wres.io.reading.fews;

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
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link PublishedInterfaceXmlReader}.
 * @author James Brown
 */

class PublishedInterfaceXmlReaderTest
{
    private static final String TEST_CSV = "test.csv";
    private static final String TEST = "test";
    private static final Geometry DRRC2 = MessageFactory.getGeometry( "DRRC2",
                                                                      "DOLORES, CO",
                                                                      null,
                                                                      "POINT ( 108.5045 37.4739 )" );
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

    // An observation-like pi-xml string with two time-series
    private static final String PI_STRING_ONE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
                                                "<TimeSeries xmlns=\"http://www.wldelft.nl/fews/PI\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd\" version=\"1.2\">\r\n"
                                                + "    <timeZone>0.0</timeZone>\r\n"
                                                + "    <series>\r\n"
                                                + "        <header>\r\n"
                                                + "            <type>instantaneous</type>\r\n"
                                                + "            <locationId>DRRC2</locationId>\r\n"
                                                + "            <parameterId>QINE</parameterId>\r\n"
                                                + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                + "            <endDate date=\"1985-06-01\" time=\"15:00:00\"/>\r\n"
                                                + "            <missVal>-999.0</missVal>\r\n"
                                                + "            <stationName>DOLORES, CO</stationName>\r\n"
                                                + "            <lat>37.4739</lat>\r\n"
                                                + "            <lon>108.5045</lon>\r\n"
                                                + "            <units>CFS</units>\r\n"
                                                + "        </header>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"13:00:00\" value=\"1\" flag=\"0\"/>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"14:00:00\" value=\"2\" flag=\"0\"/>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"15:00:00\" value=\"3\" flag=\"0\"/>\r\n"
                                                + "    </series>\r\n"
                                                + "    <series>\r\n"
                                                + "        <header>\r\n"
                                                + "            <type>instantaneous</type>\r\n"
                                                + "            <locationId>DRRC2</locationId>\r\n"
                                                + "            <parameterId>QINE</parameterId>\r\n"
                                                + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                + "            <endDate date=\"1985-06-01\" time=\"15:00:00\"/>\r\n"
                                                + "            <missVal>-999.0</missVal>\r\n"
                                                + "            <stationName>DOLORES, CO</stationName>\r\n"
                                                + "            <lat>37.4739</lat>\r\n"
                                                + "            <lon>108.5045</lon>\r\n"
                                                + "            <units>CFS</units>\r\n"
                                                + "        </header>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"13:00:00\" value=\"4\" flag=\"0\"/>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"14:00:00\" value=\"5\" flag=\"0\"/>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"15:00:00\" value=\"6\" flag=\"0\"/>\r\n"
                                                + "    </series>\r\n"
                                                + "</TimeSeries>";

    // A forecast-like pi-xml string with three single-valued forecasts
    private static final String PI_STRING_TWO = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
                                                "<TimeSeries xmlns=\"http://www.wldelft.nl/fews/PI\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd\" version=\"1.2\">\r\n"
                                                + "    <timeZone>0.0</timeZone>\r\n"
                                                + "    <series>\r\n"
                                                + "        <header>\r\n"
                                                + "            <type>instantaneous</type>\r\n"
                                                + "            <locationId>DRRC2</locationId>\r\n"
                                                + "            <parameterId>QINE</parameterId>\r\n"
                                                + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                + "            <endDate date=\"1985-06-01\" time=\"14:00:00\"/>\r\n"
                                                + "            <forecastDate date=\"1985-06-01\" time=\"12:00:00\"/>\r\n"
                                                + "            <missVal>-999.0</missVal>\r\n"
                                                + "            <stationName>DOLORES, CO</stationName>\r\n"
                                                + "            <lat>37.4739</lat>\r\n"
                                                + "            <lon>108.5045</lon>\r\n"
                                                + "            <units>CFS</units>\r\n"
                                                + "        </header>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"13:00:00\" value=\"1\" flag=\"0\"/>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"14:00:00\" value=\"2\" flag=\"0\"/>\r\n"
                                                + "    </series>\r\n"
                                                + "    <series>\r\n"
                                                + "        <header>\r\n"
                                                + "            <type>instantaneous</type>\r\n"
                                                + "            <locationId>DRRC2</locationId>\r\n"
                                                + "            <parameterId>QINE</parameterId>\r\n"
                                                + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                + "            <endDate date=\"1985-06-01\" time=\"14:00:00\"/>\r\n"
                                                + "            <forecastDate date=\"1985-06-02\" time=\"12:00:00\"/>\r\n"
                                                + "            <missVal>-999.0</missVal>\r\n"
                                                + "            <stationName>DOLORES, CO</stationName>\r\n"
                                                + "            <lat>37.4739</lat>\r\n"
                                                + "            <lon>108.5045</lon>\r\n"
                                                + "            <units>CFS</units>\r\n"
                                                + "        </header>\r\n"
                                                + "        <event date=\"1985-06-02\" time=\"13:00:00\" value=\"3\" flag=\"0\"/>\r\n"
                                                + "        <event date=\"1985-06-02\" time=\"14:00:00\" value=\"4\" flag=\"0\"/>\r\n"
                                                + "    </series>\r\n"
                                                + "    <series>\r\n"
                                                + "        <header>\r\n"
                                                + "            <type>instantaneous</type>\r\n"
                                                + "            <locationId>DRRC3</locationId>\r\n"
                                                + "            <parameterId>QINE</parameterId>\r\n"
                                                + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                + "            <endDate date=\"1985-06-01\" time=\"14:00:00\"/>\r\n"
                                                + "            <forecastDate date=\"1985-06-01\" time=\"12:00:00\"/>\r\n"
                                                + "            <missVal>-999.0</missVal>\r\n"
                                                + "            <units>CFS</units>\r\n"
                                                + "        </header>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"13:00:00\" value=\"5\" flag=\"0\"/>\r\n"
                                                + "        <event date=\"1985-06-01\" time=\"14:00:00\" value=\"6\" flag=\"0\"/>\r\n"
                                                + "    </series>\r\n"
                                                + "</TimeSeries>";

    // An ensemble-forecast-like pi-xml string with one forecast that contains three ensemble members
    private static final String PI_STRING_THREE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
                                                  "<TimeSeries xmlns=\"http://www.wldelft.nl/fews/PI\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd\" version=\"1.2\">\r\n"
                                                  + "    <timeZone>0.0</timeZone>\r\n"
                                                  + "    <series>\r\n"
                                                  + "        <header>\r\n"
                                                  + "            <type>instantaneous</type>\r\n"
                                                  + "            <locationId>DRRC2</locationId>\r\n"
                                                  + "            <parameterId>QINE</parameterId>\r\n"
                                                  + "            <ensembleMemberIndex>1961</ensembleMemberIndex>\r\n"
                                                  + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                  + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                  + "            <endDate date=\"1985-06-01\" time=\"14:00:00\"/>\r\n"
                                                  + "            <forecastDate date=\"1985-06-01\" time=\"12:00:00\"/>\r\n"
                                                  + "            <missVal>-999.0</missVal>\r\n"
                                                  + "            <stationName>DOLORES, CO</stationName>\r\n"
                                                  + "            <lat>37.4739</lat>\r\n"
                                                  + "            <lon>108.5045</lon>\r\n"
                                                  + "            <units>CFS</units>\r\n"
                                                  + "        </header>\r\n"
                                                  + "        <event date=\"1985-06-01\" time=\"13:00:00\" value=\"1\" flag=\"0\"/>\r\n"
                                                  + "        <event date=\"1985-06-01\" time=\"14:00:00\" value=\"2\" flag=\"0\"/>\r\n"
                                                  + "    </series>\r\n"
                                                  + "    <series>\r\n"
                                                  + "        <header>\r\n"
                                                  + "            <type>instantaneous</type>\r\n"
                                                  + "            <locationId>DRRC2</locationId>\r\n"
                                                  + "            <parameterId>QINE</parameterId>\r\n"
                                                  + "            <ensembleMemberIndex>1962</ensembleMemberIndex>\r\n"
                                                  + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                  + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                  + "            <endDate date=\"1985-06-01\" time=\"14:00:00\"/>\r\n"
                                                  + "            <forecastDate date=\"1985-06-01\" time=\"12:00:00\"/>\r\n"
                                                  + "            <missVal>-999.0</missVal>\r\n"
                                                  + "            <stationName>DOLORES, CO</stationName>\r\n"
                                                  + "            <lat>37.4739</lat>\r\n"
                                                  + "            <lon>108.5045</lon>\r\n"
                                                  + "            <units>CFS</units>\r\n"
                                                  + "        </header>\r\n"
                                                  + "        <event date=\"1985-06-01\" time=\"13:00:00\" value=\"3\" flag=\"0\"/>\r\n"
                                                  + "        <event date=\"1985-06-01\" time=\"14:00:00\" value=\"4\" flag=\"0\"/>\r\n"
                                                  + "    </series>\r\n"
                                                  + "    <series>\r\n"
                                                  + "        <header>\r\n"
                                                  + "            <type>instantaneous</type>\r\n"
                                                  + "            <locationId>DRRC2</locationId>\r\n"
                                                  + "            <parameterId>QINE</parameterId>\r\n"
                                                  + "            <ensembleMemberIndex>1963</ensembleMemberIndex>\r\n"
                                                  + "            <timeStep unit=\"second\" multiplier=\"3600\"/>\r\n"
                                                  + "            <startDate date=\"1985-06-01\" time=\"13:00:00\"/>\r\n"
                                                  + "            <endDate date=\"1985-06-01\" time=\"14:00:00\"/>\r\n"
                                                  + "            <forecastDate date=\"1985-06-01\" time=\"12:00:00\"/>\r\n"
                                                  + "            <missVal>-999.0</missVal>\r\n"
                                                  + "            <stationName>DOLORES, CO</stationName>\r\n"
                                                  + "            <lat>37.4739</lat>\r\n"
                                                  + "            <lon>108.5045</lon>\r\n"
                                                  + "            <units>CFS</units>\r\n"
                                                  + "        </header>\r\n"
                                                  + "        <event date=\"1985-06-01\" time=\"13:00:00\" value=\"5\" flag=\"0\"/>\r\n"
                                                  + "        <event date=\"1985-06-01\" time=\"14:00:00\" value=\"6\" flag=\"0\"/>\r\n"
                                                  + "    </series>\r\n"
                                                  + "</TimeSeries>";

    @Test
    void testReadObservationsResultsInTwoTimeSeries() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new pi-xml file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( PI_STRING_ONE );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.hasSourcePath() )
                   .thenReturn( true );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.XML_PI_TIMESERIES );

            PublishedInterfaceXmlReader reader = PublishedInterfaceXmlReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .collect( Collectors.toList() );

                TimeSeriesMetadata expectedMetadata =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               TimeScaleOuter.of(),
                                               QINE,
                                               Feature.of( DRRC2 ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 3.0 ) )
                                                        .build();

                TimeSeries<Double> expectedTwo =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
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
    void testReadObservationsFromInputStreamResultsInTwoTimeSeries() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new pi-xml file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( PI_STRING_ONE );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.hasSourcePath() )
                   .thenReturn( true );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.XML_PI_TIMESERIES );

            PublishedInterfaceXmlReader reader = PublishedInterfaceXmlReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( InputStream inputStream = new BufferedInputStream( Files.newInputStream( csvPath ) );
                  Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource, inputStream ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .collect( Collectors.toList() );

                TimeSeriesMetadata expectedMetadata =
                        TimeSeriesMetadata.of( Collections.emptyMap(),
                                               TimeScaleOuter.of(),
                                               QINE,
                                               Feature.of( DRRC2 ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 3.0 ) )
                                                        .build();

                TimeSeries<Double> expectedTwo =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
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
            // Write a new pi-xml file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_CSV );
            Path csvPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( csvPath ) )
            {
                writer.append( PI_STRING_TWO );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.hasSourcePath() )
                   .thenReturn( true );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.XML_PI_TIMESERIES );

            PublishedInterfaceXmlReader reader = PublishedInterfaceXmlReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                             .collect( Collectors.toList() );

                TimeSeriesMetadata expectedMetadataOne =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, T1985_06_01T12_00_00Z ),
                                               TimeScaleOuter.of(),
                                               QINE,
                                               Feature.of( DRRC2 ),
                                               CFS );

                TimeSeries<Double> expectedOne =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataTwo =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, T1985_06_02T12_00_00Z ),
                                               TimeScaleOuter.of(),
                                               QINE,
                                               Feature.of( DRRC2 ),
                                               CFS );

                TimeSeries<Double> expectedTwo =
                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataTwo )
                                                        .addEvent( Event.of( T1985_06_02T13_00_00Z, 3.0 ) )
                                                        .addEvent( Event.of( T1985_06_02T14_00_00Z, 4.0 ) )
                                                        .build();

                TimeSeriesMetadata expectedMetadataThree =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, T1985_06_01T12_00_00Z ),
                                               TimeScaleOuter.of(),
                                               QINE,
                                               Feature.of( MessageFactory.getGeometry( DRRC3 ) ),
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
    void testReadEnsembleForecastsResultsInOneTimeSeries() throws IOException
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
                writer.append( PI_STRING_THREE );
            }

            DataSource dataSource = Mockito.mock( DataSource.class );
            Mockito.when( dataSource.getUri() )
                   .thenReturn( csvPath.toUri() );
            Mockito.when( dataSource.getVariable() )
                   .thenReturn( new Variable( QINE, null ) );
            Mockito.when( dataSource.hasSourcePath() )
                   .thenReturn( true );
            Mockito.when( dataSource.getDisposition() )
                   .thenReturn( DataDisposition.XML_PI_TIMESERIES );

            PublishedInterfaceXmlReader reader = PublishedInterfaceXmlReader.of();

            // No reading yet, we are just opening a pipe to the file here
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( dataSource ) )
            {
                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series 
                // creates as many reads from the file system as necessary to read that time-series into memory
                List<TimeSeries<Ensemble>> actual = tupleStream.map( TimeSeriesTuple::getEnsembleTimeSeries )
                                                               .collect( Collectors.toList() );

                TimeSeriesMetadata expectedMetadata =
                        TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, T1985_06_01T12_00_00Z ),
                                               TimeScaleOuter.of(),
                                               QINE,
                                               Feature.of( DRRC2 ),
                                               CFS );

                Ensemble.Labels labels = Ensemble.Labels.of( "1961", "1962", "1963" );

                TimeSeries<Ensemble> expectedSeries =
                        new TimeSeries.Builder<Ensemble>().setMetadata( expectedMetadata )
                                                          .addEvent( Event.of( T1985_06_01T13_00_00Z,
                                                                               Ensemble.of( new double[] { 1.0, 3.0,
                                                                                                           5.0 },
                                                                                            labels ) ) )
                                                          .addEvent( Event.of( T1985_06_01T14_00_00Z,
                                                                               Ensemble.of( new double[] { 2.0, 4.0,
                                                                                                           6.0 },
                                                                                            labels ) ) )
                                                          .build();

                List<TimeSeries<Ensemble>> expected = List.of( expectedSeries );

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
