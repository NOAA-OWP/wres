//package wres.io.reading;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//
//import java.io.BufferedOutputStream;
//import java.io.IOException;
//import java.nio.file.FileSystem;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.time.Instant;
//import java.util.Collections;
//import java.util.List;
//import java.util.StringJoiner;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import java.util.zip.GZIPOutputStream;
//
//import org.junit.jupiter.api.Test;
//
//import com.google.common.jimfs.Configuration;
//import com.google.common.jimfs.Jimfs;
//
//import wres.config.generated.DataSourceConfig;
//import wres.config.generated.LeftOrRightOrBaseline;
//import wres.config.generated.DataSourceConfig.Variable;
//import wres.datamodel.space.Feature;
//import wres.datamodel.time.Event;
//import wres.datamodel.time.TimeSeries;
//import wres.datamodel.time.TimeSeriesMetadata;
//import wres.io.reading.DataSource.DataDisposition;
//import wres.statistics.MessageFactory;
//
///**
// * Tests the {@link ZippedReader}.
// * @author James Brown
// */
//
//class ZippedReaderTest
//{
//    private static final String TEST = "test";
//    private static final String DRRC2 = "DRRC2";
//    private static final String CFS = "CFS";
//    private static final String QINE = "QINE";
//    private static final Instant T1985_06_01T15_00_00Z = Instant.parse( "1985-06-01T15:00:00Z" );
//    private static final Instant T1985_06_01T14_00_00Z = Instant.parse( "1985-06-01T14:00:00Z" );
//    private static final Instant T1985_06_01T13_00_00Z = Instant.parse( "1985-06-01T13:00:00Z" );
//
//    @Test
//    void testReadObservationsFromCsvInsideZippedSourceResultsInOneTimeSeries() throws IOException
//    {
//        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
//        {
//            // Write a new zipped file
//            Path directory = fileSystem.getPath( TEST );
//            Files.createDirectory( directory );
//            Path pathToStore = fileSystem.getPath( TEST, "test.csv.gz" );
//            Path zipPath = Files.createFile( pathToStore );
//
//            // Create the CSV content
//            StringJoiner joiner = new StringJoiner( System.lineSeparator() );
//            joiner.add( "value_date,variable_name,location,measurement_unit,value" )
//                  .add( "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,1" )
//                  .add( "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,2" )
//                  .add( "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,3" );
//
//            byte[] csvContent = joiner.toString()
//                                      .getBytes();
//
//            // Write the content to each of two archive entries
//            try ( GZIPOutputStream out =
//                    new GZIPOutputStream( new BufferedOutputStream( Files.newOutputStream( zipPath ) ) ) )
//            {
//                // Create two entries, the first one a PI-XML file, the second a CSV file
//                out.write( csvContent );
//            }
//
//            DataSourceConfig.Source fakeDeclarationSource =
//                    new DataSourceConfig.Source( null,
//                                                 null,
//                                                 null,
//                                                 null,
//                                                 null );
//
//            DataSource fakeSource = DataSource.of( DataDisposition.GZIP,
//                                                   fakeDeclarationSource,
//                                                   new DataSourceConfig( null,
//                                                                         List.of( fakeDeclarationSource ),
//                                                                         new Variable( QINE, null ),
//                                                                         null,
//                                                                         null,
//                                                                         null,
//                                                                         null,
//                                                                         null,
//                                                                         null,
//                                                                         null,
//                                                                         null ),
//                                                   Collections.emptyList(),
//                                                   zipPath.toUri(),
//                                                   LeftOrRightOrBaseline.LEFT );
//
//            // Reader for formats within the archive
//            TimeSeriesReaderFactory readerFactory = TimeSeriesReaderFactory.of( null, null, null );
//
//            ZippedReader reader = ZippedReader.of( readerFactory );
//
//            // No reading yet, we are just opening a pipe to the file here
//            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
//            {
//                // Now we trigger reading because there is a terminal stream operation. Each pull on a time-series
//                // creates as many reads from the file system as necessary to read that time-series into memory
//                List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
//                                                             .collect( Collectors.toList() );
//
//                TimeSeriesMetadata expectedMetadataOne =
//                        TimeSeriesMetadata.of( Collections.emptyMap(),
//                                               null,
//                                               QINE,
//                                               Feature.of( MessageFactory.getGeometry( DRRC2 ) ),
//                                               CFS );
//
//                TimeSeries<Double> expectedOne =
//                        new TimeSeries.Builder<Double>().setMetadata( expectedMetadataOne )
//                                                        .addEvent( Event.of( T1985_06_01T13_00_00Z, 1.0 ) )
//                                                        .addEvent( Event.of( T1985_06_01T14_00_00Z, 2.0 ) )
//                                                        .addEvent( Event.of( T1985_06_01T15_00_00Z, 3.0 ) )
//                                                        .build();
//
//                // Two time-series across two separate archive entries
//                List<TimeSeries<Double>> expected = List.of( expectedOne );
//
//                assertEquals( expected, actual );
//            }
//
//            // Clean up
//            if ( Files.exists( zipPath ) )
//            {
//                Files.delete( zipPath );
//            }
//        }
//    }
//}
