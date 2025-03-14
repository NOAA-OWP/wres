package wres.writing.csv.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.Pool.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link SingleValuedPairsWriter}.
 *
 * @author James Brown
 */
public final class SingleValuedPairsWriterTest
{
    /** First set of pairs to use for writing. */
    private static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> pairs = null;

    /** Second set of pairs to use for writing. */
    private static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> pairsTwo = null;

    /** Third set of pairs to use for writing. */
    private static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> pairsThree = null;

    private static final String DEFAULT_PAIRS_NAME = "pairs.csv";
    private static final String VARIABLE_NAME = "ARMS";
    private static final Feature FEATURE = Feature.of(
            MessageUtilities.getGeometry( "FRUIT" ) );
    private static final String UNIT = "SCOOBIES";

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0 )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE,
                                      UNIT );
    }

    private static TimeSeriesMetadata getBoilerplateMetadata()
    {
        return TimeSeriesMetadata.of( Collections.emptyMap(),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE,
                                      UNIT );
    }

    @BeforeClass
    public static void setUpBeforeAllTests()
    {

        // Create the pairs
        Builder<TimeSeries<Pair<Double, Double>>> tsBuilder = new Builder<>();

        SortedSet<Event<Pair<Double, Double>>> setOfPairs = new TreeSet<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ),
                                  Pair.of( 1.001, 2.0 ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), Pair.of( 3.0, 4.0 ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), Pair.of( 5.0, 6.0 ) ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "RIFLE" )
                                          .setMeasurementUnit( "SCOOBIES" )
                                          .build();

        GeometryTuple geoTuple =
                MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "PLUM" ),
                                                   MessageUtilities.getGeometry( "PLUM" ),
                                                   null );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        Pool pool = MessageFactory.getPool( featureGroup,
                                            null,
                                            null,
                                            null,
                                            false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        TimeSeriesMetadata boilerplate = SingleValuedPairsWriterTest.getBoilerplateMetadataWithT0( basisTime );
        TimeSeriesMetadata metadata =
                new TimeSeriesMetadata.Builder( boilerplate ).setFeature( Feature.of(
                                                                     MessageUtilities.getGeometry( "PLUM" ) ) )
                                                             .build();

        TimeSeries<Pair<Double, Double>> timeSeriesOne =
                TimeSeries.of( metadata, setOfPairs );

        SingleValuedPairsWriterTest.pairs = tsBuilder.addData( timeSeriesOne )
                                                     .setMetadata( meta )
                                                     .build();

        // Create the second time-series of pairs
        Builder<TimeSeries<Pair<Double, Double>>> tsBuilderTwo = new Builder<>();
        SortedSet<Event<Pair<Double, Double>>> setOfPairsTwo = new TreeSet<>();
        Instant basisTimeTwo = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ),
                                     Pair.of( 7.0, 8.0 ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ),
                                     Pair.of( 9.0, 10.0 ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ),
                                     Pair.of( 11.0, 12.0 ) ) );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( "PISTOL" )
                                             .setMeasurementUnit( "SCOOBIES" )
                                             .build();

        GeometryTuple geoTupleTwo =
                MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "ORANGE" ),
                                                   MessageUtilities.getGeometry( "ORANGE" ),
                                                   null );
        GeometryGroup geoGroupTwo = MessageUtilities.getGeometryGroup( null, geoTupleTwo );
        FeatureGroup featureGroupTwo = FeatureGroup.of( geoGroupTwo );

        Pool poolTwo = MessageFactory.getPool( featureGroupTwo,
                                               null,
                                               null,
                                               null,
                                               false );

        PoolMetadata metaTwo = PoolMetadata.of( evaluationTwo, poolTwo );

        TimeSeriesMetadata boilerplateTwo = getBoilerplateMetadataWithT0( basisTimeTwo );
        TimeSeriesMetadata metadataTwo =
                new TimeSeriesMetadata.Builder( boilerplateTwo ).setFeature( Feature.of(
                                                                        MessageUtilities.getGeometry( "ORANGE" ) ) )
                                                                .build();

        TimeSeries<Pair<Double, Double>> timeSeriesTwo =
                TimeSeries.of( metadataTwo, setOfPairsTwo );

        SingleValuedPairsWriterTest.pairsTwo =
                tsBuilderTwo.addData( timeSeriesTwo ).setMetadata( metaTwo ).build();


        // Create the third time-series of pairs
        Builder<TimeSeries<Pair<Double, Double>>> tsBuilderThree = new Builder<>();
        SortedSet<Event<Pair<Double, Double>>> setOfPairsThree = new TreeSet<>();
        Instant basisTimeThree = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T07:00:00Z" ),
                                       Pair.of( 13.0, 14.0 ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ),
                                       Pair.of( 15.0, 16.0 ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ),
                                       Pair.of( 17.0, 18.0 ) ) );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( "GRENADE" )
                                               .setMeasurementUnit( "SCOOBIES" )
                                               .build();

        GeometryTuple geoTupleThree =
                MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "BANANA" ),
                                                   MessageUtilities.getGeometry( "BANANA" ),
                                                   null );
        GeometryGroup geoGroupThree = MessageUtilities.getGeometryGroup( null, geoTupleThree );
        FeatureGroup featureGroupThree = FeatureGroup.of( geoGroupThree );

        Pool poolThree = MessageFactory.getPool( featureGroupThree,
                                                 null,
                                                 null,
                                                 null,
                                                 false );

        PoolMetadata metaThree = PoolMetadata.of( evaluationThree, poolThree );

        TimeSeriesMetadata boilerplateThree =
                SingleValuedPairsWriterTest.getBoilerplateMetadataWithT0( basisTimeThree );
        TimeSeriesMetadata metadataThree =
                new TimeSeriesMetadata.Builder( boilerplateThree ).setFeature( Feature.of(
                                                                          MessageUtilities.getGeometry( "BANANA" ) ) )
                                                                  .build();
        TimeSeries<Pair<Double, Double>> timeSeriesThree =
                TimeSeries.of( metadataThree, setOfPairsThree );

        SingleValuedPairsWriterTest.pairsThree = tsBuilderThree.addData( timeSeriesThree )
                                                               .setMetadata( metaThree )
                                                               .build();
    }

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes some empty pairs, and checks that the written output matches the
     * expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptWithEmptyPairs() throws IOException
    {
        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( csvPath ) )
            {

                Builder<TimeSeries<Pair<Double, Double>>> tsBuilder = new Builder<>();

                // Set the measurement units and time scale
                GeometryTuple geoTuple =
                        MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry(
                                                                                 "PINEAPPLE" ),
                                                           MessageUtilities.getGeometry(
                                                                                 "PINEAPPLE" ),
                                                           null );
                GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
                FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

                Evaluation evaluation = Evaluation.newBuilder()
                                                  .setRightVariableName( "MORTARS" )
                                                  .setMeasurementUnit( "SCOOBIES" )
                                                  .build();

                Pool pool = MessageFactory.getPool( featureGroup,
                                                    null,
                                                    TimeScaleOuter.of( Duration.ofSeconds( 3600 ),
                                                                       TimeScaleFunction.MEAN ),
                                                    null,
                                                    false );

                PoolMetadata metadata = PoolMetadata.of( evaluation, pool );

                wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> emptyPairs =
                        tsBuilder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                                          Collections.emptySortedSet() ) )
                                 .setMetadata( metadata )
                                 .build();

                // Write the pairs
                writer.accept( emptyPairs );

                // Assert the expected results of nothing
                assertFalse( Files.exists( csvPath ) );
            }
        }
    }

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes some pairs, and checks that the written output matches the
     * expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptForOneSetOfPairs() throws IOException
    {
        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( csvPath ) )
            {

                // Write the pairs
                writer.accept( SingleValuedPairsWriterTest.pairs );

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Assert the expected results
                assertEquals( 4, results.size() );
                assertEquals( "FEATURE NAME,"
                              + "FEATURE GROUP NAME,"
                              + "VARIABLE NAME,"
                              + "REFERENCE TIME,"
                              + "VALID TIME,"
                              + "LEAD DURATION,"
                              + "OBSERVED IN SCOOBIES,PREDICTED IN SCOOBIES",
                              results.get( 0 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T01:00:00Z,PT1H,1.001,2.0", results.get( 1 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T02:00:00Z,PT2H,3.0,4.0", results.get( 2 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T03:00:00Z,PT3H,5.0,6.0", results.get( 3 ) );
            }
        }
    }

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes a pair, and checks the output is gzip.
     * @throws IOException if the writing or removal of the paired file fails
     * @throws TikaException if problem with MimeTypes or parsing XML config
     */
    @Test
    public void testAcceptForOneSetOfPairsGzip() throws IOException, TikaException
    {
        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_ZIP_NAME );

            // Create the writer
            try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( csvPath, null, true ) )
            {
                // Write the pairs with gzip set to true
                writer.accept( SingleValuedPairsWriterTest.pairs );

                Metadata metadata = new Metadata();
                TikaConfig tikaConfig = new TikaConfig();
                Detector detector = tikaConfig.getDetector();

                try ( InputStream inStream = Files.newInputStream( csvPath ) )
                {
                    InputStream bufferedStream = new BufferedInputStream( inStream );
                    MediaType detectedMediaType = detector.detect( bufferedStream, metadata );
                    String subtype = detectedMediaType.getSubtype();
                    assertEquals( "gzip", subtype.toLowerCase() );
                }
            }
        }
    }

    /**
     * Builds a {@link SingleValuedPairsWriter} whose {@link PoolMetadata} includes a {@link TimeWindowOuter},
     * writes some pairs, and checks that the written output matches the expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptForOneSetOfPairsWithTimeWindow() throws IOException
    {
        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( csvPath ) )
            {

                // Create the pairs with a time window
                Builder<TimeSeries<Pair<Double, Double>>> tsBuilder = new Builder<>();
                tsBuilder.addPool( SingleValuedPairsWriterTest.pairs );
                TimeWindow inner =
                        MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                        Instant.parse( "1990-01-01T00:00:00Z" ),
                                                        Duration.ZERO );
                TimeWindowOuter outer = TimeWindowOuter.of( inner );
                tsBuilder.setMetadata( PoolMetadata.of( SingleValuedPairsWriterTest.pairs.getMetadata(),
                                                        outer ) );


                // Write the pairs
                writer.accept( tsBuilder.build() );

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Assert the expected results
                assertEquals( 4, results.size() );
                assertEquals( "FEATURE NAME,"
                              + "FEATURE GROUP NAME,"
                              + "VARIABLE NAME,"
                              + "EARLIEST ISSUE TIME,"
                              + "LATEST ISSUE TIME,"
                              + "EARLIEST VALID TIME,"
                              + "LATEST VALID TIME,"
                              + "EARLIEST LEAD TIME,"
                              + "LATEST LEAD TIME,"
                              + "REFERENCE TIME,"
                              + "VALID TIME,"
                              + "LEAD DURATION,"
                              + "OBSERVED IN SCOOBIES,"
                              + "PREDICTED IN SCOOBIES",
                              results.get( 0 ) );

                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,"
                              + "1990-01-01T00:00:00Z,"
                              + Instant.MIN
                              + ","
                              + Instant.MAX
                              + ",PT0S,PT0S,1985-01-01T00:00:00Z,1985-01-01T01:00:00Z,PT1H,1.001,2.0",
                              results.get( 1 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,"
                              + "1990-01-01T00:00:00Z,"
                              + Instant.MIN
                              + ","
                              + Instant.MAX
                              + ",PT0S,PT0S,1985-01-01T00:00:00Z,1985-01-01T02:00:00Z,PT2H,3.0,4.0",
                              results.get( 2 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,"
                              + "1990-01-01T00:00:00Z,"
                              + Instant.MIN
                              + ","
                              + Instant.MAX
                              + ",PT0S,PT0S,1985-01-01T00:00:00Z,1985-01-01T03:00:00Z,PT3H,5.0,6.0",
                              results.get( 3 ) );
            }
        }
    }

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes two sets of pairs synchronously, and checks that the written 
     * output matches the expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptForTwoSetsOfPairsWrittenSync() throws IOException
    {
        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( csvPath ) )
            {

                // Write the pairs
                writer.accept( SingleValuedPairsWriterTest.pairs );
                writer.accept( SingleValuedPairsWriterTest.pairsTwo );

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Assert the expected results
                assertEquals( 7, results.size() );
                assertEquals( "FEATURE NAME,"
                              + "FEATURE GROUP NAME,"
                              + "VARIABLE NAME,"
                              + "REFERENCE TIME,"
                              + "VALID TIME,"
                              + "LEAD DURATION,"
                              + "OBSERVED IN SCOOBIES,"
                              + "PREDICTED IN SCOOBIES",
                              results.get( 0 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T01:00:00Z,PT1H,1.001,2.0", results.get( 1 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T02:00:00Z,PT2H,3.0,4.0", results.get( 2 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T03:00:00Z,PT3H,5.0,6.0", results.get( 3 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T04:00:00Z,PT4H,7.0,8.0", results.get( 4 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T05:00:00Z,PT5H,9.0,10.0",
                              results.get( 5 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T06:00:00Z,PT6H,11.0,12.0",
                              results.get( 6 ) );
            }
        }
    }

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes three sets of pairs asynchronously, and checks that the written 
     * output matches the expected output.
     * @throws IOException if the writing or removal of the paired file fails
     * @throws ExecutionException if the asynchronous execution fails
     * @throws InterruptedException the the execution is interrupted 
     */

    @Test
    public void testAcceptForThreeSetsOfPairsWrittenAsync() throws IOException, InterruptedException, ExecutionException
    {
        // Create the writer with a decimal format
        DecimalFormat formatter = new DecimalFormat();
        formatter.applyPattern( "0.0" );
        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( SingleValuedPairsWriter writer =
                          SingleValuedPairsWriter.of( csvPath, formatter ) )
            {

                // Write the pairs async on the common FJP
                CompletableFuture.allOf( CompletableFuture.runAsync( () -> writer.accept( SingleValuedPairsWriterTest.pairs ) ),
                                         CompletableFuture.runAsync( () -> writer.accept( SingleValuedPairsWriterTest.pairsTwo ) ),
                                         CompletableFuture.runAsync( () -> writer.accept( SingleValuedPairsWriterTest.pairsThree ) ) )
                                 .get();

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Sort the results
                results.sort( Comparator.naturalOrder() );

                // Assert the expected results
                assertEquals( 10, results.size() );
                assertEquals( "BANANA,,ARMS,1985-01-01T00:00:00Z,1985-01-01T07:00:00Z,PT7H,13.0,14.0",
                              results.get( 0 ) );
                assertEquals( "BANANA,,ARMS,1985-01-01T00:00:00Z,1985-01-01T08:00:00Z,PT8H,15.0,16.0",
                              results.get( 1 ) );
                assertEquals( "BANANA,,ARMS,1985-01-01T00:00:00Z,1985-01-01T09:00:00Z,PT9H,17.0,18.0",
                              results.get( 2 ) );
                assertEquals( "FEATURE NAME,"
                              + "FEATURE GROUP NAME,"
                              + "VARIABLE NAME,"
                              + "REFERENCE TIME,"
                              + "VALID TIME,"
                              + "LEAD DURATION,"
                              + "OBSERVED IN SCOOBIES,"
                              + "PREDICTED IN SCOOBIES",
                              results.get( 3 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T04:00:00Z,PT4H,7.0,8.0", results.get( 4 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T05:00:00Z,PT5H,9.0,10.0",
                              results.get( 5 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T06:00:00Z,PT6H,11.0,12.0",
                              results.get( 6 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T01:00:00Z,PT1H,1.0,2.0", results.get( 7 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T02:00:00Z,PT2H,3.0,4.0", results.get( 8 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T03:00:00Z,PT3H,5.0,6.0", results.get( 9 ) );
            }
        }
    }

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes some pairs, and checks that the 
     * {@link SingleValuedPairsWriter#get()} returns the correct path written.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testSuppliedPath() throws IOException
    {
        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( csvPath ) )
            {

                // Write the pairs
                writer.accept( SingleValuedPairsWriterTest.pairs );

                // Assert the expected results
                assertEquals( writer.get(), Set.of( csvPath ) );
            }
        }
    }

}
