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
import java.util.ArrayList;
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

import wres.datamodel.types.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.Pool.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link EnsemblePairsWriter}.
 *
 * @author James Brown
 */
public final class EnsemblePairsWriterTest
{
    private static final String DEFAULT_PAIRS_NAME = "pairs.csv";
    private static final String VARIABLE_NAME = "ARMS";
    private static final Feature FEATURE = Feature.of(
            wres.statistics.MessageFactory.getGeometry( "FRUIT" ) );
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


    /**
     * First set of pairs to use for writing.
     */

    private static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> pairs = null;

    /**
     * Second set of pairs to use for writing.
     */

    private static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> pairsTwo = null;

    /**
     * Third set of pairs to use for writing.
     */

    private static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> pairsThree = null;

    @BeforeClass
    public static void setUpBeforeAllTests()
    {

        // Create the pairs
        Builder<TimeSeries<Pair<Double, Ensemble>>> tsBuilder = new Builder<>();

        SortedSet<Event<Pair<Double, Ensemble>>> setOfPairs = new TreeSet<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ),
                                  Pair.of( 1.001, Ensemble.of( 2, 3, 4 ) ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ),
                                  Pair.of( 5.0, Ensemble.of( 6, 7, 8 ) ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ),
                                  Pair.of( 9.0, Ensemble.of( 10, 11, 12 ) ) ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "RIFLE" )
                                          .setMeasurementUnit( "SCOOBIES" )
                                          .build();

        GeometryTuple geoTuple =
                wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "PLUM" ),
                                                                 wres.statistics.MessageFactory.getGeometry( "PLUM" ),
                                                                 null );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        Pool pool = MessageFactory.getPool( featureGroup,
                                            null,
                                            null,
                                            null,
                                            false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );
        TimeSeriesMetadata boilerplate = EnsemblePairsWriterTest.getBoilerplateMetadataWithT0( basisTime );
        TimeSeriesMetadata metadata =
                new TimeSeriesMetadata.Builder( boilerplate ).setFeature( Feature.of(
                                                                     wres.statistics.MessageFactory.getGeometry( "PLUM" ) ) )
                                                             .build();
        TimeSeries<Pair<Double, Ensemble>> timeSeriesOne =
                TimeSeries.of( metadata, setOfPairs );

        EnsemblePairsWriterTest.pairs = tsBuilder.addData( timeSeriesOne ).setMetadata( meta ).build();

        // Create the second time-series of pairs
        Builder<TimeSeries<Pair<Double, Ensemble>>> tsBuilderTwo = new Builder<>();
        SortedSet<Event<Pair<Double, Ensemble>>> setOfPairsTwo = new TreeSet<>();
        Instant basisTimeTwo = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ),
                                     Pair.of( 13.0, Ensemble.of( 14, 15, 16 ) ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ),
                                     Pair.of( 17.0, Ensemble.of( 18, 19, 20 ) ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ),
                                     Pair.of( 21.0, Ensemble.of( 22, 23, 24 ) ) ) );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( "PISTOL" )
                                             .setMeasurementUnit( "SCOOBIES" )
                                             .build();

        GeometryTuple geoTupleTwo =
                wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "ORANGE" ),
                                                                 wres.statistics.MessageFactory.getGeometry( "ORANGE" ),
                                                                 null );
        GeometryGroup geoGroupTwo = wres.statistics.MessageFactory.getGeometryGroup( null, geoTupleTwo );
        FeatureGroup featureGroupTwo = FeatureGroup.of( geoGroupTwo );

        Pool poolTwo = MessageFactory.getPool( featureGroupTwo,
                                               null,
                                               null,
                                               null,
                                               false );

        PoolMetadata metaTwo = PoolMetadata.of( evaluationTwo, poolTwo );
        TimeSeriesMetadata boilerplateTwo = EnsemblePairsWriterTest.getBoilerplateMetadataWithT0( basisTimeTwo );
        TimeSeriesMetadata metadataTwo =
                new TimeSeriesMetadata.Builder( boilerplateTwo ).setFeature( Feature.of(
                                                                        wres.statistics.MessageFactory.getGeometry( "ORANGE" ) ) )
                                                                .build();
        TimeSeries<Pair<Double, Ensemble>> timeSeriesTwo =
                TimeSeries.of( metadataTwo, setOfPairsTwo );

        EnsemblePairsWriterTest.pairsTwo = tsBuilderTwo.addData( timeSeriesTwo )
                                                       .setMetadata( metaTwo )
                                                       .build();

        // Create the third time-series of pairs
        Builder<TimeSeries<Pair<Double, Ensemble>>> tsBuilderThree = new Builder<>();
        SortedSet<Event<Pair<Double, Ensemble>>> setOfPairsThree = new TreeSet<>();
        Instant basisTimeThree = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T07:00:00Z" ),
                                       Pair.of( 25.0, Ensemble.of( 26, 27, 28 ) ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ),
                                       Pair.of( 29.0, Ensemble.of( 30, 31, 32 ) ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ),
                                       Pair.of( 33.0, Ensemble.of( 34, 35, 36 ) ) ) );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( "GRENADE" )
                                               .setMeasurementUnit( "SCOOBIES" )
                                               .build();

        GeometryTuple geoTupleThree =
                wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "BANANA" ),
                                                                 wres.statistics.MessageFactory.getGeometry( "BANANA" ),
                                                                 null );
        GeometryGroup geoGroupThree = wres.statistics.MessageFactory.getGeometryGroup( null, geoTupleThree );
        FeatureGroup featureGroupThree = FeatureGroup.of( geoGroupThree );

        Pool poolThree = MessageFactory.getPool( featureGroupThree,
                                                 null,
                                                 null,
                                                 null,
                                                 false );

        PoolMetadata metaThree = PoolMetadata.of( evaluationThree, poolThree );

        TimeSeriesMetadata boilerplateThree = EnsemblePairsWriterTest.getBoilerplateMetadataWithT0( basisTimeThree );
        TimeSeriesMetadata metadataThree =
                new TimeSeriesMetadata.Builder( boilerplateThree ).setFeature( Feature.of(
                                                                          wres.statistics.MessageFactory.getGeometry( "BANANA" ) ) )
                                                                  .build();

        TimeSeries<Pair<Double, Ensemble>> timeSeriesThree =
                TimeSeries.of( metadataThree, setOfPairsThree );

        EnsemblePairsWriterTest.pairsThree = tsBuilderThree.addData( timeSeriesThree )
                                                           .setMetadata( metaThree )
                                                           .build();


    }

    /**
     * Builds a {@link EnsemblePairsWriter} and attempts to write some empty pairs, which should not be written.
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
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath ) )
            {

                Builder<TimeSeries<Pair<Double, Ensemble>>> tsBuilder = new Builder<>();

                // Set the measurement units and time scale
                GeometryTuple geoTuple =
                        wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry(
                                                                                 "PINEAPPLE" ),
                                                                         wres.statistics.MessageFactory.getGeometry(
                                                                                 "PINEAPPLE" ),
                                                                         null );
                GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
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

                PoolMetadata meta = PoolMetadata.of( evaluation, pool );

                TimeSeries<Pair<Double, Ensemble>> timeSeriesOne = TimeSeries.of( getBoilerplateMetadata(),
                                                                                  Collections.emptySortedSet() );

                wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> emptyPairs =
                        tsBuilder.addData( timeSeriesOne ).setMetadata( meta ).build();

                // Write the pairs
                writer.accept( emptyPairs );

                // Assert the expected results of nothing
                assertFalse( Files.exists( csvPath ) );
            }
        }
    }

    /**
     * Builds a {@link EnsemblePairsWriter}, writes some pairs with {@link Double#NaN} values, and checks that the 
     * written output matches the
     * expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptWithNaNPairs() throws IOException
    {
        // Formatter
        DecimalFormat formatter = new DecimalFormat();
        formatter.applyPattern( "0.0" );

        // Create the file system
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path csvPath = fileSystem.getPath( "test", DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, formatter ) )
            {
                // Prime the writer with the expected ensemble structure
                writer.prime( new TreeSet<>( Set.of( "1" ) ) );

                Builder<TimeSeries<Pair<Double, Ensemble>>> tsBuilder = new Builder<>();

                SortedSet<Event<Pair<Double, Ensemble>>> setOfPairs = new TreeSet<>();

                Event<Pair<Double, Ensemble>> event = Event.of( Instant.MAX,
                                                                Pair.of( Double.NaN, Ensemble.of( Double.NaN ) ) );

                setOfPairs.add( event );

                TimeSeriesMetadata boilerplate = EnsemblePairsWriterTest.getBoilerplateMetadata();
                TimeSeriesMetadata metadata =
                        new TimeSeriesMetadata.Builder( boilerplate ).setFeature( Feature.of(
                                                                             wres.statistics.MessageFactory.getGeometry( "PINEAPPLE" ) ) )
                                                                     .build();

                TimeSeries<Pair<Double, Ensemble>> timeSeriesNaN = TimeSeries.of( metadata,
                                                                                  setOfPairs );
                tsBuilder.addData( timeSeriesNaN );

                // Set the measurement units and time scale
                GeometryTuple geoTuple =
                        wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry(
                                                                                 "PINEAPPLE" ),
                                                                         wres.statistics.MessageFactory.getGeometry(
                                                                                 "PINEAPPLE" ),
                                                                         null );
                GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( null, geoTuple );
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

                PoolMetadata meta = PoolMetadata.of( evaluation, pool );

                TimeSeries<Pair<Double, Ensemble>> timeSeriesOne = TimeSeries.of( metadata,
                                                                                  Collections.emptySortedSet() );

                wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> emptyPairs =
                        tsBuilder.addData( timeSeriesOne )
                                 .setMetadata( meta )
                                 .build();

                // Write the pairs
                writer.accept( emptyPairs );

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Assert the expected results
                assertEquals( 2, results.size() );
                assertEquals( "FEATURE NAME,"
                              + "FEATURE GROUP NAME,"
                              + "VARIABLE NAME,"
                              + "REFERENCE TIME,"
                              + "VALID TIME,"
                              + "LEAD DURATION "
                              + "[MEAN OVER PAST PT1H],"
                              + "OBSERVED IN SCOOBIES,"
                              + "PREDICTED MEMBER 1 IN SCOOBIES",
                              results.get( 0 ) );

                assertEquals( "PINEAPPLE,,ARMS,,+1000000000-12-31T23:59:59.999999999Z,"
                              + "PT0S,NaN,NaN",
                              results.get( 1 ) );
            }
        }
    }

    /**
     * Builds a {@link EnsemblePairsWriter}, writes some pairs, and checks that the written output matches the
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
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath ) )
            {
                // Prime the writer with the expected ensemble structure
                writer.prime( new TreeSet<>( Set.of( "1", "2", "3" ) ) );

                // Write the pairs
                writer.accept( EnsemblePairsWriterTest.pairs );

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
                              + "OBSERVED IN SCOOBIES,"
                              + "PREDICTED MEMBER 1 IN SCOOBIES,"
                              + "PREDICTED MEMBER 2 IN SCOOBIES,"
                              + "PREDICTED MEMBER 3 IN SCOOBIES",
                              results.get( 0 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T01:00:00Z,PT1H,1.001,2.0,3.0,4.0",
                              results.get( 1 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T02:00:00Z,PT2H,5.0,6.0,7.0,8.0",
                              results.get( 2 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T03:00:00Z,PT3H,9.0,10.0,11.0,12.0",
                              results.get( 3 ) );
            }
        }
    }

    /**
     * Builds a {@link EnsemblePairsWriter}, writes a pair, and checks the output is zipped
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
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, null, true ) )
            {
                // Prime the writer with the expected ensemble structure
                writer.prime( new TreeSet<>( Set.of( "1", "2", "3" ) ) );

                // Write the pairs with gzip set to true
                writer.accept( EnsemblePairsWriterTest.pairs );

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
     * Builds a {@link EnsemblePairsWriter}, writes two sets of pairs synchronously, and checks that the written 
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
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath ) )
            {
                // Prime the writer with the expected ensemble structure
                writer.prime( new TreeSet<>( Set.of( "1", "2", "3" ) ) );

                // Write the pairs
                writer.accept( EnsemblePairsWriterTest.pairs );
                writer.accept( EnsemblePairsWriterTest.pairsTwo );

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
                              + "PREDICTED MEMBER 1 IN SCOOBIES,"
                              + "PREDICTED MEMBER 2 IN SCOOBIES,"
                              + "PREDICTED MEMBER 3 IN SCOOBIES",
                              results.get( 0 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T01:00:00Z,PT1H,1.001,2.0,3.0,4.0",
                              results.get( 1 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T02:00:00Z,PT2H,5.0,6.0,7.0,8.0",
                              results.get( 2 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T03:00:00Z,PT3H,9.0,10.0,11.0,12.0",
                              results.get( 3 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T04:00:00Z,PT4H,13.0,14.0,15.0,16.0",
                              results.get( 4 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T05:00:00Z,PT5H,17.0,18.0,19.0,20.0",
                              results.get( 5 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T06:00:00Z,PT6H,21.0,22.0,23.0,24.0",
                              results.get( 6 ) );
            }
        }
    }

    /**
     * Builds a {@link EnsemblePairsWriter}, writes three sets of pairs asynchronously, and checks that the written 
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
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, formatter ) )
            {
                // Prime the writer with the expected ensemble structure
                writer.prime( new TreeSet<>( Set.of( "1", "2", "3" ) ) );

                // Write the pairs async on the common FJP
                CompletableFuture.allOf( CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairs ) ),
                                         CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairsTwo ) ),
                                         CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairsThree ) ) )
                                 .get();

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Sort the results
                results.sort( Comparator.naturalOrder() );

                // Assert the expected results
                assertEquals( 10, results.size() );
                assertEquals( "BANANA,,ARMS,1985-01-01T00:00:00Z,1985-01-01T07:00:00Z,PT7H,25.0,26.0,27.0,28.0",
                              results.get( 0 ) );
                assertEquals( "BANANA,,ARMS,1985-01-01T00:00:00Z,1985-01-01T08:00:00Z,PT8H,29.0,30.0,31.0,32.0",
                              results.get( 1 ) );
                assertEquals( "BANANA,,ARMS,1985-01-01T00:00:00Z,1985-01-01T09:00:00Z,PT9H,33.0,34.0,35.0,36.0",
                              results.get( 2 ) );
                assertEquals( "FEATURE NAME,"
                              + "FEATURE GROUP NAME,"
                              + "VARIABLE NAME,"
                              + "REFERENCE TIME,"
                              + "VALID TIME,"
                              + "LEAD DURATION,"
                              + "OBSERVED IN SCOOBIES,"
                              + "PREDICTED MEMBER 1 IN SCOOBIES,"
                              + "PREDICTED MEMBER 2 IN SCOOBIES,"
                              + "PREDICTED MEMBER 3 IN SCOOBIES",
                              results.get( 3 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T04:00:00Z,PT4H,13.0,14.0,15.0,16.0",
                              results.get( 4 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T05:00:00Z,PT5H,17.0,18.0,19.0,20.0",
                              results.get( 5 ) );
                assertEquals( "ORANGE,,ARMS,1985-01-01T00:00:00Z,1985-01-01T06:00:00Z,PT6H,21.0,22.0,23.0,24.0",
                              results.get( 6 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T01:00:00Z,PT1H,1.0,2.0,3.0,4.0",
                              results.get( 7 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T02:00:00Z,PT2H,5.0,6.0,7.0,8.0",
                              results.get( 8 ) );
                assertEquals( "PLUM,,ARMS,1985-01-01T00:00:00Z,1985-01-01T03:00:00Z,PT3H,9.0,10.0,11.0,12.0",
                              results.get( 9 ) );
            }
        }
    }

    /**
     * Builds a {@link EnsemblePairsWriter}, writes a large number of pairs asynchronously, and checks that the written 
     * output contains the expected number of rows.
     * @throws IOException if the writing or removal of the paired file fails
     * @throws ExecutionException if the asynchronous execution fails
     * @throws InterruptedException the the execution is interrupted 
     */

    @Test
    public void testAcceptForManySetsOfPairsWrittenAsync() throws IOException, InterruptedException, ExecutionException
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
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, formatter ) )
            {
                // Prime the writer with the expected ensemble structure
                writer.prime( new TreeSet<>( Set.of( "1", "2", "3" ) ) );

                // Write the pairs async on the common FJP
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for ( int i = 0; i < 100; i++ )
                {
                    futures.add( CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairs ) ) );
                }

                CompletableFuture.allOf( futures.toArray( new CompletableFuture[0] ) )
                                 .get();

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Sort the results
                results.sort( Comparator.naturalOrder() );

                // Assert the expected results by dimension
                assertEquals( 301, results.size() );
            }
        }
    }

    /**
     * Builds a {@link EnsemblePairsWriter}, writes some pairs, and checks that the 
     * {@link EnsemblePairsWriter#get()} returns the correct path written.
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
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath ) )
            {
                // Prime the writer with the expected ensemble structure
                writer.prime( new TreeSet<>( Set.of() ) );

                // Write the pairs
                writer.accept( EnsemblePairsWriterTest.pairs );

                // Assert the expected results
                assertEquals( writer.get(), Set.of( csvPath ) );
            }
        }
    }

}
