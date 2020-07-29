package wres.io.writing.commaseparated.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link SingleValuedPairsWriter}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SingleValuedPairsWriterTest
{

    /**
     * First set of pairs to use for writing.
     */

    private static PoolOfPairs<Double, Double> pairs = null;

    /**
     * Second set of pairs to use for writing.
     */

    private static PoolOfPairs<Double, Double> pairsTwo = null;

    /**
     * Third set of pairs to use for writing.
     */

    private static PoolOfPairs<Double, Double> pairsThree = null;

    private static final String VARIABLE_NAME = "ARMS";
    private static final FeatureKey FEATURE = FeatureKey.of( "FRUIT" );
    private static final String UNIT = "SCOOBIES";

    private Path tempDir = null;

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
        PoolOfPairsBuilder<Double, Double> tsBuilder = new PoolOfPairsBuilder<>();

        SortedSet<Event<Pair<Double, Double>>> setOfPairs = new TreeSet<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ),
                                  Pair.of( 1.001, 2.0 ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), Pair.of( 3.0, 4.0 ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), Pair.of( 5.0, 6.0 ) ) );

        SampleMetadata meta =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( new FeatureTuple( FeatureKey.of( "PLUM" ),
                                                                           FeatureKey.of( "PLUM" ),
                                                                           null ),
                                                         "RIFLE" ) );
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithT0( basisTime );
        TimeSeries<Pair<Double, Double>> timeSeriesOne =
                TimeSeries.of( metadata, setOfPairs );

        pairs = tsBuilder.addTimeSeries( timeSeriesOne ).setMetadata( meta ).build();

        // Create the second time-series of pairs
        PoolOfPairsBuilder<Double, Double> tsBuilderTwo = new PoolOfPairsBuilder<>();
        SortedSet<Event<Pair<Double, Double>>> setOfPairsTwo = new TreeSet<>();
        Instant basisTimeTwo = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ),
                                     Pair.of( 7.0, 8.0 ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ),
                                     Pair.of( 9.0, 10.0 ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ),
                                     Pair.of( 11.0, 12.0 ) ) );

        SampleMetadata metaTwo =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( new FeatureTuple( FeatureKey.of( "ORANGE" ),
                                                                           FeatureKey.of( "ORANGE" ),
                                                                           null ),
                                                         "PISTOL" ) );
        TimeSeriesMetadata metadataTwo = getBoilerplateMetadataWithT0( basisTimeTwo );
        TimeSeries<Pair<Double, Double>> timeSeriesTwo =
                TimeSeries.of( metadataTwo, setOfPairsTwo );

        pairsTwo = tsBuilderTwo.addTimeSeries( timeSeriesTwo ).setMetadata( metaTwo ).build();


        // Create the third time-series of pairs
        PoolOfPairsBuilder<Double, Double> tsBuilderThree = new PoolOfPairsBuilder<>();
        SortedSet<Event<Pair<Double, Double>>> setOfPairsThree = new TreeSet<>();
        Instant basisTimeThree = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T07:00:00Z" ),
                                       Pair.of( 13.0, 14.0 ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ),
                                       Pair.of( 15.0, 16.0 ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ),
                                       Pair.of( 17.0, 18.0 ) ) );

        SampleMetadata metaThree =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( new FeatureTuple( FeatureKey.of( "BANANA" ),
                                                                           FeatureKey.of( "BANANA" ),
                                                                           null ),
                                                         "GRENADE" ) );
        TimeSeriesMetadata metadataThree = getBoilerplateMetadataWithT0( basisTimeThree );
        TimeSeries<Pair<Double, Double>> timeSeriesThree =
                TimeSeries.of( metadataThree, setOfPairsThree );

        pairsThree = tsBuilderThree.addTimeSeries( timeSeriesThree ).setMetadata( metaThree ).build();
    }

    @Before
    public void setup() throws IOException
    {
        this.tempDir = Files.createTempDirectory( "wres_temp" );
    }

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes some empty pairs, and checks that the written output matches the
     * expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptWithEmptyPairs() throws IOException
    {
        // Create the path
        Path pathToFile = Paths.get( this.tempDir.toString(), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
        {

            PoolOfPairsBuilder<Double, Double> tsBuilder = new PoolOfPairsBuilder<>();

            // Set the measurement units and time scale
            FeatureTuple featureTuple = new FeatureTuple( FeatureKey.of( "PINEAPPLE" ),
                                                          FeatureKey.of( "PINEAPPLE" ),
                                                          null );
            Evaluation evaluation = MessageFactory.parse( MeasurementUnit.of( "SCOOBIES" ),
                                                          DatasetIdentifier.of( featureTuple,
                                                                                "MORTARS" ),
                                                          null );
            Pool pool = MessageFactory.parse( featureTuple,
                                              null,
                                              TimeScaleOuter.of( Duration.ofSeconds( 3600 ),
                                                                 TimeScaleFunction.MEAN ),
                                              null,
                                              false );

            SampleMetadata metadata = SampleMetadata.of( evaluation, pool );

            PoolOfPairs<Double, Double> emptyPairs =
                    tsBuilder.addTimeSeries( TimeSeries.of( getBoilerplateMetadata(),
                                                            Collections.emptySortedSet() ) )
                             .setMetadata( metadata )
                             .build();

            // Write the pairs
            writer.accept( emptyPairs );

            // Assert the expected results of nothing
            assertFalse( pathToFile.toFile().exists() );

            // Nothing expected
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( this.tempDir.toString(), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
        {

            // Write the pairs
            writer.accept( pairs );

            // Read the results
            List<String> results = Files.readAllLines( pathToFile );

            // Assert the expected results
            assertTrue( results.size() == 4 );
            assertTrue( results.get( 0 )
                               .equals( "FEATURE DESCRIPTION,"
                                        + "VALID TIME OF PAIR,"
                                        + "LEAD DURATION OF PAIR IN SECONDS,"
                                        + "LEFT IN SCOOBIES,RIGHT IN SCOOBIES" ) );
            assertTrue( results.get( 1 ).equals( "PLUM,1985-01-01T01:00:00Z,3600,1.001,2.0" ) );
            assertTrue( results.get( 2 ).equals( "PLUM,1985-01-01T02:00:00Z,7200,3.0,4.0" ) );
            assertTrue( results.get( 3 ).equals( "PLUM,1985-01-01T03:00:00Z,10800,5.0,6.0" ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
        }
    }

    /**
     * Builds a {@link SingleValuedPairsWriter} whose {@link SampleMetadata} includes a {@link TimeWindowOuter},
     * writes some pairs, and checks that the written output matches the expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptForOneSetOfPairsWithTimeWindow() throws IOException
    {
        // Create the path
        Path pathToFile = Paths.get( this.tempDir.toString(), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
        {

            // Create the pairs with a time window
            PoolOfPairsBuilder<Double, Double> tsBuilder = new PoolOfPairsBuilder<>();
            tsBuilder.addTimeSeries( pairs );
            tsBuilder.setMetadata( SampleMetadata.of( pairs.getMetadata(),
                                                      TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                          Instant.parse( "1990-01-01T00:00:00Z" ),
                                                                          Duration.ZERO ) ) );


            // Write the pairs
            writer.accept( tsBuilder.build() );

            // Read the results
            List<String> results = Files.readAllLines( pathToFile );

            // Assert the expected results
            assertTrue( results.size() == 4 );
            assertTrue( results.get( 0 )
                               .equals( "FEATURE DESCRIPTION,"
                                        + "EARLIEST ISSUE TIME,"
                                        + "LATEST ISSUE TIME,"
                                        + "EARLIEST VALID TIME,"
                                        + "LATEST VALID TIME,"
                                        + "EARLIEST LEAD TIME IN SECONDS,"
                                        + "LATEST LEAD TIME IN SECONDS,"
                                        + "VALID TIME OF PAIR,"
                                        + "LEAD DURATION OF PAIR IN SECONDS,"
                                        + "LEFT IN SCOOBIES,"
                                        + "RIGHT IN SCOOBIES" ) );

            assertTrue( results.get( 1 )
                               .equals( "PLUM,1985-01-01T00:00:00Z,"
                                        + "1990-01-01T00:00:00Z,"
                                        + Instant.MIN
                                        + ","
                                        + Instant.MAX
                                        + ",0,0,1985-01-01T01:00:00Z,3600,1.001,2.0" ) );
            assertTrue( results.get( 2 )
                               .equals( "PLUM,1985-01-01T00:00:00Z,"
                                        + "1990-01-01T00:00:00Z,"
                                        + Instant.MIN
                                        + ","
                                        + Instant.MAX
                                        + ",0,0,1985-01-01T02:00:00Z,7200,3.0,4.0" ) );
            assertTrue( results.get( 3 )
                               .equals( "PLUM,1985-01-01T00:00:00Z,"
                                        + "1990-01-01T00:00:00Z,"
                                        + Instant.MIN
                                        + ","
                                        + Instant.MAX
                                        + ",0,0,1985-01-01T03:00:00Z,10800,5.0,6.0" ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( this.tempDir.toString(), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
        {

            // Write the pairs
            writer.accept( pairs );
            writer.accept( pairsTwo );

            // Read the results
            List<String> results = Files.readAllLines( pathToFile );

            // Assert the expected results
            assertTrue( results.size() == 7 );
            assertTrue( results.get( 0 )
                               .equals( "FEATURE DESCRIPTION,"
                                        + "VALID TIME OF PAIR,"
                                        + "LEAD DURATION OF PAIR IN SECONDS,"
                                        + "LEFT IN SCOOBIES,"
                                        + "RIGHT IN SCOOBIES" ) );
            assertTrue( results.get( 1 ).equals( "PLUM,1985-01-01T01:00:00Z,3600,1.001,2.0" ) );
            assertTrue( results.get( 2 ).equals( "PLUM,1985-01-01T02:00:00Z,7200,3.0,4.0" ) );
            assertTrue( results.get( 3 ).equals( "PLUM,1985-01-01T03:00:00Z,10800,5.0,6.0" ) );
            assertTrue( results.get( 4 ).equals( "ORANGE,1985-01-01T04:00:00Z,14400,7.0,8.0" ) );
            assertTrue( results.get( 5 ).equals( "ORANGE,1985-01-01T05:00:00Z,18000,9.0,10.0" ) );
            assertTrue( results.get( 6 ).equals( "ORANGE,1985-01-01T06:00:00Z,21600,11.0,12.0" ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( this.tempDir.toString(), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer with a decimal format
        DecimalFormat formatter = new DecimalFormat();
        formatter.applyPattern( "0.0" );
        try ( SingleValuedPairsWriter writer =
                SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS, formatter ) )
        {

            // Write the pairs async on the common FJP
            CompletableFuture.allOf( CompletableFuture.runAsync( () -> writer.accept( pairs ) ),
                                     CompletableFuture.runAsync( () -> writer.accept( pairsTwo ) ),
                                     CompletableFuture.runAsync( () -> writer.accept( pairsThree ) ) )
                             .get();

            // Read the results
            List<String> results = Files.readAllLines( pathToFile );

            // Sort the results
            Collections.sort( results, Comparator.naturalOrder() );

            // Assert the expected results
            assertTrue( results.size() == 10 );
            assertTrue( results.get( 0 ).equals( "BANANA,1985-01-01T07:00:00Z,25200,13.0,14.0" ) );
            assertTrue( results.get( 1 ).equals( "BANANA,1985-01-01T08:00:00Z,28800,15.0,16.0" ) );
            assertTrue( results.get( 2 ).equals( "BANANA,1985-01-01T09:00:00Z,32400,17.0,18.0" ) );
            assertTrue( results.get( 3 )
                               .equals( "FEATURE DESCRIPTION,"
                                        + "VALID TIME OF PAIR,"
                                        + "LEAD DURATION OF PAIR IN SECONDS,"
                                        + "LEFT IN SCOOBIES,"
                                        + "RIGHT IN SCOOBIES" ) );
            assertTrue( results.get( 4 ).equals( "ORANGE,1985-01-01T04:00:00Z,14400,7.0,8.0" ) );
            assertTrue( results.get( 5 ).equals( "ORANGE,1985-01-01T05:00:00Z,18000,9.0,10.0" ) );
            assertTrue( results.get( 6 ).equals( "ORANGE,1985-01-01T06:00:00Z,21600,11.0,12.0" ) );
            assertTrue( results.get( 7 ).equals( "PLUM,1985-01-01T01:00:00Z,3600,1.0,2.0" ) );
            assertTrue( results.get( 8 ).equals( "PLUM,1985-01-01T02:00:00Z,7200,3.0,4.0" ) );
            assertTrue( results.get( 9 ).equals( "PLUM,1985-01-01T03:00:00Z,10800,5.0,6.0" ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( this.tempDir.toString(), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
        {

            // Write the pairs
            writer.accept( SingleValuedPairsWriterTest.pairs );

            // Assert the expected results
            assertEquals( writer.get(), Set.of( pathToFile ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
        }
    }

    @After
    public void tearDown() throws IOException
    {
        Files.deleteIfExists( this.tempDir );
    }
}
