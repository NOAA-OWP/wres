package wres.io.writing.commaseparated.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.pairs.PoolOfPairs.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link EnsemblePairsWriter}.
 * 
 * @author James Brown
 */
public final class EnsemblePairsWriterTest
{

    private static final String VARIABLE_NAME = "ARMS";
    private static final FeatureKey FEATURE = FeatureKey.of( "FRUIT" );
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

    private static wres.datamodel.pools.Pool<Pair<Double, Ensemble>> pairs = null;

    /**
     * Second set of pairs to use for writing.
     */

    private static wres.datamodel.pools.Pool<Pair<Double, Ensemble>> pairsTwo = null;

    /**
     * Third set of pairs to use for writing.
     */

    private static wres.datamodel.pools.Pool<Pair<Double, Ensemble>> pairsThree = null;

    @BeforeClass
    public static void setUpBeforeAllTests()
    {

        // Create the pairs
        Builder<Double, Ensemble> tsBuilder = new Builder<>();

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

        FeatureTuple featureTuple = new FeatureTuple( FeatureKey.of( "PLUM" ),
                                                      FeatureKey.of( "PLUM" ),
                                                      null );
        FeatureGroup featureGroup = FeatureGroup.of( featureTuple );

        Pool pool = MessageFactory.parse( featureGroup,
                                          null,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );
        TimeSeriesMetadata boilerplate = EnsemblePairsWriterTest.getBoilerplateMetadataWithT0( basisTime );
        TimeSeriesMetadata metadata =
                new TimeSeriesMetadata.Builder( boilerplate ).setFeature( FeatureKey.of( "PLUM" ) )
                                                             .build();
        TimeSeries<Pair<Double, Ensemble>> timeSeriesOne =
                TimeSeries.of( metadata, setOfPairs );

        EnsemblePairsWriterTest.pairs = tsBuilder.addTimeSeries( timeSeriesOne ).setMetadata( meta ).build();

        // Create the second time-series of pairs
        Builder<Double, Ensemble> tsBuilderTwo = new Builder<>();
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

        FeatureTuple featureTupleTwo = new FeatureTuple( FeatureKey.of( "ORANGE" ),
                                                         FeatureKey.of( "ORANGE" ),
                                                         null );
        FeatureGroup featureGroupTwo = FeatureGroup.of( featureTupleTwo );

        Pool poolTwo = MessageFactory.parse( featureGroupTwo,
                                             null,
                                             null,
                                             null,
                                             false );

        PoolMetadata metaTwo = PoolMetadata.of( evaluationTwo, poolTwo );
        TimeSeriesMetadata boilerplateTwo = EnsemblePairsWriterTest.getBoilerplateMetadataWithT0( basisTimeTwo );
        TimeSeriesMetadata metadataTwo =
                new TimeSeriesMetadata.Builder( boilerplateTwo ).setFeature( FeatureKey.of( "ORANGE" ) )
                                                                .build();
        TimeSeries<Pair<Double, Ensemble>> timeSeriesTwo =
                TimeSeries.of( metadataTwo, setOfPairsTwo );

        EnsemblePairsWriterTest.pairsTwo = tsBuilderTwo.addTimeSeries( timeSeriesTwo )
                                                       .setMetadata( metaTwo )
                                                       .build();

        // Create the third time-series of pairs
        Builder<Double, Ensemble> tsBuilderThree = new Builder<>();
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

        FeatureTuple featureTupleThree = new FeatureTuple( FeatureKey.of( "BANANA" ),
                                                           FeatureKey.of( "BANANA" ),
                                                           null );
        FeatureGroup featureGroupThree = FeatureGroup.of( featureTupleThree );

        Pool poolThree = MessageFactory.parse( featureGroupThree,
                                               null,
                                               null,
                                               null,
                                               false );

        PoolMetadata metaThree = PoolMetadata.of( evaluationThree, poolThree );

        TimeSeriesMetadata boilerplateThree = EnsemblePairsWriterTest.getBoilerplateMetadataWithT0( basisTimeThree );
        TimeSeriesMetadata metadataThree =
                new TimeSeriesMetadata.Builder( boilerplateThree ).setFeature( FeatureKey.of( "BANANA" ) )
                                                                  .build();

        TimeSeries<Pair<Double, Ensemble>> timeSeriesThree =
                TimeSeries.of( metadataThree, setOfPairsThree );

        EnsemblePairsWriterTest.pairsThree = tsBuilderThree.addTimeSeries( timeSeriesThree )
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
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, ChronoUnit.SECONDS ) )
            {

                Builder<Double, Ensemble> tsBuilder = new Builder<>();

                // Set the measurement units and time scale
                FeatureTuple featureTuple = new FeatureTuple( FeatureKey.of( "PINEAPPLE" ),
                                                              FeatureKey.of( "PINEAPPLE" ),
                                                              null );

                Evaluation evaluation = Evaluation.newBuilder()
                                                  .setRightVariableName( "MORTARS" )
                                                  .setMeasurementUnit( "SCOOBIES" )
                                                  .build();

                Pool pool = MessageFactory.parse( FeatureGroup.of( featureTuple ),
                                                  null,
                                                  TimeScaleOuter.of( Duration.ofSeconds( 3600 ),
                                                                     TimeScaleFunction.MEAN ),
                                                  null,
                                                  false );

                PoolMetadata meta = PoolMetadata.of( evaluation, pool );

                TimeSeries<Pair<Double, Ensemble>> timeSeriesOne = TimeSeries.of( getBoilerplateMetadata(),
                                                                                  Collections.emptySortedSet() );

                wres.datamodel.pools.Pool<Pair<Double, Ensemble>> emptyPairs =
                        tsBuilder.addTimeSeries( timeSeriesOne ).setMetadata( meta ).build();

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
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, ChronoUnit.SECONDS, formatter ) )
            {

                Builder<Double, Ensemble> tsBuilder = new Builder<>();

                SortedSet<Event<Pair<Double, Ensemble>>> setOfPairs = new TreeSet<>();

                Event<Pair<Double, Ensemble>> event = Event.of( Instant.MAX,
                                                                Pair.of( Double.NaN, Ensemble.of( Double.NaN ) ) );

                setOfPairs.add( event );

                TimeSeriesMetadata boilerplate = EnsemblePairsWriterTest.getBoilerplateMetadata();
                TimeSeriesMetadata metadata =
                        new TimeSeriesMetadata.Builder( boilerplate ).setFeature( FeatureKey.of( "PINEAPPLE" ) )
                                                                     .build();

                TimeSeries<Pair<Double, Ensemble>> timeSeriesNaN = TimeSeries.of( metadata,
                                                                                  setOfPairs );
                tsBuilder.addTimeSeries( timeSeriesNaN );

                // Set the measurement units and time scale
                FeatureTuple featureTuple = new FeatureTuple( FeatureKey.of( "PINEAPPLE" ),
                                                              FeatureKey.of( "PINEAPPLE" ),
                                                              null );
                Evaluation evaluation = Evaluation.newBuilder()
                                                  .setRightVariableName( "MORTARS" )
                                                  .setMeasurementUnit( "SCOOBIES" )
                                                  .build();

                Pool pool = MessageFactory.parse( FeatureGroup.of( featureTuple ),
                                                  null,
                                                  TimeScaleOuter.of( Duration.ofSeconds( 3600 ),
                                                                     TimeScaleFunction.MEAN ),
                                                  null,
                                                  false );

                PoolMetadata meta = PoolMetadata.of( evaluation, pool );

                TimeSeries<Pair<Double, Ensemble>> timeSeriesOne = TimeSeries.of( metadata,
                                                                                  Collections.emptySortedSet() );

                wres.datamodel.pools.Pool<Pair<Double, Ensemble>> emptyPairs = tsBuilder.addTimeSeries( timeSeriesOne )
                                                                                        .setMetadata( meta )
                                                                                        .build();

                // Write the pairs
                writer.accept( emptyPairs );

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Assert the expected results
                assertEquals( 2, results.size() );
                assertEquals( "FEATURE DESCRIPTION,"
                              + "FEATURE GROUP NAME,"
                              + "VALID TIME OF PAIR,"
                              + "LEAD DURATION OF PAIR IN SECONDS "
                              + "[MEAN OVER PAST 3600 SECONDS],"
                              + "LEFT IN SCOOBIES,"
                              + "RIGHT MEMBER 1 IN SCOOBIES",
                              results.get( 0 ) );

                assertEquals( "PINEAPPLE,,+1000000000-12-31T23:59:59.999999999Z,"
                              + "0,NaN,NaN",
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
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, ChronoUnit.SECONDS ) )
            {
                // Write the pairs
                writer.accept( EnsemblePairsWriterTest.pairs );

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Assert the expected results
                assertEquals( 4, results.size() );
                assertEquals( "FEATURE DESCRIPTION,"
                              + "FEATURE GROUP NAME,"
                              + "VALID TIME OF PAIR,"
                              + "LEAD DURATION OF PAIR IN SECONDS,"
                              + "LEFT IN SCOOBIES,"
                              + "RIGHT MEMBER 1 IN SCOOBIES,"
                              + "RIGHT MEMBER 2 IN SCOOBIES,"
                              + "RIGHT MEMBER 3 IN SCOOBIES",
                              results.get( 0 ) );
                assertEquals( "PLUM,,1985-01-01T01:00:00Z,3600,1.001,2.0,3.0,4.0", results.get( 1 ) );
                assertEquals( "PLUM,,1985-01-01T02:00:00Z,7200,5.0,6.0,7.0,8.0", results.get( 2 ) );
                assertEquals( "PLUM,,1985-01-01T03:00:00Z,10800,9.0,10.0,11.0,12.0", results.get( 3 ) );

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
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, ChronoUnit.SECONDS ) )
            {

                // Write the pairs
                writer.accept( EnsemblePairsWriterTest.pairs );
                writer.accept( EnsemblePairsWriterTest.pairsTwo );

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Assert the expected results
                assertEquals( 7, results.size() );
                assertEquals( "FEATURE DESCRIPTION,"
                              + "FEATURE GROUP NAME,"
                              + "VALID TIME OF PAIR,"
                              + "LEAD DURATION OF PAIR IN SECONDS,"
                              + "LEFT IN SCOOBIES,"
                              + "RIGHT MEMBER 1 IN SCOOBIES,"
                              + "RIGHT MEMBER 2 IN SCOOBIES,"
                              + "RIGHT MEMBER 3 IN SCOOBIES",
                              results.get( 0 ) );
                assertEquals( "PLUM,,1985-01-01T01:00:00Z,3600,1.001,2.0,3.0,4.0", results.get( 1 ) );
                assertEquals( "PLUM,,1985-01-01T02:00:00Z,7200,5.0,6.0,7.0,8.0", results.get( 2 ) );
                assertEquals( "PLUM,,1985-01-01T03:00:00Z,10800,9.0,10.0,11.0,12.0", results.get( 3 ) );
                assertEquals( "ORANGE,,1985-01-01T04:00:00Z,14400,13.0,14.0,15.0,16.0", results.get( 4 ) );
                assertEquals( "ORANGE,,1985-01-01T05:00:00Z,18000,17.0,18.0,19.0,20.0", results.get( 5 ) );
                assertEquals( "ORANGE,,1985-01-01T06:00:00Z,21600,21.0,22.0,23.0,24.0", results.get( 6 ) );
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
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, ChronoUnit.SECONDS, formatter ) )
            {

                // Write the pairs async on the common FJP
                CompletableFuture.allOf( CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairs ) ),
                                         CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairsTwo ) ),
                                         CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairsThree ) ) )
                                 .get();

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Sort the results
                Collections.sort( results, Comparator.naturalOrder() );

                // Assert the expected results
                assertEquals( 10, results.size() );
                assertEquals( "BANANA,,1985-01-01T07:00:00Z,25200,25.0,26.0,27.0,28.0", results.get( 0 ) );
                assertEquals( "BANANA,,1985-01-01T08:00:00Z,28800,29.0,30.0,31.0,32.0", results.get( 1 ) );
                assertEquals( "BANANA,,1985-01-01T09:00:00Z,32400,33.0,34.0,35.0,36.0", results.get( 2 ) );
                assertEquals( "FEATURE DESCRIPTION,"
                              + "FEATURE GROUP NAME,"
                              + "VALID TIME OF PAIR,"
                              + "LEAD DURATION OF PAIR IN SECONDS,"
                              + "LEFT IN SCOOBIES,"
                              + "RIGHT MEMBER 1 IN SCOOBIES,"
                              + "RIGHT MEMBER 2 IN SCOOBIES,"
                              + "RIGHT MEMBER 3 IN SCOOBIES",
                              results.get( 3 ) );
                assertEquals( "ORANGE,,1985-01-01T04:00:00Z,14400,13.0,14.0,15.0,16.0", results.get( 4 ) );
                assertEquals( "ORANGE,,1985-01-01T05:00:00Z,18000,17.0,18.0,19.0,20.0", results.get( 5 ) );
                assertEquals( "ORANGE,,1985-01-01T06:00:00Z,21600,21.0,22.0,23.0,24.0", results.get( 6 ) );
                assertEquals( "PLUM,,1985-01-01T01:00:00Z,3600,1.0,2.0,3.0,4.0", results.get( 7 ) );
                assertEquals( "PLUM,,1985-01-01T02:00:00Z,7200,5.0,6.0,7.0,8.0", results.get( 8 ) );
                assertEquals( "PLUM,,1985-01-01T03:00:00Z,10800,9.0,10.0,11.0,12.0", results.get( 9 ) );
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
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, ChronoUnit.SECONDS, formatter ) )
            {
                // Write the pairs async on the common FJP
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for ( int i = 0; i < 100; i++ )
                {
                    futures.add( CompletableFuture.runAsync( () -> writer.accept( EnsemblePairsWriterTest.pairs ) ) );
                }

                CompletableFuture.allOf( futures.toArray( new CompletableFuture[futures.size()] ) ).get();

                // Read the results
                List<String> results = Files.readAllLines( csvPath );

                // Sort the results
                Collections.sort( results, Comparator.naturalOrder() );

                // Assert the expected results by dimension
                assertEquals( results.size(), 301 );
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
            Path csvPath = fileSystem.getPath( "test", PairsWriter.DEFAULT_PAIRS_NAME );

            // Create the writer
            try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( csvPath, ChronoUnit.SECONDS ) )
            {


                // Write the pairs
                writer.accept( EnsemblePairsWriterTest.pairs );

                // Assert the expected results
                assertEquals( writer.get(), Set.of( csvPath ) );
            }
        }
    }

}
