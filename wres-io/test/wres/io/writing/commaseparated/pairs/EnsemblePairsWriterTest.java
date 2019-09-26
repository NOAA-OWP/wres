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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link EnsemblePairsWriter}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class EnsemblePairsWriterTest
{

    /**
     * First set of pairs to use for writing.
     */

    private static TimeSeriesOfEnsemblePairs pairs = null;

    /**
     * Second set of pairs to use for writing.
     */

    private static TimeSeriesOfEnsemblePairs pairsTwo = null;

    /**
     * Third set of pairs to use for writing.
     */

    private static TimeSeriesOfEnsemblePairs pairsThree = null;

    @BeforeClass
    public static void setUpBeforeAllTests()
    {

        // Create the pairs
        TimeSeriesOfEnsemblePairsBuilder tsBuilder = new TimeSeriesOfEnsemblePairsBuilder();

        SortedSet<Event<Pair<Double, Ensemble>>> setOfPairs = new TreeSet<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ),
                                  Pair.of( 1.001, Ensemble.of( 2, 3, 4 ) ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ),
                                  Pair.of( 5.0, Ensemble.of( 6, 7, 8 ) ) ) );
        setOfPairs.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ),
                                  Pair.of( 9.0, Ensemble.of( 10, 11, 12 ) ) ) );

        SampleMetadata meta =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( Location.of( "PLUM" ), "RIFLE" ) );

        TimeSeries<Pair<Double, Ensemble>> timeSeriesOne =
                TimeSeries.of( basisTime, ReferenceTimeType.DEFAULT, setOfPairs );

        pairs = (TimeSeriesOfEnsemblePairs) tsBuilder.addTimeSeries( timeSeriesOne )
                                                     .setMetadata( meta )
                                                     .build();

        // Create the second time-series of pairs
        TimeSeriesOfEnsemblePairsBuilder tsBuilderTwo = new TimeSeriesOfEnsemblePairsBuilder();
        SortedSet<Event<Pair<Double, Ensemble>>> setOfPairsTwo = new TreeSet<>();
        Instant basisTimeTwo = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ),
                                     Pair.of( 13.0, Ensemble.of( 14, 15, 16 ) ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ),
                                     Pair.of( 17.0, Ensemble.of( 18, 19, 20 ) ) ) );
        setOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ),
                                     Pair.of( 21.0, Ensemble.of( 22, 23, 24 ) ) ) );

        SampleMetadata metaTwo =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( Location.of( "ORANGE" ), "PISTOL" ) );

        TimeSeries<Pair<Double, Ensemble>> timeSeriesTwo =
                TimeSeries.of( basisTimeTwo, ReferenceTimeType.DEFAULT, setOfPairsTwo );

        pairsTwo = (TimeSeriesOfEnsemblePairs) tsBuilderTwo.addTimeSeries( timeSeriesTwo )
                                                           .setMetadata( metaTwo )
                                                           .build();


        // Create the third time-series of pairs
        TimeSeriesOfEnsemblePairsBuilder tsBuilderThree = new TimeSeriesOfEnsemblePairsBuilder();
        SortedSet<Event<Pair<Double, Ensemble>>> setOfPairsThree = new TreeSet<>();
        Instant basisTimeThree = Instant.parse( "1985-01-01T00:00:00Z" );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T07:00:00Z" ),
                                       Pair.of( 25.0, Ensemble.of( 26, 27, 28 ) ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ),
                                       Pair.of( 29.0, Ensemble.of( 30, 31, 32 ) ) ) );
        setOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ),
                                       Pair.of( 33.0, Ensemble.of( 34, 35, 36 ) ) ) );

        SampleMetadata metaThree =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( Location.of( "BANANA" ), "GRENADE" ) );

        TimeSeries<Pair<Double, Ensemble>> timeSeriesThree =
                TimeSeries.of( basisTimeThree, ReferenceTimeType.DEFAULT, setOfPairsThree );

        pairsThree =
                (TimeSeriesOfEnsemblePairs) tsBuilderThree.addTimeSeries( timeSeriesThree )
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
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
        {

            TimeSeriesOfEnsemblePairsBuilder tsBuilder = new TimeSeriesOfEnsemblePairsBuilder();

            // Set the measurement units and time scale
            SampleMetadata meta =
                    new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "SCOOBIES" ) )
                                               .setIdentifier( DatasetIdentifier.of( Location.of( "PINEAPPLE" ),
                                                                                     "MORTARS" ) )
                                               .setTimeScale( TimeScale.of( Duration.ofSeconds( 3600 ),
                                                                            TimeScaleFunction.MEAN ) )
                                               .build();

            TimeSeries<Pair<Double, Ensemble>> timeSeriesOne = TimeSeries.of( Collections.emptySortedSet() );

            TimeSeriesOfEnsemblePairs emptyPairs =
                    (TimeSeriesOfEnsemblePairs) tsBuilder.addTimeSeries( timeSeriesOne )
                                                         .setMetadata( meta )
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
     * Builds a {@link EnsemblePairsWriter}, writes some pairs with {@link Double#NaN} values, and checks that the 
     * written output matches the
     * expected output.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testAcceptWithNaNPairs() throws IOException
    {
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Formatter
        DecimalFormat formatter = new DecimalFormat();
        formatter.applyPattern( "0.0" );

        // Create the writer
        try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( pathToFile, ChronoUnit.SECONDS, formatter ) )
        {

            TimeSeriesOfEnsemblePairsBuilder tsBuilder = new TimeSeriesOfEnsemblePairsBuilder();

            SortedSet<Event<Pair<Double,Ensemble>>> setOfPairs = new TreeSet<>();

            Event<Pair<Double, Ensemble>> event = Event.of( Instant.MAX,
                                                            Pair.of( Double.NaN, Ensemble.of( Double.NaN ) ) );

            setOfPairs.add( event );

            TimeSeries<Pair<Double, Ensemble>> timeSeriesNaN = TimeSeries.of( setOfPairs );
            tsBuilder.addTimeSeries( timeSeriesNaN );

            // Set the measurement units and time scale
            SampleMetadata meta =
                    new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "SCOOBIES" ) )
                                               .setIdentifier( DatasetIdentifier.of( Location.of( "PINEAPPLE" ),
                                                                                     "MORTARS" ) )
                                               .setTimeScale( TimeScale.of( Duration.ofSeconds( 3600 ),
                                                                            TimeScaleFunction.MEAN ) )
                                               .build();

            TimeSeries<Pair<Double,Ensemble>> timeSeriesOne = TimeSeries.of( Collections.emptySortedSet() );

            TimeSeriesOfEnsemblePairs emptyPairs =
                    (TimeSeriesOfEnsemblePairs) tsBuilder.addTimeSeries( timeSeriesOne )
                                                         .setMetadata( meta )
                                                         .build();

            // Write the pairs
            writer.accept( emptyPairs );

            // Read the results
            List<String> results = Files.readAllLines( pathToFile );

            // Assert the expected results
            assertTrue( results.size() == 2 );
            assertEquals( results.get( 0 ),
                          "FEATURE DESCRIPTION,"
                                            + "VALID TIME OF PAIR,"
                                            + "LEAD DURATION OF PAIR IN SECONDS "
                                            + "[MEAN OVER PAST 3600 SECONDS],"
                                            + "LEFT IN SCOOBIES,"
                                            + "RIGHT MEMBER 1 IN SCOOBIES" );

            assertEquals( "PINEAPPLE,+1000000000-12-31T23:59:59.999999999Z,"
                          + "0,NaN,NaN",
                          results.get( 1 ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
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
                                        + "LEFT IN SCOOBIES,"
                                        + "RIGHT MEMBER 1 IN SCOOBIES,"
                                        + "RIGHT MEMBER 2 IN SCOOBIES,"
                                        + "RIGHT MEMBER 3 IN SCOOBIES" ) );
            assertTrue( results.get( 1 ).equals( "PLUM,1985-01-01T01:00:00Z,3600,1.001,2.0,3.0,4.0" ) );
            assertTrue( results.get( 2 ).equals( "PLUM,1985-01-01T02:00:00Z,7200,5.0,6.0,7.0,8.0" ) );
            assertTrue( results.get( 3 ).equals( "PLUM,1985-01-01T03:00:00Z,10800,9.0,10.0,11.0,12.0" ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
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
                                        + "RIGHT MEMBER 1 IN SCOOBIES,"
                                        + "RIGHT MEMBER 2 IN SCOOBIES,"
                                        + "RIGHT MEMBER 3 IN SCOOBIES" ) );
            assertTrue( results.get( 1 ).equals( "PLUM,1985-01-01T01:00:00Z,3600,1.001,2.0,3.0,4.0" ) );
            assertTrue( results.get( 2 ).equals( "PLUM,1985-01-01T02:00:00Z,7200,5.0,6.0,7.0,8.0" ) );
            assertTrue( results.get( 3 ).equals( "PLUM,1985-01-01T03:00:00Z,10800,9.0,10.0,11.0,12.0" ) );
            assertTrue( results.get( 4 ).equals( "ORANGE,1985-01-01T04:00:00Z,14400,13.0,14.0,15.0,16.0" ) );
            assertTrue( results.get( 5 ).equals( "ORANGE,1985-01-01T05:00:00Z,18000,17.0,18.0,19.0,20.0" ) );
            assertTrue( results.get( 6 ).equals( "ORANGE,1985-01-01T06:00:00Z,21600,21.0,22.0,23.0,24.0" ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer with a decimal format
        DecimalFormat formatter = new DecimalFormat();
        formatter.applyPattern( "0.0" );
        try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( pathToFile, ChronoUnit.SECONDS, formatter ) )
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
            assertTrue( results.get( 0 ).equals( "BANANA,1985-01-01T07:00:00Z,25200,25.0,26.0,27.0,28.0" ) );
            assertTrue( results.get( 1 ).equals( "BANANA,1985-01-01T08:00:00Z,28800,29.0,30.0,31.0,32.0" ) );
            assertTrue( results.get( 2 ).equals( "BANANA,1985-01-01T09:00:00Z,32400,33.0,34.0,35.0,36.0" ) );
            assertTrue( results.get( 3 )
                               .equals( "FEATURE DESCRIPTION,"
                                        + "VALID TIME OF PAIR,"
                                        + "LEAD DURATION OF PAIR IN SECONDS,"
                                        + "LEFT IN SCOOBIES,"
                                        + "RIGHT MEMBER 1 IN SCOOBIES,"
                                        + "RIGHT MEMBER 2 IN SCOOBIES,"
                                        + "RIGHT MEMBER 3 IN SCOOBIES" ) );
            assertTrue( results.get( 4 ).equals( "ORANGE,1985-01-01T04:00:00Z,14400,13.0,14.0,15.0,16.0" ) );
            assertTrue( results.get( 5 ).equals( "ORANGE,1985-01-01T05:00:00Z,18000,17.0,18.0,19.0,20.0" ) );
            assertTrue( results.get( 6 ).equals( "ORANGE,1985-01-01T06:00:00Z,21600,21.0,22.0,23.0,24.0" ) );
            assertTrue( results.get( 7 ).equals( "PLUM,1985-01-01T01:00:00Z,3600,1.0,2.0,3.0,4.0" ) );
            assertTrue( results.get( 8 ).equals( "PLUM,1985-01-01T02:00:00Z,7200,5.0,6.0,7.0,8.0" ) );
            assertTrue( results.get( 9 ).equals( "PLUM,1985-01-01T03:00:00Z,10800,9.0,10.0,11.0,12.0" ) );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer with a decimal format
        DecimalFormat formatter = new DecimalFormat();
        formatter.applyPattern( "0.0" );
        try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( pathToFile, ChronoUnit.SECONDS, formatter ) )
        {

            // Write the pairs async on the common FJP

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for ( int i = 0; i < 100; i++ )
            {
                futures.add( CompletableFuture.runAsync( () -> writer.accept( pairs ) ) );
            }

            CompletableFuture.allOf( futures.toArray( new CompletableFuture[futures.size()] ) ).get();

            // Read the results
            List<String> results = Files.readAllLines( pathToFile );

            // Sort the results
            Collections.sort( results, Comparator.naturalOrder() );

            // Assert the expected results by dimension
            assertEquals( results.size(), 301 );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
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
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        try ( EnsemblePairsWriter writer = EnsemblePairsWriter.of( pathToFile, ChronoUnit.SECONDS ) )
        {

            // Write the pairs
            writer.accept( pairs );

            // Assert the expected results
            assertEquals( writer.get(), pathToFile );

            // If all succeeded, remove the file, otherwise leave to help debugging
            Files.deleteIfExists( pathToFile );
        }
    }

}
