package wres.io.writing.commaseparated.pairs;

import static org.junit.Assert.assertEquals;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.Test;

import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.metadata.TimeScale;
import wres.datamodel.metadata.TimeScale.TimeScaleFunction;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;

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

    private static TimeSeriesOfSingleValuedPairs pairs = null;

    /**
     * Second set of pairs to use for writing.
     */

    private static TimeSeriesOfSingleValuedPairs pairsTwo = null;

    /**
     * Third set of pairs to use for writing.
     */

    private static TimeSeriesOfSingleValuedPairs pairsThree = null;

    @BeforeClass
    public static void setUpBeforeAllTests()
    {

        // Create the pairs
        TimeSeriesOfSingleValuedPairsBuilder tsBuilder = new TimeSeriesOfSingleValuedPairsBuilder();

        List<Event<SingleValuedPair>> listOfPairs = new ArrayList<>();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        listOfPairs.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1.001, 2 ) ) );
        listOfPairs.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 3, 4 ) ) );
        listOfPairs.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 5, 6 ) ) );

        SampleMetadata meta =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( Location.of( "PLUM" ), "RIFLE" ) );

        pairs = (TimeSeriesOfSingleValuedPairs) tsBuilder.addTimeSeriesData( basisTime, listOfPairs )
                                                         .setMetadata( meta )
                                                         .build();

        // Create the second time-series of pairs
        TimeSeriesOfSingleValuedPairsBuilder tsBuilderTwo = new TimeSeriesOfSingleValuedPairsBuilder();
        List<Event<SingleValuedPair>> listOfPairsTwo = new ArrayList<>();
        Instant basisTimeTwo = Instant.parse( "1985-01-01T00:00:00Z" );
        listOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T04:00:00Z" ), SingleValuedPair.of( 7, 8 ) ) );
        listOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T05:00:00Z" ), SingleValuedPair.of( 9, 10 ) ) );
        listOfPairsTwo.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ), SingleValuedPair.of( 11, 12 ) ) );

        SampleMetadata metaTwo =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( Location.of( "ORANGE" ), "PISTOL" ) );

        pairsTwo = (TimeSeriesOfSingleValuedPairs) tsBuilderTwo.addTimeSeriesData( basisTimeTwo, listOfPairsTwo )
                                                               .setMetadata( metaTwo )
                                                               .build();


        // Create the third time-series of pairs
        TimeSeriesOfSingleValuedPairsBuilder tsBuilderThree = new TimeSeriesOfSingleValuedPairsBuilder();
        List<Event<SingleValuedPair>> listOfPairsThree = new ArrayList<>();
        Instant basisTimeThree = Instant.parse( "1985-01-01T00:00:00Z" );
        listOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T07:00:00Z" ), SingleValuedPair.of( 13, 14 ) ) );
        listOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T08:00:00Z" ), SingleValuedPair.of( 15, 16 ) ) );
        listOfPairsThree.add( Event.of( Instant.parse( "1985-01-01T09:00:00Z" ), SingleValuedPair.of( 17, 18 ) ) );

        SampleMetadata metaThree =
                SampleMetadata.of( MeasurementUnit.of( "SCOOBIES" ),
                                   DatasetIdentifier.of( Location.of( "BANANA" ), "GRENADE" ) );

        pairsThree =
                (TimeSeriesOfSingleValuedPairs) tsBuilderThree.addTimeSeriesData( basisTimeThree, listOfPairsThree )
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
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS );

        TimeSeriesOfSingleValuedPairsBuilder tsBuilder = new TimeSeriesOfSingleValuedPairsBuilder();

        // Set the measurement units and time scale
        SampleMetadata meta =
                new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "SCOOBIES" ) )
                                           .setIdentifier( DatasetIdentifier.of( Location.of( "PINEAPPLE" ),
                                                                                 "MORTARS" ) )
                                           .setTimeScale( TimeScale.of( Duration.ofSeconds( 3600 ),
                                                                        TimeScaleFunction.MEAN ) )
                                           .build();

        TimeSeriesOfSingleValuedPairs emptyPairs =
                (TimeSeriesOfSingleValuedPairs) tsBuilder.addTimeSeriesData( Instant.now(), Collections.emptyList() )
                                                         .setMetadata( meta )
                                                         .build();

        // Write the pairs
        writer.accept( emptyPairs );

        // Read the results
        List<String> results = Files.readAllLines( pathToFile );

        // Assert the expected results
        assertTrue( results.size() == 1 );
        assertTrue( results.get( 0 ).equals( "FEATURE DESCRIPTION,VALID TIME,LEAD DURATION IN SECONDS "
                                             + "[MEAN OVER PAST 3600 SECONDS],"
                                             + "LEFT IN SCOOBIES,RIGHT IN SCOOBIES" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging
        Files.deleteIfExists( pathToFile );
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
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS );

        // Write the pairs
        writer.accept( pairs );

        // Read the results
        List<String> results = Files.readAllLines( pathToFile );

        // Assert the expected results
        assertTrue( results.size() == 4 );
        assertTrue( results.get( 0 ).equals( "FEATURE DESCRIPTION,"
                                             + "VALID TIME,"
                                             + "LEAD DURATION IN SECONDS,"
                                             + "LEFT IN SCOOBIES,RIGHT IN SCOOBIES" ) );
        assertTrue( results.get( 1 ).equals( "PLUM,1985-01-01T01:00:00Z,3600,1.001,2.0" ) );
        assertTrue( results.get( 2 ).equals( "PLUM,1985-01-01T02:00:00Z,7200,3.0,4.0" ) );
        assertTrue( results.get( 3 ).equals( "PLUM,1985-01-01T03:00:00Z,10800,5.0,6.0" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging
        Files.deleteIfExists( pathToFile );
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
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS );

        // Write the pairs
        writer.accept( pairs );
        writer.accept( pairsTwo );

        // Read the results
        List<String> results = Files.readAllLines( pathToFile );

        // Assert the expected results
        assertTrue( results.size() == 7 );
        assertTrue( results.get( 0 ).equals( "FEATURE DESCRIPTION,VALID TIME,"
                                             + "LEAD DURATION IN SECONDS,"
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

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes two sets of pairs asynchronously, and checks that the written 
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
        SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS, formatter );

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
        assertTrue( results.get( 3 ).equals( "FEATURE DESCRIPTION,"
                                             + "VALID TIME,"
                                             + "LEAD DURATION IN SECONDS,"
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

    /**
     * Builds a {@link SingleValuedPairsWriter}, writes some pairs, and checks that the 
     * {@link SingleValuedPairsWriter#get()} returns the correct path written.
     * @throws IOException if the writing or removal of the paired file fails
     */

    @Test
    public void testSuppliedPath() throws IOException
    {
        // Create the path
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ), PairsWriter.DEFAULT_PAIRS_NAME );

        // Create the writer
        SingleValuedPairsWriter writer = SingleValuedPairsWriter.of( pathToFile, ChronoUnit.SECONDS );

        // Write the pairs
        writer.accept( pairs );

        // Assert the expected results
        assertEquals( writer.get(), Collections.singleton( pathToFile ) );

        // If all succeeded, remove the file, otherwise leave to help debugging
        Files.deleteIfExists( pathToFile );
    }

}
