package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfStatistics.ListOfStatisticsBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link ListOfStatistics}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class ListOfStatisticsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Instance of some output metadata to use in testing.
     */

    private StatisticMetadata metadata;


    @Before
    public void runBeforeEachTest()
    {
        metadata = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                         0,
                                         MeasurementUnit.of(),
                                         MetricConstants.BIAS_FRACTION,
                                         MetricConstants.MAIN );
    }

    /**
     * Tests the construction of a {@link ListOfStatistics} using the 
     * {@link ListOfStatistics#of(List, StatisticMetadata)}
     */

    @Test
    public void testBuild()
    {
        assertNotNull( ListOfStatistics.of( Collections.emptyList() ) );
    }

    /**
     * Tests the construction of a {@link ListOfStatistics} using the 
     * {@link ListOfStatisticsBuilder}.
     */

    @Test
    public void testBuildUsingBuilder()
    {
        ListOfStatisticsBuilder<DoubleScoreStatistic> builder = new ListOfStatisticsBuilder<>();

        ListOfStatistics<DoubleScoreStatistic> actualOutput =
                builder.addStatistic( DoubleScoreStatistic.of( 0.1, metadata ) ).build();

        ListOfStatistics<DoubleScoreStatistic> expectedOutput =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        assertEquals( actualOutput, expectedOutput );
    }

    /**
     * Tests the incremental construction of a {@link ListOfStatistics} using the 
     * {@link ListOfStatisticsBuilder} with multiple threads operating concurrently.
     */

    @Test
    public void testBuildUsingBuilderWithMultipleThreads()
    {
        ListOfStatisticsBuilder<DoubleScoreStatistic> builder =
                new ListOfStatisticsBuilder<DoubleScoreStatistic>();

        // Initialize 100 futures that add results to the builder
        SortedSet<Double> expectedOutput = new TreeSet<>();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[100];

        ExecutorService service = null;
        try
        {
            service = Executors.newFixedThreadPool( 10 );
            for ( int i = 0; i < 100; i++ )
            {
                double next = i;
                expectedOutput.add( next );
                futures[i] =
                        CompletableFuture.supplyAsync( () -> builder.addStatistic( DoubleScoreStatistic.of( next,
                                                                                                            metadata ) ),
                                                       service );
            }

            // Complete all additions by joining
            CompletableFuture.allOf( futures ).join();
        }
        finally
        {
            if ( Objects.nonNull( service ) )
            {
                service.shutdownNow();
            }
        }

        // Build and validate
        SortedSet<Double> actualOutput = Slicer.discover( builder.build(), output -> output.getData() );

        assertEquals( actualOutput, expectedOutput );
    }

    /**
     * Tests that construction of a {@link ListOfStatistics} using the 
     * {@link ListOfStatistics#of(List, StatisticMetadata)} throws an exception when the outputs are null.
     */

    @Test
    public void testCannotBuildWithNullOutput()
    {
        exception.expect( NullPointerException.class );

        exception.expectMessage( "Specify a non-null list of outputs." );

        ListOfStatistics.of( null );
    }

    /**
     * Tests that construction of a {@link ListOfStatistics} using the 
     * {@link ListOfStatistics#of(List, StatisticMetadata)} throws an exception when one or more of the listed 
     * outputs is null.
     */

    @Test
    public void testCannotBuildWithOneOrMoreNullOutputs()
    {
        exception.expect( StatisticException.class );

        exception.expectMessage( "Cannot build a list of outputs with one or more null entries." );

        ListOfStatistics.of( Collections.singletonList( null ) );
    }

    /**
     * Tests the iteration cannot lead to mutation of the output list.
     */

    @Test
    public void testIteratorCannotMutate()
    {
        exception.expect( UnsupportedOperationException.class );

        ListOfStatistics<DoubleScoreStatistic> list =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        // Removing an element throws an exception
        list.iterator().remove();
    }

    /**
     * Tests that the expected data is returned by {@link ListOfStatistics#getData()}.
     */

    @Test
    public void testGetData()
    {
        ListOfStatistics<DoubleScoreStatistic> list =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        assertEquals( list.getData(), Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );
    }

    /**
     * Tests that mutations to the data returned by {@link ListOfStatistics#getData()} are not allowed.
     */

    @Test
    public void testGetDataCannotMutate()
    {
        exception.expect( UnsupportedOperationException.class );

        ListOfStatistics<DoubleScoreStatistic> list =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        list.getData().add( DoubleScoreStatistic.of( 0.1, metadata ) );
    }

    /**
     * Tests the {@link ListOfStatistics#equals(Object)}.
     */

    @SuppressWarnings( "unlikely-arg-type" )
    @Test
    public void testEquals()
    {
        // Reflexive 
        ListOfStatistics<DoubleScoreStatistic> first =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        assertTrue( "The output list does not meet the equals contract for reflexivity.",
                    first.equals( first ) );

        // Symmetric
        ListOfStatistics<DoubleScoreStatistic> second =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        assertTrue( "The output list does not meet the equals contract for symmetry.",
                    second.equals( first ) && first.equals( second ) );

        // Transitive
        ListOfStatistics<DoubleScoreStatistic> third =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        assertTrue( "The output list does not meet the equals contract for transitivity.",
                    first.equals( third ) && third.equals( second ) && first.equals( second ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The output list does not meet the equals contract for consistency.",
                        first.equals( second ) );
        }

        // Different non-null type
        assertFalse( "Unexpected equality.", first.equals( metadata ) );

        // Check unequal cases

        // Unequal on data
        ListOfStatistics<DoubleScoreStatistic> fourth =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.2, metadata ) ) );

        assertFalse( "Expected unequal data.", first.equals( fourth ) );

        // Unequal on null type
        assertFalse( "Expected unequal outputs.", first.equals( null ) );

    }

    /**
     * Tests the {@link ListOfStatistics#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Consistent with equals
        ListOfStatistics<DoubleScoreStatistic> first =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        ListOfStatistics<DoubleScoreStatistic> second =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1, metadata ) ) );

        assertTrue( "The hashcode of the output list is inconsistent with equals.",
                    first.equals( second ) && first.hashCode() == second.hashCode() );

        // Consistent when called repeatedly
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The output lists do not meet the hashcode contract for consistency.",
                        first.hashCode() == second.hashCode() );
        }
    }

    /**
     * Tests the {@link ListOfStatistics#toString()}.
     */

    @Test
    public void testToString()
    {
        TimeWindow window = TimeWindow.of( Instant.MIN, Instant.MAX );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 2.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        OneOrTwoThresholds thresholdThree =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        ListOfStatistics<DoubleScoreStatistic> listOfOutputs =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1,
                                                                             StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                                                                                      null,
                                                                                                                      window,
                                                                                                                      thresholdOne ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ),
                                                    DoubleScoreStatistic.of( 0.2,
                                                                             StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                                                                                      null,
                                                                                                                      window,
                                                                                                                      thresholdTwo ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ),
                                                    DoubleScoreStatistic.of( 0.3,
                                                                             StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                                                                                      null,
                                                                                                                      window,
                                                                                                                      thresholdThree ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ) ) );

        StringBuilder expected = new StringBuilder();
        expected.append( "{([-1000000000-01-01T00:00:00Z, +1000000000-12-31T23:59:59.999999999Z, VALID TIME, PT0S, "
                         + "PT0S],> 1.0,DIMENSIONLESS,DIMENSIONLESS,0,BIAS FRACTION,MAIN): 0.1}" )
                .append( System.lineSeparator() )
                .append( "{([-1000000000-01-01T00:00:00Z, +1000000000-12-31T23:59:59.999999999Z, VALID TIME, PT0S, "
                         + "PT0S],> 2.0,DIMENSIONLESS,DIMENSIONLESS,0,BIAS FRACTION,MAIN): 0.2}" )
                .append( System.lineSeparator() )
                .append( "{([-1000000000-01-01T00:00:00Z, +1000000000-12-31T23:59:59.999999999Z, VALID TIME, PT0S, "
                         + "PT0S],> 3.0,DIMENSIONLESS,DIMENSIONLESS,0,BIAS FRACTION,MAIN): 0.3}" );

        assertEquals( expected.toString(), listOfOutputs.toString() );

    }

}
