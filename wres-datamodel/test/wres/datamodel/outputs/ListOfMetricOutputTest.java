package wres.datamodel.outputs;

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
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.ListOfMetricOutput.ListOfMetricOutputBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link ListOfMetricOutput}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class ListOfMetricOutputTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Instance of some output metadata to use in testing.
     */

    private MetricOutputMetadata metadata;


    @Before
    public void runBeforeEachTest()
    {
        metadata = MetricOutputMetadata.of( 0,
                                            MeasurementUnit.of(),
                                            MeasurementUnit.of(),
                                            MetricConstants.BIAS_FRACTION );
    }

    /**
     * Tests the construction of a {@link ListOfMetricOutput} using the 
     * {@link ListOfMetricOutput#of(List, MetricOutputMetadata)}
     */

    @Test
    public void testBuild()
    {
        assertNotNull( ListOfMetricOutput.of( Collections.emptyList() ) );
    }

    /**
     * Tests the construction of a {@link ListOfMetricOutput} using the 
     * {@link ListOfMetricOutputBuilder}.
     */

    @Test
    public void testBuildUsingBuilder()
    {
        ListOfMetricOutputBuilder<DoubleScoreOutput> builder = new ListOfMetricOutputBuilder<>();

        ListOfMetricOutput<DoubleScoreOutput> actualOutput =
                builder.setMetadata( metadata ).addOutput( DoubleScoreOutput.of( 0.1, metadata ) ).build();

        ListOfMetricOutput<DoubleScoreOutput> expectedOutput =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        assertEquals( actualOutput, expectedOutput );
    }

    /**
     * Tests the construction of a {@link ListOfMetricOutput} using the 
     * {@link ListOfMetricOutputBuilder} with concurrent access.
     */

    @Test
    public void testBuildUsingBuilderWithMultipleThreads()
    {
        ListOfMetricOutputBuilder<DoubleScoreOutput> builder =
                new ListOfMetricOutputBuilder<DoubleScoreOutput>().setMetadata( metadata );

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
                        CompletableFuture.supplyAsync( () -> builder.addOutput( DoubleScoreOutput.of( next,
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
     * Tests that construction of a {@link ListOfMetricOutput} using the 
     * {@link ListOfMetricOutput#of(List, MetricOutputMetadata)} throws an exception when the outputs are null.
     */

    @Test
    public void testCannotBuildWithNullOutput()
    {
        exception.expect( NullPointerException.class );

        exception.expectMessage( "Specify a non-null list of outputs." );

        ListOfMetricOutput.of( null, metadata );
    }

    /**
     * Tests that construction of a {@link ListOfMetricOutput} using the 
     * {@link ListOfMetricOutput#of(List, MetricOutputMetadata)} throws an exception when one or more of the listed 
     * outputs is null.
     */

    @Test
    public void testCannotBuildWithOneOrMoreNullOutputs()
    {
        exception.expect( MetricOutputException.class );

        exception.expectMessage( "Cannot build a list of outputs with one or more null entries." );

        ListOfMetricOutput.of( Collections.singletonList( null ), metadata );
    }

    /**
     * Tests the iteration cannot lead to mutation of the output list.
     */

    @Test
    public void testIteratorCannotMutate()
    {
        exception.expect( UnsupportedOperationException.class );

        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        // Removing an element throws an exception
        list.iterator().remove();
    }

    /**
     * Tests that the expected metadata is returned by {@link ListOfMetricOutput#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList(), metadata );

        assertTrue( list.getMetadata().equals( metadata ) );
    }

    /**
     * Tests that the expected data is returned by {@link ListOfMetricOutput#getData()}.
     */

    @Test
    public void testGetData()
    {
        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        assertEquals( list.getData(), Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ) );
    }

    /**
     * Tests that mutations to the data returned by {@link ListOfMetricOutput#getData()} are not allowed.
     */

    @Test
    public void testGetDataCannotMutate()
    {
        exception.expect( UnsupportedOperationException.class );

        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        list.getData().add( DoubleScoreOutput.of( 0.1, metadata ) );
    }

    /**
     * Tests the {@link ListOfMetricOutput#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        ListOfMetricOutput<DoubleScoreOutput> first =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        assertTrue( "The output list does not meet the equals contract for reflexivity.",
                    first.equals( first ) );

        // Symmetric
        ListOfMetricOutput<DoubleScoreOutput> second =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        assertTrue( "The output list does not meet the equals contract for symmetry.",
                    second.equals( first ) && first.equals( second ) );

        // Transitive
        ListOfMetricOutput<DoubleScoreOutput> third =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        assertTrue( "The output list does not meet the equals contract for transitivity.",
                    first.equals( third ) && third.equals( second ) && first.equals( second ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The output list does not meet the equals contract for consistency.",
                        first.equals( second ) );
        }

        // Nullity
        assertFalse( "The output list not meet the equals contract for nullity.", first.equals( metadata ) );

        // Check unequal cases

        // Unequal on data
        ListOfMetricOutput<DoubleScoreOutput> fourth =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.2, metadata ) ), metadata );

        assertFalse( "Expected unequal data.", first.equals( fourth ) );

        // Unequal on null type
        assertFalse( "Expected unequal outputs.", first.equals( null ) );

        // Unequal on metadata
        MetricOutputMetadata outerMeta = MetricOutputMetadata.of( 0,
                                                                  MeasurementUnit.of(),
                                                                  MeasurementUnit.of(),
                                                                  MetricConstants.COEFFICIENT_OF_DETERMINATION );
        ListOfMetricOutput<DoubleScoreOutput> fifth =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.2, metadata ) ), outerMeta );

        assertFalse( "Expected unequal metadata.", fifth.equals( fourth ) );

    }

    /**
     * Tests the {@link ListOfMetricOutput#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Consistent with equals
        ListOfMetricOutput<DoubleScoreOutput> first =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

        ListOfMetricOutput<DoubleScoreOutput> second =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), metadata );

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
     * Tests the {@link ListOfMetricOutput#toString()}.
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

        ListOfMetricOutput<DoubleScoreOutput> listOfOutputs =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     window,
                                                                                                     thresholdOne ) ),
                                                      DoubleScoreOutput.of( 0.2,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     window,
                                                                                                     thresholdTwo ) ),
                                                      DoubleScoreOutput.of( 0.3,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     window,
                                                                                                     thresholdThree ) ) ),
                                       metadata );

        StringBuilder expected = new StringBuilder();
        expected.append( "(DIMENSIONLESS,DIMENSIONLESS,0,BIAS FRACTION,MAIN)" )
                .append( System.lineSeparator() )
                .append( "{([-1000000000-01-01T00:00:00Z, +1000000000-12-31T23:59:59.999999999Z, VALID TIME, PT0S, "
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
