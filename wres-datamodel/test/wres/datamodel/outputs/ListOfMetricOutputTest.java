package wres.datamodel.outputs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
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
    public void testConstruction()
    {
        assertNotNull( ListOfMetricOutput.of( Collections.emptyList(), null ) );
    }

    /**
     * Tests the iteration cannot lead to mutation of the output list.
     */

    @Test
    public void testIteratorCannotMutate()
    {
        exception.expect( UnsupportedOperationException.class );

        ListOfMetricOutput<DoubleScoreOutput> list =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), null );

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
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), null );

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
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1, metadata ) ), null );

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
