package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link ContingencyTableScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContingencyTableScoreTest
{

    /**
     * Expected warning.
     */

    private static final String SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE =
            "Specify non-null input to the 'THREAT SCORE'.";

    /**
     * Score used for testing. 
     */

    private ThreatScore cs;

    /**
     * Metadata used for testing.
     */

    private StatisticMetadata meta;

    /**
     * Contingency table.
     */

    private Map<MetricConstants, Double> elements;

    @Before
    public void setupBeforeEachTest()
    {
        cs = ThreatScore.of();
        meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                     365,
                                     MeasurementUnit.of(),
                                     MetricConstants.CONTINGENCY_TABLE,
                                     MetricConstants.MAIN );
        elements = new HashMap<>();

        elements.put( MetricConstants.TRUE_POSITIVES, 82.0 );
        elements.put( MetricConstants.TRUE_NEGATIVES, 222.0 );
        elements.put( MetricConstants.FALSE_POSITIVES, 38.0 );
        elements.put( MetricConstants.FALSE_NEGATIVES, 23.0 );
    }

    /**
     * Checks that a {@link ContingencyTableScore#hasRealUnits()} returns <code>false</code> and that input with the 
     * correct shape is accepted.
     * @throws SampleDataException if the input is not accepted
     */

    @Test
    public void testHasRealUnits()
    {
        assertFalse( "The Critical Success Index should not have real units.", cs.hasRealUnits() );
    }

    /**
     * Checks that a {@link ContingencyTableScore} accepts input with the correct shape.
     * @throws SampleDataException if the input is not accepted
     */

    @Test
    public void testContingencyTableScoreAcceptsCorrectInput()
    {
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( this.elements, meta );

        cs.is2x2ContingencyTable( expected, cs );
    }

    /**
     * Checks that input with the correct shape is accepted for a table of arbitrary size.
     * @throws SampleDataException if the input is not accepted
     */

    @Test
    public void testContingencyTableScoreAcceptsCorrectInputForLargeTable()
    {
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( this.elements, meta );

        cs.isContingencyTable( expected, cs );
    }

    /**
     * Checks that {@link ContingencyTableScore#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( cs.getCollectionOf() == MetricConstants.CONTINGENCY_TABLE );
    }

    /**
     * Compares the output from {@link ContingencyTableScore#getInputForAggregation(wres.datamodel.sampledata.pairs.MulticategoryPairs)} 
     * against a benchmark.
     */

    @Test
    public void testGetCollectionInput()
    {
        final SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final StatisticMetadata m1 =
                StatisticMetadata.of( Boilerplate.getSampleMetadata(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.CONTINGENCY_TABLE,
                                      null );

        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( this.elements, m1 );

        final DoubleScoreStatistic actual = cs.getInputForAggregation( input );

        assertTrue( "Unexpected result for the contingency table.", actual.equals( expected ) );
    }

    /**
     * Checks that {@link ContingencyTableScore#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposableReturnsFalse()
    {
        assertFalse( cs.isDecomposable() );
    }

    /**
     * Checks that {@link ContingencyTableScore#getScoreOutputGroup()} returns {@link MetricGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroupReturnsNone()
    {
        assertTrue( cs.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Checks the output from {@link ContingencyTableScore#getMetadata(DoubleScoreStatistic)} against a benchmark.
     */

    @Test
    public void testGetMetadataReturnsExpectedOutput()
    {
        final SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        final StatisticMetadata expected =
                StatisticMetadata.of( Boilerplate.getSampleMetadata(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.THREAT_SCORE,
                                      MetricConstants.MAIN );

        assertTrue( cs.getMetadata( cs.getInputForAggregation( input ) ).equals( expected ) );
    }

    /**
     * Checks for an exception on null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> cs.apply( (SampleData<Pair<Boolean, Boolean>>) null ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on null input when computing the score from an existing contingency table.
     */

    @Test
    public void testExceptionOnNullInputInternal()
    {
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> cs.is2x2ContingencyTable( (DoubleScoreStatistic) null, cs ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on null input when computing the score from an existing contingency table of 
     * arbitray size.
     */

    @Test
    public void testExceptionOnNullInputInternalForLargeTable()
    {
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> cs.isContingencyTable( (DoubleScoreStatistic) null, cs ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on receiving an input that is too small.
     */

    @Test
    public void testExceptionOnInputThatIsTooSmall()
    {

        Map<MetricConstants, Double> table = new HashMap<>();
        table.put( MetricConstants.TRUE_POSITIVES, 82.0 );

        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> cs.is2x2ContingencyTable( DoubleScoreStatistic.of( table, this.meta ), this.cs ) );

        Set<MetricConstants> expected = Set.of( MetricConstants.TRUE_POSITIVES,
                                                MetricConstants.TRUE_NEGATIVES,
                                                MetricConstants.FALSE_POSITIVES,
                                                MetricConstants.FALSE_NEGATIVES );

        String expectedMessage = "Expected an intermediate result with elements "
                                 + expected
                                 + " but found elements [TRUE POSITIVES].";

        assertEquals( expectedMessage, exception.getMessage() );
    }


    /**
     * Checks for an exception on receiving a null metric.
     */

    @Test
    public void testExceptionOnNullMetric()
    {
        Map<MetricConstants, Double> table = new HashMap<>();
        table.put( MetricConstants.TRUE_POSITIVES, 82.0 );

        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> cs.is2x2ContingencyTable( DoubleScoreStatistic.of( table, meta ), null ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on receiving a null metric for a contingency table of arbitrary size.
     */

    @Test
    public void testExceptionOnNullMetricForLargeTable()
    {
        Map<MetricConstants, Double> table = new HashMap<>();
        table.put( MetricConstants.TRUE_POSITIVES, 82.0 );

        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> cs.isContingencyTable( DoubleScoreStatistic.of( table, meta ), null ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

}
