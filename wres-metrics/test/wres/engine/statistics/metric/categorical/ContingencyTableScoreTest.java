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
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

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

    private DoubleScoreStatistic table;

    /**
     * Invalid contingency table.
     */

    private DoubleScoreStatistic invalidTable;

    @Before
    public void setupBeforeEachTest()
    {
        this.cs = ThreatScore.of();
        this.meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                          365,
                                          MeasurementUnit.of(),
                                          MetricConstants.CONTINGENCY_TABLE,
                                          MetricConstants.MAIN );

        this.table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                 .setValue( 82.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_POSITIVES )
                                                                                 .setValue( 38.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_NEGATIVES )
                                                                                 .setValue( 23.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_NEGATIVES )
                                                                                 .setValue( 222.0 ) )
                                    .build();

        this.invalidTable = DoubleScoreStatistic.newBuilder()
                                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                             .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                             .setValue( 82.0 ) )
                                                .build();
    }

    /**
     * Checks that a {@link ContingencyTableScore#hasRealUnits()} returns <code>false</code> and that input with the 
     * correct shape is accepted.
     * @throws SampleDataException if the input is not accepted
     */

    @Test
    public void testHasRealUnits()
    {
        assertFalse( "The Critical Success Index should not have real units.", this.cs.hasRealUnits() );
    }

    /**
     * Checks that a {@link ContingencyTableScore} accepts input with the correct shape.
     * @throws SampleDataException if the input is not accepted
     */

    @Test
    public void testContingencyTableScoreAcceptsCorrectInput()
    {
        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( this.table, meta );

        this.cs.is2x2ContingencyTable( expected, this.cs );
    }

    /**
     * Checks that input with the correct shape is accepted for a table of arbitrary size.
     * @throws SampleDataException if the input is not accepted
     */

    @Test
    public void testContingencyTableScoreAcceptsCorrectInputForLargeTable()
    {
        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( this.table, meta );

        this.cs.isContingencyTable( expected, this.cs );
    }

    /**
     * Checks that {@link ContingencyTableScore#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( this.cs.getCollectionOf() == MetricConstants.CONTINGENCY_TABLE );
    }

    /**
     * Compares the output from {@link ContingencyTableScore#getInputForAggregation(SampleData)} 
     * against a benchmark.
     */

    @Test
    public void testGetCollectionInput()
    {
        final SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final StatisticMetadata m1 =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS" ) ),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.CONTINGENCY_TABLE,
                                      null );

        final DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( this.table, m1 );

        final DoubleScoreStatisticOuter actual = this.cs.getInputForAggregation( input );

        assertEquals( "Unexpected result for the contingency table.", expected, actual );
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
     * Checks the output from {@link ContingencyTableScore#getMetadata(DoubleScoreStatisticOuter)} against a benchmark.
     */

    @Test
    public void testGetMetadataReturnsExpectedOutput()
    {
        final SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        final StatisticMetadata expected =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS" ) ),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.THREAT_SCORE,
                                      MetricConstants.MAIN );

        assertTrue( this.cs.getMetadata( this.cs.getInputForAggregation( input ) ).equals( expected ) );
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
                              () -> cs.is2x2ContingencyTable( (DoubleScoreStatisticOuter) null, cs ) );

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
                              () -> cs.isContingencyTable( (DoubleScoreStatisticOuter) null, cs ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on receiving an input that is too small.
     */

    @Test
    public void testExceptionOnInputThatIsTooSmall()
    {
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> cs.is2x2ContingencyTable( DoubleScoreStatisticOuter.of( this.invalidTable,
                                                                                            this.meta ),
                                                              this.cs ) );

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
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> this.cs.is2x2ContingencyTable( DoubleScoreStatisticOuter.of( this.invalidTable,
                                                                                                 this.meta ),
                                                                   null ) );

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
                              () -> this.cs.isContingencyTable( DoubleScoreStatisticOuter.of( this.invalidTable,
                                                                                              this.meta ),
                                                                null ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

}
