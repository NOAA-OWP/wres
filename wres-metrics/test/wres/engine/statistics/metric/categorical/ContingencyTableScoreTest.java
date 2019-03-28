package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.statistics.MatrixStatistic;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link ContingencyTableScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContingencyTableScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Score used for testing. 
     */

    private ThreatScore cs;

    /**
     * Metadata used for testing.
     */

    private StatisticMetadata meta;

    @Before
    public void setupBeforeEachTest()
    {
        cs = ThreatScore.of();
        meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                              365,
                              MeasurementUnit.of(),
                              MetricConstants.CONTINGENCY_TABLE,
                              MetricConstants.MAIN );        
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
        final double[][] benchmark = new double[][] { { 82.0, 38.0 }, { 23.0, 222.0 } };
        final MatrixStatistic expected = MatrixStatistic.of( benchmark, meta );

        cs.is2x2ContingencyTable( expected, cs );
    }

    /**
     * Checks that input with the correct shape is accepted for a table of arbitrary size.
     * @throws SampleDataException if the input is not accepted
     */

    @Test
    public void testContingencyTableScoreAcceptsCorrectInputForLargeTable()
    {
        final double[][] benchmark = new double[][] { { 82.0, 38.0 }, { 23.0, 222.0 } };
        final MatrixStatistic expected = MatrixStatistic.of( benchmark, meta );

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
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final StatisticMetadata m1 =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS" ) ),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.CONTINGENCY_TABLE,
                                      MetricConstants.MAIN );

        final double[][] benchmark = new double[][] { { 82.0, 38.0 }, { 23.0, 222.0 } };
        final MatrixStatistic expected = MatrixStatistic.of( benchmark,
                                                                  Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                                                 MetricDimension.FALSE_POSITIVES,
                                                                                 MetricDimension.FALSE_NEGATIVES,
                                                                                 MetricDimension.TRUE_NEGATIVES ),
                                                                  m1 );

        final MatrixStatistic actual = cs.getInputForAggregation( input );

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
     * Checks that {@link ContingencyTableScore#getScoreOutputGroup()} returns {@link ScoreGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroupReturnsNone()
    {
        assertTrue( cs.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Checks the output from {@link ContingencyTableScore#getMetadata(MatrixStatistic)} against a benchmark.
     */

    @Test
    public void testGetMetadataReturnsExpectedOutput()
    {
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        final StatisticMetadata expected =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS" ) ),
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
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'THREAT SCORE'." );

        cs.apply( (DichotomousPairs) null );
    }

    /**
     * Checks for an exception on null input when computing the score from an existing contingency table.
     */

    @Test
    public void testExceptionOnNullInputInternal()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'THREAT SCORE'." );

        cs.is2x2ContingencyTable( (MatrixStatistic) null, cs );
    }

    /**
     * Checks for an exception on null input when computing the score from an existing contingency table of 
     * arbitray size.
     */

    @Test
    public void testExceptionOnNullInputInternalForLargeTable()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'THREAT SCORE'." );

        cs.isContingencyTable( (MatrixStatistic) null, cs );
    }

    /**
     * Checks for an exception on receiving an input that is too small.
     */

    @Test
    public void testExceptionOnInputThatIsTooSmall()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Expected an intermediate result with a 2x2 square matrix when computing the "
                                 + "'THREAT SCORE': [1, 1]." );

        cs.is2x2ContingencyTable( MatrixStatistic.of( new double[][] { { 1.0 } }, meta ), cs );
    }

    /**
     * Checks for an exception on receiving an input that is not square.
     */

    @Test
    public void testExceptionOnInputThatIsWrongShape()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Expected an intermediate result with a 2x2 square matrix when computing the "
                                 + "'THREAT SCORE': [2, 3]." );

        cs.is2x2ContingencyTable( MatrixStatistic.of( new double[][] { { 1.0, 1.0, 1.0 }, { 1.0, 1.0, 1.0 } },
                                                              meta ),
                                  cs );
    }

    /**
     * Checks for an exception on receiving an input that is not square for a table of arbitrary size.
     */

    @Test
    public void testExceptionOnInputThatIsWrongShapeForLargeTable()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Expected an intermediate result with a square matrix when computing the "
                                 + "'THREAT SCORE': [2, 3]." );

        cs.isContingencyTable( MatrixStatistic.of( new double[][] { { 1.0, 1.0, 1.0 }, { 1.0, 1.0, 1.0 } },
                                                           meta ),
                               cs );
    }

    /**
     * Checks for an exception on receiving a null metric.
     */

    @Test
    public void testExceptionOnNullMetric()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'THREAT SCORE'." );

        cs.is2x2ContingencyTable( MatrixStatistic.of( new double[][] { { 1.0 } }, meta ), null );
    }

    /**
     * Checks for an exception on receiving a null metric for a contingency table of arbitrary size.
     */

    @Test
    public void testExceptionOnNullMetricForLargeTable()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'THREAT SCORE'." );

        cs.isContingencyTable( MatrixStatistic.of( new double[][] { { 1.0, 1.0 }, { 1.0, 1.0 } }, meta ),
                               null );
    }

}
