package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;

/**
 * Tests the {@link SampleSize}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SampleSizeTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Constructs a {@link SampleSize} and compares the actual result to the expected result. Also, checks the 
     * parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testSampleSize() throws MetricParameterException
    {
        //Obtain the factories

        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                           input.getRawData().size(),
                                                           MeasurementUnit.of(),
                                                           MetricConstants.SAMPLE_SIZE,
                                                           MetricConstants.MAIN );
        
        //Build the metric
        SampleSize<SingleValuedPairs> ss = SampleSize.of();

        //Check the results
        DoubleScoreStatistic actual = ss.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( (double) input.getRawData().size(), m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for the Sample Size.",
                    ss.getName().equals( MetricConstants.SAMPLE_SIZE.toString() ) );
        assertTrue( "The Sample Size is not decomposable.", !ss.isDecomposable() );
        assertTrue( "The Sample Size is not a skill score.", !ss.isSkillScore() );
        assertTrue( "The Sample Size cannot be decomposed.", ss.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Constructs a {@link SampleSize} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptions() throws MetricParameterException
    {
        //Build the metric
        SampleSize<SingleValuedPairs> ss = SampleSize.of();

        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'SAMPLE SIZE'." );
        ss.apply( null );

    }


}
