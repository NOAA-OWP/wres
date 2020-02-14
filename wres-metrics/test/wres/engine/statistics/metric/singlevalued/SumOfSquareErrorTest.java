package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link SumOfSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SumOfSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link SumOfSquareError}.
     */

    private SumOfSquareError sse;

    @Before
    public void setupBeforeEachTest()
    {
        this.sse = SumOfSquareError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Metadata for the output
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                                    "SQIN",
                                                                                                    "HEFS" ) ),
                                                           input.getRawData().size(),
                                                           MeasurementUnit.of( "CMS" ),
                                                           MetricConstants.SUM_OF_SQUARE_ERROR,
                                                           MetricConstants.MAIN );

        //Check the results
        DoubleScoreStatistic actual = sse.apply( input );

        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 4000039.29, m1 );

        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = sse.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertTrue( sse.getName().equals( MetricConstants.SUM_OF_SQUARE_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( sse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( sse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( sse.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( sse.getCollectionOf().equals( MetricConstants.SUM_OF_SQUARE_ERROR ) );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.apply( null );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.aggregate( null );
    }

}
