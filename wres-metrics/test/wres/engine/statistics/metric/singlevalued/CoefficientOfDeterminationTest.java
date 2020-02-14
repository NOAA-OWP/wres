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
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link CoefficientOfDetermination}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class CoefficientOfDeterminationTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link CoefficientOfDetermination}.
     */

    private CoefficientOfDetermination cod;

    @Before
    public void setupBeforeEachTest()
    {
        this.cod = CoefficientOfDetermination.of();
    }

    @Test
    public void testApply()
    {
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                     MetricConstants.MAIN );

        //Compute normally
        DoubleScoreStatistic actual = cod.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( Math.pow( 0.9999999910148981, 2 ), m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );
 
        DoubleScoreStatistic actual = cod.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertTrue( cod.getName().equals( MetricConstants.COEFFICIENT_OF_DETERMINATION.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( cod.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( cod.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( cod.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( cod.getCollectionOf().equals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) );
    }    

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'COEFFICIENT OF DETERMINATION'." );
        
        cod.apply( null );
    }    

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'COEFFICIENT OF DETERMINATION'." );
        
        cod.aggregate( null );
    }
    
}
