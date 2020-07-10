package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
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
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link CorrelationPearsons}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class CorrelationPearsonsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link CorrelationPearsons}.
     */

    private CorrelationPearsons rho;

    @Before
    public void setupBeforeEachTest()
    {
        this.rho = CorrelationPearsons.of();
    }

    @Test
    public void testApply()
    {
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                     MetricConstants.MAIN );

        //Compute normally
        DoubleScoreStatisticOuter actual = this.rho.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( 0.9999999910148981 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( CorrelationPearsons.METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    @Test
    public void testAggregate()
    {
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        assertTrue( rho.apply( input ).equals( rho.aggregate( rho.getInputForAggregation( input ) ) ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = rho.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( rho.getName().equals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( rho.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( rho.isSkillScore() );
    }

    @Test
    public void testhasRealUnits()
    {
        assertFalse( rho.hasRealUnits() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( rho.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( rho.getCollectionOf().equals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'PEARSON CORRELATION COEFFICIENT'." );

        rho.apply( null );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'PEARSON CORRELATION COEFFICIENT'." );

        rho.aggregate( null );
    }

}
