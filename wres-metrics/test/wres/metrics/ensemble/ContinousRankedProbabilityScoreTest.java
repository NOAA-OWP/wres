package wres.metrics.ensemble;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ContinuousRankedProbabilityScore}.
 * 
 * @author James Brown
 */
public final class ContinousRankedProbabilityScoreTest
{
    /**
     * Default instance of a {@link ContinuousRankedProbabilityScore}.
     */

    private ContinuousRankedProbabilityScore crps;

    @Before
    public void setupBeforeEachTest()
    {
        this.crps = ContinuousRankedProbabilityScore.of();
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * where the input contains no missing data.
     */

    @Test
    public void testApplyWithNoMissings()
    {
        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 23, 54 ) ) );
        pairs.add( Pair.of( 21.4, Ensemble.of( 19, 16, 57, 23, 9 ) ) );
        pairs.add( Pair.of( 32.1, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        pairs.add( Pair.of( 47.0, Ensemble.of( 12, 54, 23, 54, 78 ) ) );
        pairs.add( Pair.of( 12.1, Ensemble.of( 9, 8, 5, 6, 12 ) ) );
        pairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12, 34, 10 ) ) );
        Pool<Pair<Double, Ensemble>> input = Pool.of( pairs, PoolMetadata.of() );

        //Metadata for the output
        PoolMetadata m1 = PoolMetadata.of();

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        // Units for the pairs are undeclared, which means MeasurementUnit.DIMENSIONLESS
        DoubleScoreMetricComponent expectedMetricComponent = ContinuousRankedProbabilityScore.MAIN.toBuilder()
                                                                                                  .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                                  .build();

        DoubleScoreStatisticComponent expectedStatisticComponent = DoubleScoreStatisticComponent.newBuilder()
                                                                                                .setMetric( expectedMetricComponent )
                                                                                                .setValue( 7.63 )
                                                                                                .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                         .addStatistics( expectedStatisticComponent )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * where the input contains missing data.
     */

    @Test
    public void testApplyWithMissings()
    {

        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 34.2, 23, 54 ) ) );
        pairs.add( Pair.of( 21.4, Ensemble.of( 19, 16, 57, 23, 9 ) ) );
        pairs.add( Pair.of( 32.1, Ensemble.of( 23, 54, 23, 12, 32, 45.3, 67.1 ) ) );
        pairs.add( Pair.of( 47.0, Ensemble.of( 12, 54, 23, 54 ) ) );
        pairs.add( Pair.of( 12.0, Ensemble.of( 9, 8, 5 ) ) );
        pairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12 ) ) );
        Pool<Pair<Double, Ensemble>> input = Pool.of( pairs, PoolMetadata.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        // Units for the pairs are undeclared, which means MeasurementUnit.DIMENSIONLESS
        DoubleScoreMetricComponent expectedMetricComponent = ContinuousRankedProbabilityScore.MAIN.toBuilder()
                                                                                                  .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                                  .build();

        DoubleScoreStatisticComponent expectedStatisticComponent = DoubleScoreStatisticComponent.newBuilder()
                                                                                                .setMetric( expectedMetricComponent )
                                                                                                .setValue( 8.734401927437641 )
                                                                                                .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                            .addStatistics( expectedStatisticComponent )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );

    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * where the observation falls below the lowest member.
     */

    @Test
    public void testApplyObsMissesLow()
    {
        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 8.0, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        Pool<Pair<Double, Ensemble>> input = Pool.of( pairs, PoolMetadata.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        // Units for the pairs are undeclared, which means MeasurementUnit.DIMENSIONLESS
        DoubleScoreMetricComponent expectedMetricComponent = ContinuousRankedProbabilityScore.MAIN.toBuilder()
                                                                                                  .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                                  .build();
        DoubleScoreStatisticComponent expectedStatisticComponent = DoubleScoreStatisticComponent.newBuilder()
                                                                                                .setMetric( expectedMetricComponent )
                                                                                                .setValue( 13.36 )
                                                                                                .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                            .addStatistics( expectedStatisticComponent )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * for a scenario where the observed value overlaps one ensemble member. This exposes a mistake in the Hersbach 
     * (2000) paper where rows 1 and 3 of table/eqn. 26 should be inclusive bounds.
     */

    @Test
    public void testApplyObsEqualsMember()
    {

        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 32.0, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        Pool<Pair<Double, Ensemble>> input = Pool.of( pairs, PoolMetadata.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        // Units for the pairs are undeclared, which means MeasurementUnit.DIMENSIONLESS
        DoubleScoreMetricComponent expectedMetricComponent = ContinuousRankedProbabilityScore.MAIN.toBuilder()
                                                                                                  .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                                  .build();

        DoubleScoreStatisticComponent expectedStatisticComponent = DoubleScoreStatisticComponent.newBuilder()
                                                                                                .setMetric( expectedMetricComponent )
                                                                                                .setValue( 4.56 )
                                                                                                .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                            .addStatistics( expectedStatisticComponent )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }


    /**
     * Validates the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} when supplied with no 
     * data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Ensemble>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE.toString(), crps.getMetricNameString() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( crps.isDecomposable() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( crps.isSkillScore() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#getScoreOutputGroup()} returns the result provided on 
     * construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.crps.getScoreOutputGroup() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertTrue( crps.isProper() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertTrue( crps.isStrictlyProper() );
    }

    /**
     * Tests for an expected exception on calling {@link ContinuousRankedProbabilityScore#apply(Pool)} with 
     * null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class, () -> this.crps.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.crps.getMetricNameString() + "'.", actual.getMessage() );
    }
}
