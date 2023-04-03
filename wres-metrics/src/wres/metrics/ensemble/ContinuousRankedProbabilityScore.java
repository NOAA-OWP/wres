package wres.metrics.ensemble;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.DecomposableScore;
import wres.metrics.FunctionFactory;
import wres.metrics.ProbabilityScore;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>
 * The Continuous Ranked Probability Score (CRPS) is the square difference between the empirical distribution function
 * of an ensemble forecast and the step function associated with a single-valued observation, integrated over the unit
 * interval. By convention, the CRPS is then averaged over each pair of ensemble forecasts and observations. Optionally,
 * the CRPS may be factored into a three-component decomposition, {@link MetricGroup#CR}.
 * </p>
 * <p>
 * Uses the procedure outlined in Hersbach, H. (2000) Decomposition of the Continuous Ranked Probability Score for
 * Ensemble Prediction Systems. <i>Weather and Forecasting</i>, <b>15</b>(5) pp. 559-570. When the inputs contain
 * ensemble forecasts with a varying number of ensemble members, the pairs are split into groups with an equal number of
 * ensemble members, and the per-forecast CRPS is computed for each group separately. The average CRPS is then computed
 * from the per-forecast CRPS values.
 * </p>
 *
 * @author James Brown
 */
public class ContinuousRankedProbabilityScore extends DecomposableScore<Pool<Pair<Double, Ensemble>>>
        implements ProbabilityScore<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SCORE )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( 1 )
                                                                                    .setOptimum( 0 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( ContinuousRankedProbabilityScore.MAIN )
                                                                    .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SCORE )
                                                                    .build();

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( ContinuousRankedProbabilityScore.class );

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static ContinuousRankedProbabilityScore of()
    {
        return new ContinuousRankedProbabilityScore();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Double, Ensemble>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        LOGGER.trace( "Found {} pairs in the input to the {} for '{}'.",
                      pool.get().size(),
                      this.getName(),
                      pool.getMetadata() );

        // Slice the data into groups with an equal number of ensemble members
        Map<Integer, List<Pair<Double, Ensemble>>> grouped =
                Slicer.filterByRightSize( pool.get() );

        // CRPS, currently without decomposition
        // TODO: implement the decomposition
        double[] crps = new double[1];
        for ( Map.Entry<Integer, List<Pair<Double, Ensemble>>> nextGroup : grouped.entrySet() )
        {
            int count = nextGroup.getKey();
            List<Pair<Double, Ensemble>> pairs = nextGroup.getValue();
            double[] crpsSum = this.getSumCRPS( pairs, count );
            crps[0] += crpsSum[0]; // Main score in index 0
        }

        if ( !Double.isFinite( crps[0] ) )
        {
            LOGGER.trace( "Found a non-finite value of {} for the {} at '{}'.",
                          crps[0],
                          this.getName(),
                          pool.getMetadata() );
        }

        // Compute the average (implicitly weighted by the number of pairs in each group)
        crps[0] = FunctionFactory.finiteOrMissing().applyAsDouble( crps[0] / pool.get().size() );

        String units = pool.getMetadata()
                           .getMeasurementUnit()
                           .toString();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric(
                                                                                       ContinuousRankedProbabilityScore.MAIN.toBuilder()
                                                                                                                            .setUnits(
                                                                                                                                    units ) )
                                                                               .setValue( crps[0] )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE;
    }

    @Override
    public boolean isProper()
    {
        return true;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Constructor.
     */

    ContinuousRankedProbabilityScore()
    {
        super();
    }

    /**
     * <p>Returns the sum of the CRPS values for the individual pairs, without any decomposition. Uses the procedure
     * outlined in Hersbach, H. (2000). Requires an equal number of ensemble members in each pair.
     *
     * <p>TODO: implement the decomposition
     *
     * @param pairs the pairs
     * @param memberCount the number of ensemble members
     * @return the mean CRPS, with decomposition if required
     */

    private double[] getSumCRPS( List<Pair<Double, Ensemble>> pairs, int memberCount )
    {
        double totCRPS = 0.0;

        // Form the sorted pairs: #93061
        List<double[]> sortedPairs = new ArrayList<>();
        for ( Pair<Double, Ensemble> nextPair : pairs )
        {
            //Combine and sort forecast
            double[] sorted = new double[memberCount + 1];
            sorted[0] = nextPair.getLeft();
            System.arraycopy( nextPair.getRight().getMembers(), 0, sorted, 1, memberCount );
            Arrays.sort( sorted, 1, memberCount + 1 );
            sortedPairs.add( sorted );
        }

        //Iterate through the member positions and determine the mean alpha and beta      
        BiConsumer<double[], Incrementer> summer = ContinuousRankedProbabilityScore.sumAlphaBeta();
        for ( int i = 0; i < memberCount + 1; i++ )
        {
            Incrementer incrementer = new Incrementer( i, memberCount );
            for ( double[] nextSorted : sortedPairs )
            {
                //Increment
                summer.accept( nextSorted, incrementer );
            }

            totCRPS += incrementer.totCRPS;
        }

        return new double[] { totCRPS };
    }

    /**
     * Returns a consumer that increments the parameters within an {@link Incrementer} for each input pair with sorted
     * forecasts. 
     *
     * @return a consumer that increments the parameters in the {@link Incrementer} for each input pair
     */

    private static BiConsumer<double[], Incrementer> sumAlphaBeta()
    {
        //Handle three possibilities
        BiConsumer<double[], Incrementer> low = sumAlphaBetaLow();
        BiConsumer<double[], Incrementer> middle = sumAlphaBetaMiddle();
        BiConsumer<double[], Incrementer> high = sumAlphaBetaHigh();
        return ( pair, inc ) -> {
            if ( inc.member == 0 )
            {
                low.accept( pair, inc );
            }
            //Deal with high outlier: case 2
            else if ( inc.member == inc.totalMembers )
            {
                high.accept( pair, inc );
            }
            //Deal with remaining 3 cases, for 0 < i < N
            else
            {
                middle.accept( pair, inc );
            }
        };
    }

    /**
     * Returns a consumer that increments the parameters within an {@link Incrementer} for each input pair with sorted
     * forecasts. Appropriate for low outliers, where the observation falls below the lowest member.
     *
     * @return a consumer that increments the parameters in the {@link Incrementer} for each input pair
     */

    private static BiConsumer<double[], Incrementer> sumAlphaBetaLow()
    {
        return ( pair, inc ) -> {
            //Deal with low outlier: case 1
            if ( pair[0] < pair[1] )
            {
                final double nextBeta = pair[1] - pair[0];
                //                    inc.betaSum += nextBeta; //Alpha unchanged
                inc.totCRPS += ( nextBeta * inc.invProbSquared );
            }
        };
    }

    /**
     * Returns a consumer that increments the parameters within an {@link Incrementer} for each input pair with sorted
     * forecasts. Appropriate where the observation falls within the ensemble forecast distribution.
     *
     * @return a consumer that increments the parameters in the {@link Incrementer} for each input pair
     */

    private static BiConsumer<double[], Incrementer> sumAlphaBetaMiddle()
    {
        //Hersbach 2000 has an error in table/eqn (26) and should read >= for the first entry in table, so that 
        //the alpha is incremented when the observation is exactly equal to the upper bound of the interval. 
        //Likewise the third entry should be <= to include the beta when the observation is exactly equal to the 
        //lower bound of the interval
        return ( pair, inc ) -> {
            //Case 3: observed exceeds ith + 1
            if ( pair[0] >= pair[inc.member + 1] ) //Correction to Hersbach
            {
                final double nextAlpha = pair[inc.member + 1] - pair[inc.member];
                inc.totCRPS += nextAlpha * inc.probSquared;
            } //Case 4: observed falls below ith
            else if ( pair[0] <= pair[inc.member] ) //Correction to Hersbach
            {
                final double nextBeta = pair[inc.member + 1] - pair[inc.member];
                inc.totCRPS += nextBeta * inc.invProbSquared;
            } //Case 5: observed falls between ith and ith+1
            else if ( pair[0] > pair[inc.member] && pair[0] < pair[inc.member + 1] )
            {
                final double nextAlpha = pair[0] - pair[inc.member];
                final double nextBeta = pair[inc.member + 1] - pair[0];
                inc.totCRPS += ( ( nextAlpha * inc.probSquared ) + ( nextBeta * inc.invProbSquared ) );
            }
        };
    }

    /**
     * Returns a consumer that increments the parameters within an {@link Incrementer} for each input pair with sorted
     * forecasts. Appropriate for high outliers, where the observation falls above the highest member.
     *
     * @return a consumer that increments the parameters in the {@link Incrementer} for each input pair
     */

    private static BiConsumer<double[], Incrementer> sumAlphaBetaHigh()
    {
        return ( pair, inc ) -> {
            //Deal with high outlier: case 2
            if ( pair[0] > pair[inc.member] )
            {
                final double nextAlpha = pair[0] - pair[inc.member];
                //                inc.alphaSum += nextAlpha; //Beta unchanged
                inc.totCRPS += ( nextAlpha * inc.probSquared );
            }
        };
    }

    /**
     * Class to increment CRPS components.
     */

    private static class Incrementer
    {
        /**
         * Member number.
         */

        private final int member;

        /**
         * Total number of members.
         */

        private final int totalMembers;

        /**
         * Probability squared.
         */

        private final double probSquared;

        /**
         * Inverse probability squared.
         */

        private final double invProbSquared;

        /**
         * The incremented total CRPS
         */

        private double totCRPS = 0.0;

        /**
         * Construct the incrementer.
         *
         * @param member the member position
         * @param totalMembers the total number of members
         */

        private Incrementer( int member, int totalMembers )
        {
            this.member = member;
            this.totalMembers = totalMembers;
            double prob = ( ( double ) member ) / totalMembers;
            this.probSquared = Math.pow( prob, 2 );
            this.invProbSquared = Math.pow( 1.0 - prob, 2 );
        }

    }

}
