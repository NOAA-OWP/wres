package wres.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;

/**
 * Calculates a score difference by subtracting the statistic associated with a supplied {@link Score} for a baseline
 * dataset from the corresponding statistic for the main dataset.
 *
 * @param <S> the pool content type
 * @param <T> the pool type
 * @author James Brown
 */
public class DoubleScoreDifference<S, T extends Pool<S>> implements Score<T, DoubleScoreStatisticOuter>
{
    /** The name of the underlying metric. */
    private final MetricConstants metricName;

    /** The underlying score for which the difference should be computed. */
    private final Score<T, DoubleScoreStatisticOuter> score;

    /**
     * Creates an instance.
     *
     * @param <S> the pool content type
     * @param <T> the pool type
     * @param score the score, required
     * @return the difference score
     */

    public static <S, T extends Pool<S>> DoubleScoreDifference<S, T> of( Score<T, DoubleScoreStatisticOuter> score )
    {
        return new DoubleScoreDifference<>( score );
    }

    @Override
    public DoubleScoreStatisticOuter apply( T pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this.score + " DIFFERENCE'." );
        }
        if ( !pool.hasBaseline() )
        {
            throw new PoolException( "Specify a non-null baseline for the '" + this.score + " DIFFERENCE'." );
        }

        DoubleScoreStatisticOuter main = this.score.apply( pool );

        @SuppressWarnings( "unchecked" ) // Safe because it returns the same type as the main type
        T baselinePool = ( T ) pool.getBaselineData();
        DoubleScoreStatisticOuter baseline = this.score.apply( baselinePool );

        List<DoubleScoreStatistic.DoubleScoreStatisticComponent> mainComponents = main.getStatistic()
                                                                                      .getStatisticsList();
        List<DoubleScoreStatistic.DoubleScoreStatisticComponent> baselineComponents = baseline.getStatistic()
                                                                                              .getStatisticsList();

        DoubleScoreMetric.Builder metricBuilder = main.getStatistic()
                                                      .getMetric()
                                                      .toBuilder();

        // Create the new metric name
        String nameString = metricBuilder.getName()
                                         .toString() + "_DIFFERENCE";
        MetricName name = MetricName.valueOf( nameString );
        MetricConstants innerName = MetricConstants.valueOf( nameString );
        metricBuilder.setName( name );

        int count = mainComponents.size();

        List<DoubleScoreStatistic.DoubleScoreStatisticComponent> differenceComponents = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ )
        {
            DoubleScoreStatistic.DoubleScoreStatisticComponent nextMain = mainComponents.get( i );
            DoubleScoreStatistic.DoubleScoreStatisticComponent nextBaseline = baselineComponents.get( i );

            // Clear the optimal value from each component, which is not applicable to a score difference, and adjust
            // the lower and upper limits when they are finite
            DoubleScoreMetric.DoubleScoreMetricComponent adjusted = nextMain.getMetric()
                                                                            .toBuilder()
                                                                            .setOptimum( innerName.getOptimum() )
                                                                            .setMinimum( innerName.getMinimum() )
                                                                            .setMaximum( innerName.getMaximum() )
                                                                            .build();

            DoubleScoreStatistic.DoubleScoreStatisticComponent.Builder differenceComponent
                    = nextMain.toBuilder()
                              .setMetric( adjusted );

            double nextDifference = nextMain.getValue() - nextBaseline.getValue();
            differenceComponent.setValue( nextDifference );

            DoubleScoreStatistic.DoubleScoreStatisticComponent differenceComponentBuilt = differenceComponent.build();
            differenceComponents.add( differenceComponentBuilt );
        }

        DoubleScoreMetric metric = metricBuilder.build();

        DoubleScoreStatistic difference = main.getStatistic()
                                              .toBuilder()
                                              .clearStatistics()
                                              .addAllStatistics( differenceComponents )
                                              .setMetric( metric )
                                              .build();

        return DoubleScoreStatisticOuter.of( difference, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return this.metricName;
    }

    @Override
    public boolean hasRealUnits()
    {
        return this.score.hasRealUnits();
    }

    @Override
    public boolean isDecomposable()
    {
        return this.score.isDecomposable();
    }

    @Override
    public MetricConstants.MetricGroup getScoreOutputGroup()
    {
        return this.score.getScoreOutputGroup();
    }

    /**
     * Construct an instance.
     * @param score the score, not null
     */
    private DoubleScoreDifference( Score<T, DoubleScoreStatisticOuter> score )
    {
        Objects.requireNonNull( score );

        this.score = score;
        this.metricName = MetricConstants.valueOf( score.getMetricName().name() + "_DIFFERENCE" );
    }
}
