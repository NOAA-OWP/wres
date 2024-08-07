package wres.metrics.categorical;

import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.units.Units;
import wres.metrics.Collectable;
import wres.metrics.Metric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;

/**
 * <p>
 * Base class for a contingency table. A contingency table compares the number of predictions and observations 
 * associated with each of the N possible outcomes of an N-category variable. The rows of the contingency
 * table store the number of predicted outcomes and the columns store the number of observed outcomes.
 * </p>
 *
 * @author James Brown
 */

public class ContingencyTable implements Metric<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter>,
                                         Collectable<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{
    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.CONTINGENCY_TABLE )
                                                                          .build();

    /**
     * True positives.
     */

    public static final DoubleScoreMetricComponent TRUE_POSITIVES =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.CONTINGENCY_TABLE.getMinimum() )
                                      .setMaximum( MetricConstants.CONTINGENCY_TABLE.getMaximum() )
                                      .setOptimum( MetricConstants.CONTINGENCY_TABLE.getOptimum() )
                                      .setName( MetricName.TRUE_POSITIVES )
                                      .setUnits( Units.COUNT )
                                      .build();

    /**
     * False positives.
     */

    public static final DoubleScoreMetricComponent FALSE_POSITIVES = DoubleScoreMetricComponent.newBuilder()
                                                                                               .setMinimum( 0 )
                                                                                               .setMaximum( Double.POSITIVE_INFINITY )
                                                                                               .setName( MetricName.FALSE_POSITIVES )
                                                                                               .setUnits( Units.COUNT )
                                                                                               .build();
    /**
     * True negatives.
     */

    public static final DoubleScoreMetricComponent TRUE_NEGATIVES = DoubleScoreMetricComponent.newBuilder()
                                                                                              .setMinimum( 0 )
                                                                                              .setMaximum( Double.POSITIVE_INFINITY )
                                                                                              .setName( MetricName.TRUE_NEGATIVES )
                                                                                              .setUnits( Units.COUNT )
                                                                                              .build();

    /**
     * False negatives.
     */

    public static final DoubleScoreMetricComponent FALSE_NEGATIVES = DoubleScoreMetricComponent.newBuilder()
                                                                                               .setMinimum( 0 )
                                                                                               .setMaximum( Double.POSITIVE_INFINITY )
                                                                                               .setName( MetricName.FALSE_NEGATIVES )
                                                                                               .setUnits( Units.COUNT )
                                                                                               .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( TRUE_POSITIVES )
                                                                    .addComponents( FALSE_POSITIVES )
                                                                    .addComponents( FALSE_NEGATIVES )
                                                                    .addComponents( TRUE_NEGATIVES )
                                                                    .setName( MetricName.CONTINGENCY_TABLE )
                                                                    .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static ContingencyTable of()
    {
        return new ContingencyTable();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Boolean, Boolean>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }
        final int outcomes = 2;
        final double[][] returnMe = new double[outcomes][outcomes];

        // Function that returns the index within the contingency table to increment
        final Consumer<Pair<Boolean, Boolean>> f = a -> {
            boolean left = a.getLeft();
            boolean right = a.getRight();

            // True positives aka hits
            if ( left && right )
            {
                returnMe[0][0] += 1;
            }
            // True negatives 
            else if ( !left && !right )
            {
                returnMe[1][1] += 1;
            }
            // False positives aka false alarms
            else if ( !left )
            {
                returnMe[0][1] += 1;
            }
            // False negatives aka misses
            else
            {
                returnMe[1][0] += 1;
            }
        };

        // Increment the count in a serial stream as the lambda is stateful
        pool.get()
            .forEach( f );

        // Name the outcomes for a 2x2 contingency table
        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( returnMe[0][0] ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( returnMe[0][1] ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( returnMe[1][0] ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( returnMe[1][1] ) )
                                    .build();

        return DoubleScoreStatisticOuter.of( table, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.CONTINGENCY_TABLE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return getMetricName().toString();
    }

    @Override
    public DoubleScoreStatisticOuter applyIntermediate( DoubleScoreStatisticOuter output,
                                                        Pool<Pair<Boolean, Boolean>> pool )
    {
        Objects.requireNonNull( output );

        return output;
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediate( Pool<Pair<Boolean, Boolean>> input )
    {
        return this.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.CONTINGENCY_TABLE;
    }

    /**
     * Hidden constructor.
     */

    ContingencyTable()
    {
    }

}
