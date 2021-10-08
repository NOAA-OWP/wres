package wres.metrics.singlevalued.univariate;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.pools.Pool;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Score that applies a function to each side of a paired dataset and returns the score components.
 * 
 * @author James Brown
 */

class UnivariateScore implements Function<Pool<Pair<Double, Double>>, DoubleScoreStatistic>
{

    /**
     * The scoring rule.
     */

    private final ToDoubleFunction<VectorOfDoubles> function;

    /**
     * Is true if the score units are the same as the paired units.
     */

    private final boolean pairedUnits;

    /**
     * Basic description of the metric.
     */

    private final DoubleScoreMetric metric;

    /**
     * The left component description (without units).
     */

    private final DoubleScoreMetricComponent left;

    /**
     * The right component description (without units).
     */

    private final DoubleScoreMetricComponent right;

    /**
     * The baseline component description (without units).
     */

    private final DoubleScoreMetricComponent baseline;

    @Override
    public DoubleScoreStatistic apply( Pool<Pair<Double, Double>> pairs )
    {
        Objects.requireNonNull( pairs );

        double leftInner = MissingValues.DOUBLE;
        double rightInner = MissingValues.DOUBLE;

        // Data available?
        List<Pair<Double, Double>> rawPairs = pairs.get();
        if ( !rawPairs.isEmpty() )
        {
            double[] leftDoubles = new double[rawPairs.size()];
            double[] rightDoubles = new double[rawPairs.size()];

            for ( int i = 0; i < leftDoubles.length; i++ )
            {
                leftDoubles[i] = rawPairs.get( i ).getLeft();
                rightDoubles[i] = rawPairs.get( i ).getRight();
            }

            VectorOfDoubles leftVector = VectorOfDoubles.of( leftDoubles );
            VectorOfDoubles rightVector = VectorOfDoubles.of( rightDoubles );


            leftInner = this.getFunction()
                       .applyAsDouble( leftVector );
            rightInner = this.getFunction()
                        .applyAsDouble( rightVector );
        }

        // Empty string for default units
        String units = "";
        if ( this.pairedUnits )
        {
            units = pairs.getMetadata().getMeasurementUnit().getUnit();
        }

        // Add the metric components with the actual units of the pairs
        DoubleScoreStatisticComponent leftComp = DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( this.left.toBuilder()
                                                                                                   .setUnits( units ) )
                                                                              .setValue( leftInner )
                                                                              .build();

        DoubleScoreStatisticComponent rightComp = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( this.right.toBuilder()
                                                                                                     .setUnits( units ) )

                                                                               .setValue( rightInner )
                                                                               .build();

        DoubleScoreStatistic.Builder builder = DoubleScoreStatistic.newBuilder()
                                                                   .setMetric( this.metric )
                                                                   .addStatistics( leftComp )
                                                                   .addStatistics( rightComp );

        // Baseline pairs available?
        if ( pairs.hasBaseline() )
        {
            Pool<Pair<Double, Double>> baselinePairs = pairs.getBaselineData();
            List<Pair<Double, Double>> rawBaselinePairs = baselinePairs.get();

            double baselineInner = MissingValues.DOUBLE;

            if ( !rawBaselinePairs.isEmpty() )
            {
                double[] baselineDoubles = new double[rawBaselinePairs.size()];
                for ( int i = 0; i < baselineDoubles.length; i++ )
                {
                    baselineDoubles[i] = rawBaselinePairs.get( i ).getRight();
                }

                VectorOfDoubles baselineVector = VectorOfDoubles.of( baselineDoubles );
                baselineInner = this.getFunction()
                               .applyAsDouble( baselineVector );
            }

            // Add the metric component with the actual units of the pairs
            DoubleScoreStatisticComponent baselineComp = DoubleScoreStatisticComponent.newBuilder()
                                                                                      .setMetric( this.baseline.toBuilder()
                                                                                                               .setUnits( units ) )

                                                                                      .setValue( baselineInner )
                                                                                      .build();

            builder.addStatistics( baselineComp );
        }

        return builder.build();
    }


    /**
     * Builds the univariate score with a scoring rule.
     * 
     * @param function the scoring rule
     * @param metric the metric description
     * @param a template score metric component from which to derive the l/r/b components
     * @param pairedUnits is true if the units of the score are the same as the pairs
     * @throws NullPointerException if any nullable input is null
     */

    UnivariateScore( ToDoubleFunction<VectorOfDoubles> function,
                     DoubleScoreMetric metric,
                     DoubleScoreMetricComponent template,
                     boolean pairedUnits )
    {
        Objects.requireNonNull( function );
        Objects.requireNonNull( metric );
        Objects.requireNonNull( template );

        this.function = function;
        this.pairedUnits = pairedUnits;
        this.metric = metric;

        this.left = template.toBuilder()
                            .setName( ComponentName.LEFT )
                            .build();

        this.right = template.toBuilder()
                             .setName( ComponentName.RIGHT )
                             .build();

        this.baseline = template.toBuilder()
                                .setName( ComponentName.BASELINE )
                                .build();
    }

    /**
     * @return the function.
     */

    private ToDoubleFunction<VectorOfDoubles> getFunction()
    {
        return this.function;
    }
}
