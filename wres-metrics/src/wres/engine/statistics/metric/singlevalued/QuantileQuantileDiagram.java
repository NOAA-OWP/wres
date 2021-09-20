package wres.engine.statistics.metric.singlevalued;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.engine.statistics.metric.Diagram;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Compares the quantiles of two samples at a prescribed number (<code>N</code>) of (evenly-spaced) probabilities on the
 * unit interval, namely <code>{1/N+1,...,N/N+1}</code>. If the samples originate from the same probability
 * distribution, the order statistics (and hence the quantiles) should be the same, notwithstanding any sampling error.
 * Uses as many quantiles as the smaller of the number of order statistics (pairs) and the prescribed count.
 * 
 * @author James Brown
 */

public class QuantileQuantileDiagram extends Diagram<Pool<Pair<Double, Double>>, DiagramStatisticOuter>
{

    /**
     * Observed quantiles.
     */

    public static final DiagramMetricComponent OBSERVED_QUANTILES = DiagramMetricComponent.newBuilder()
                                                                                          .setName( DiagramComponentName.OBSERVED_QUANTILES )
                                                                                          .setMinimum( Double.NEGATIVE_INFINITY )
                                                                                          .setMaximum( Double.POSITIVE_INFINITY )
                                                                                          .build();

    /**
     * Predicted quantiles.
     */

    public static final DiagramMetricComponent PREDICTED_QUANTILES = DiagramMetricComponent.newBuilder()
                                                                                           .setName( DiagramComponentName.PREDICTED_QUANTILES )
                                                                                           .setMinimum( Double.NEGATIVE_INFINITY )
                                                                                           .setMaximum( Double.POSITIVE_INFINITY )
                                                                                           .build();

    /**
     * Basic description of the metric.
     */

    public static final DiagramMetric BASIC_METRIC = DiagramMetric.newBuilder()
                                                                  .setName( MetricName.QUANTILE_QUANTILE_DIAGRAM )
                                                                  .build();

    /**
     * Full description of the metric.
     */

    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .addComponents( QuantileQuantileDiagram.OBSERVED_QUANTILES )
                                                            .addComponents( QuantileQuantileDiagram.PREDICTED_QUANTILES )
                                                            .setName( MetricName.QUANTILE_QUANTILE_DIAGRAM )
                                                            .build();

    /**
     * The default number of probabilities at which to compute the order statistics.
     */

    private static final int DEFAULT_PROBABILITY_COUNT = 100;
    
    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( QuantileQuantileDiagram.class );
    
    /**
     * The number of probabilities at which to compute the order statistics.
     */

    private final int probCount;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static QuantileQuantileDiagram of()
    {
        return new QuantileQuantileDiagram();
    }

    /**
     * Returns an instance.
     * 
     * @param probCount the number of quantiles in the diagram
     * @return an instance
     * @throws IllegalArgumentException if the probCount is less than ot equal to zero
     */

    public static QuantileQuantileDiagram of( int probCount )
    {
        return new QuantileQuantileDiagram( probCount );
    }

    @Override
    public DiagramStatisticOuter apply( Pool<Pair<Double, Double>> pairs )
    {
        Objects.requireNonNull( pairs, "Specify non-null input to the '" + this.getName() + "'." );

        int quantileCount = this.probCount;

        // Use the smaller of the number of pairs and the default: no value in more quantiles than order statistics
        int orderStatistics = pairs.get()
                                   .size();
        if ( orderStatistics < quantileCount )
        {
            LOGGER.debug( "While building a quantile-quantile diagram for {}, discovered a smaller number of order "
                          + "statistics ({}) than probabilities requested ({}). Using the number of order statistics.",
                          pairs.getMetadata(),
                          orderStatistics,
                          quantileCount );

            quantileCount = orderStatistics;
        }

        // Determine the number of order statistics to compute
        double[] observedQ = new double[quantileCount];
        double[] predictedQ = new double[quantileCount];

        // Remove non-finite
        double[] sortedLeft = Slicer.getLeftSide( pairs );
        double[] sortedRight = Slicer.getRightSide( pairs );
        sortedLeft = Arrays.stream( sortedLeft )
                           .filter( Double::isFinite )
                           .toArray();
        sortedRight = Arrays.stream( sortedRight )
                            .filter( Double::isFinite )
                            .toArray();

        // Sort in place
        Arrays.sort( sortedLeft );
        Arrays.sort( sortedRight );
        
        DoubleUnaryOperator qLeft = Slicer.getQuantileFunction( sortedLeft );
        DoubleUnaryOperator qRight = Slicer.getQuantileFunction( sortedRight );

        // Compute the order statistics
        for ( int i = 0; i < quantileCount; i++ )
        {
            double prob = ( i + 1.0 ) / ( quantileCount + 1.0 );
            observedQ[i] = qLeft.applyAsDouble( prob );
            predictedQ[i] = qRight.applyAsDouble( prob );
        }

        // Add the units to the quantiles
        DiagramMetricComponent obsWithUnits = QuantileQuantileDiagram.OBSERVED_QUANTILES.toBuilder()
                                                                                        .setUnits( pairs.getMetadata()
                                                                                                    .getMeasurementUnit()
                                                                                                    .toString() )
                                                                                        .build();

        DiagramMetricComponent predWithUnits = QuantileQuantileDiagram.PREDICTED_QUANTILES.toBuilder()
                                                                                          .setUnits( pairs.getMetadata()
                                                                                                      .getMeasurementUnit()
                                                                                                      .toString() )
                                                                                          .build();

        DiagramStatisticComponent oqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( obsWithUnits )
                                         .addAllValues( Arrays.stream( observedQ )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatisticComponent pqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( predWithUnits )
                                         .addAllValues( Arrays.stream( predictedQ )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatistic qqDiagram = DiagramStatistic.newBuilder()
                                                     .addStatistics( oqs )
                                                     .addStatistics( pqs )
                                                     .setMetric( QuantileQuantileDiagram.BASIC_METRIC )
                                                     .build();

        return DiagramStatisticOuter.of( qqDiagram, pairs.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.QUANTILE_QUANTILE_DIAGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    private QuantileQuantileDiagram()
    {
        super();

        this.probCount = DEFAULT_PROBABILITY_COUNT;
    }

    /**
     * Hidden constructor.
     * @param probCount the number of quantiles in the diagram
     * @throws IllegalArgumentException if the probCount is less than ot equal to zero
     */

    private QuantileQuantileDiagram( int probCount )
    {
        super();

        if ( probCount <= 0 )
        {
            throw new IllegalArgumentException( "The number of quantiles in the diagram must exceed zero." );
        }

        this.probCount = probCount;
    }

}
