package wres.metrics.ensemble;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.metrics.Diagram;
import wres.metrics.MetricParameterException;
import wres.metrics.singlevalued.QuantileQuantileDiagram;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Creates a quantile-quantile curve for each trace in an ensemble. Uses as many quantiles as the smaller of the number
 * of order statistics and the {@link #DEFAULT_PROBABILITY_COUNT}.
 * 
 * @author James Brown
 */

public class EnsembleQuantileQuantileDiagram extends Diagram<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter>
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
                                                                  .setName( MetricName.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM )
                                                                  .build();

    /**
     * Full description of the metric.
     */

    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .addComponents( EnsembleQuantileQuantileDiagram.OBSERVED_QUANTILES )
                                                            .addComponents( EnsembleQuantileQuantileDiagram.PREDICTED_QUANTILES )
                                                            .setName( MetricName.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM )
                                                            .build();

    /**
     * The default number of probabilities at which to compute the order statistics.
     */

    private static final int DEFAULT_PROBABILITY_COUNT = 100;

    /**
     * Single-valued quantile-quantile diagram.
     */

    private final QuantileQuantileDiagram qqDiagram;

    /**
     * @return an instance with the {@link #DEFAULT_PROBABILITY_COUNT }
     */

    public static EnsembleQuantileQuantileDiagram of()
    {
        return new EnsembleQuantileQuantileDiagram( EnsembleQuantileQuantileDiagram.DEFAULT_PROBABILITY_COUNT );
    }

    /**
     * @param count the number of quantiles in the diagram
     * @return an instance with the prescribed number of quantiles
     * @throws MetricParameterException if the number of quantiles is less than one
     */

    public static EnsembleQuantileQuantileDiagram of( int count )
    {
        return new EnsembleQuantileQuantileDiagram( count );
    }

    @Override
    public DiagramStatisticOuter apply( Pool<Pair<Double, Ensemble>> pairs )
    {
        Objects.requireNonNull( pairs );

        // Find the unique labels across ensemble members
        Set<String> labels = new TreeSet<>();
        for ( Pair<Double, Ensemble> pair : pairs.get() )
        {
            String[] nextLabels = pair.getRight()
                                      .getLabels()
                                      .getLabels();

            Arrays.stream( nextLabels ).forEach( labels::add );
        }

        DiagramStatistic.Builder qqBuilder = DiagramStatistic.newBuilder()
                                                             .setMetric( EnsembleQuantileQuantileDiagram.BASIC_METRIC );

        // Build a quantile-quantile-curve per label
        for ( String ensembleName : labels )
        {
            // Function that extracts the relevant member
            Function<Pair<Double, Ensemble>, Pair<Double, Double>> transformer = this.getTransformer( ensembleName );

            // Create the pool of pairs for the relevant member
            Pool<Pair<Double, Double>> transformed = PoolSlicer.transform( pairs, transformer );

            // Create the qq diagram
            DiagramStatisticOuter qq = this.qqDiagram.apply( transformed );
            List<DiagramStatisticComponent> statistics = qq.getData().getStatisticsList();

            // Add the qualifying names to the components and then add to the qq diagram
            for ( DiagramStatisticComponent next : statistics )
            {
                DiagramStatisticComponent named = next.toBuilder()
                                                      .setName( ensembleName )
                                                      .build();
                qqBuilder.addStatistics( named );
            }
        }

        return DiagramStatisticOuter.of( qqBuilder.build(), pairs.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * @param ensembleName the ensemble name.
     * @return a transformer the transforms from an ensemble pairing to a single-valued pairing.
     */

    private Function<Pair<Double, Ensemble>, Pair<Double, Double>> getTransformer( String ensembleName )
    {
        return pair -> {
            Ensemble ensemble = pair.getRight();
            double member = Double.NaN;

            // Does the label exist for this pair?
            if ( ensemble.getLabels().hasLabel( ensembleName ) )
            {
                member = ensemble.getMember( ensembleName );
            }

            return Pair.of( pair.getLeft(), member );
        };
    }

    /**
     * Hidden constructor.
     * @param count the count of probabilities
     * @throws MetricParameterException if the number of quantiles is less than one
     */

    private EnsembleQuantileQuantileDiagram( int count )
    {
        if ( count < 1 )
        {
            throw new MetricParameterException( "The number of quantiles must be greater than zero: " + count + "." );
        }

        this.qqDiagram = QuantileQuantileDiagram.of( count );
    }

}
