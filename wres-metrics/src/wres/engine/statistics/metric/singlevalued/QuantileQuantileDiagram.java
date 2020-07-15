package wres.engine.statistics.metric.singlevalued;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;

import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
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
 * 
 * @author james.brown@hydrosolved.com
 */

public class QuantileQuantileDiagram extends Diagram<SampleData<Pair<Double, Double>>, DiagramStatisticOuter>
{

    /**
     * Canonical representation of the metric.
     */

    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .addComponents( DiagramMetricComponent.newBuilder()
                                                                                                  .setName( DiagramComponentName.OBSERVED_QUANTILES )
                                                                                                  .setMinimum( Double.NEGATIVE_INFINITY )
                                                                                                  .setMaximum( Double.POSITIVE_INFINITY ) )
                                                            .addComponents( DiagramMetricComponent.newBuilder()
                                                                                                  .setName( DiagramComponentName.PREDICTED_QUANTILES )
                                                                                                  .setMinimum( Double.NEGATIVE_INFINITY )
                                                                                                  .setMaximum( Double.POSITIVE_INFINITY ) )
                                                            .setName( MetricName.QUANTILE_QUANTILE_DIAGRAM )
                                                            .build();

    /**
     * The default number of probabilities at which to compute the order statistics.
     */

    private static final int DEFAULT_PROBABILITY_COUNT = 1000;

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
    public DiagramStatisticOuter apply( SampleData<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        //Determine the number of order statistics to compute
        double[] observedQ = new double[this.probCount];
        double[] predictedQ = new double[this.probCount];

        //Get the ordered data
        double[] sortedLeft = Slicer.getLeftSide( s );
        double[] sortedRight = Slicer.getRightSide( s );
        Arrays.sort( sortedLeft );
        Arrays.sort( sortedRight );
        DoubleUnaryOperator qLeft = Slicer.getQuantileFunction( sortedLeft );
        DoubleUnaryOperator qRight = Slicer.getQuantileFunction( sortedRight );

        //Compute the order statistics
        for ( int i = 0; i < this.probCount; i++ )
        {
            double prob = ( i + 1.0 ) / ( this.probCount + 1.0 );
            observedQ[i] = qLeft.applyAsDouble( prob );
            predictedQ[i] = qRight.applyAsDouble( prob );
        }

        DiagramStatisticComponent oqs =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.OBSERVED_QUANTILES )
                                         .addAllValues( Arrays.stream( observedQ )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatisticComponent pqs =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PREDICTED_QUANTILES )
                                         .addAllValues( Arrays.stream( predictedQ )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatistic qqDiagram = DiagramStatistic.newBuilder()
                                                     .addStatistics( oqs )
                                                     .addStatistics( pqs )
                                                     .setMetric( QuantileQuantileDiagram.METRIC )
                                                     .build();

        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                      this.getID(),
                                      MetricConstants.MAIN,
                                      this.hasRealUnits(),
                                      s.getRawData().size(),
                                      null );
        return DiagramStatisticOuter.of( qqDiagram, metOut );
    }

    @Override
    public MetricConstants getID()
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
