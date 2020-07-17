package wres.engine.statistics.metric.ensemble;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * An concrete implementation of a {@link EnsembleBoxPlot} that plots the ensemble forecast errors (right - left) against 
 * observed value. A box is constructed for the errors associated with each ensemble forecast where the errors (whiskers) 
 * are mapped to prescribed quantiles (probability thresholds).
 * 
 * @author james.brown@hydrosolved.com
 */

public class BoxPlotErrorByObserved extends EnsembleBoxPlot
{

    /**
     * The canonical representation of the metric.
     */

    private final BoxplotMetric metric;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static BoxPlotErrorByObserved of()
    {
        return new BoxPlotErrorByObserved();
    }

    /**
     * Returns an instance.
     * 
     * @param probabilities the probabilities
     * @throws MetricParameterException if the parameters are incorrect
     * @return an instance
     */

    public static BoxPlotErrorByObserved of( VectorOfDoubles probabilities ) throws MetricParameterException
    {
        return new BoxPlotErrorByObserved( probabilities );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE;
    }

    @Override
    Box getBox( Pair<Double, Ensemble> pair )
    {
        //Get the sorted errors
        List<Double> probs = this.getMetric().getQuantilesList();
        double[] sortedErrors =
                Arrays.stream( pair.getRight().getMembers() )
                      .map( x -> x - pair.getLeft() )
                      .sorted()
                      .toArray();

        //Compute the quantiles
        List<Double> box = probs.stream()
                                .mapToDouble( Double::doubleValue )
                                .map( Slicer.getQuantileFunction( sortedErrors ) )
                                .boxed()
                                .collect( Collectors.toList() );

        return Box.newBuilder()
                  .setLinkedValue( pair.getLeft() )
                  .addAllQuantiles( box )
                  .build();
    }

    @Override
    BoxplotMetric getMetric()
    {
        return this.metric;
    }

    /**
     * Hidden constructor.
     */

    private BoxPlotErrorByObserved()
    {
        super();

        this.metric = BoxplotMetric.newBuilder()
                                   .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE )
                                   .setLinkedValueType( LinkedValueType.OBSERVED_VALUE )
                                   .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                   .addAllQuantiles( EnsembleBoxPlot.DEFAULT_PROBABILITIES )
                                   .setMinimum( Double.NEGATIVE_INFINITY )
                                   .setMaximum( Double.POSITIVE_INFINITY )
                                   .build();
    }

    /**
     * Hidden constructor.
     * 
     * @param probabilities the probabilities
     * @throws MetricParameterException if the parameters are incorrect
     */

    private BoxPlotErrorByObserved( VectorOfDoubles probabilities ) throws MetricParameterException
    {
        this.validateProbabilities( probabilities );

        BoxplotMetric.Builder builder = BoxplotMetric.newBuilder()
                                                     .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE )
                                                     .setLinkedValueType( LinkedValueType.OBSERVED_VALUE )
                                                     .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                                     .setMinimum( Double.NEGATIVE_INFINITY )
                                                     .setMaximum( Double.POSITIVE_INFINITY );

        Arrays.stream( probabilities.getDoubles() ).forEach( builder::addQuantiles );
        this.metric = builder.build();
    }

}
