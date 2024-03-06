package wres.metrics.ensemble;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.types.Ensemble;
import wres.datamodel.Slicer;
import wres.datamodel.types.VectorOfDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.metrics.FunctionFactory;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricParameterException;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;
import wres.statistics.generated.MetricName;

/**
 * An concrete implementation of a {@link EnsembleBoxPlot} that plots the ensemble forecast errors (right - left) against 
 * forecast value. A box is constructed for the errors associated with each ensemble forecast where the errors 
 * (whiskers) are mapped to prescribed quantiles (probability thresholds). The function used to map the forecast
 * value may be prescribed.
 * 
 * @author James Brown
 */

public class BoxPlotErrorByForecast extends EnsembleBoxPlot
{

    /**
     * The canonical representation of the metric.
     */

    private final BoxplotMetric metric;

    /**
     * Default dimension for the domain.
     */

    private static final MetricDimension DEFAULT_DOMAIN_DIMENSION = MetricDimension.ENSEMBLE_MEAN;

    /**
     * Default domain mapper function.
     */

    private static final ToDoubleFunction<double[]> DEFAULT_DOMAIN_MAPPER = FunctionFactory.mean();

    /**
     * The dimension associated with the domain axis, which corresponds to a function that is applied to the 
     * forecast values.
     */

    private final MetricDimension domainDimension;

    /**
     * The function used to map the forecast values for the domain axis.
     */

    private final ToDoubleFunction<double[]> domainMapper;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static BoxPlotErrorByForecast of()
    {
        return new BoxPlotErrorByForecast();
    }

    /**
     * Returns an instance.
     * 
     * @param domainDimension the domain axis dimension
     * @param probabilities the probabilities
     * @throws MetricParameterException if the parameters are incorrect
     * @return an instance
     */

    public static BoxPlotErrorByForecast of( MetricDimension domainDimension, VectorOfDoubles probabilities )
    {
        return new BoxPlotErrorByForecast( probabilities, domainDimension );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE;
    }

    @Override
    BoxplotMetric getMetric()
    {
        return this.metric;
    }

    /**
     * Creates a box from an ensemble pair.
     * 
     * @param pair an ensemble pair
     * @return a box
     * @throws MetricCalculationException if the box cannot be constructed
     */

    @Override
    Box getBox( Pair<Double, Ensemble> pair )
    {
        // Get the sorted errors
        List<Double> probs = this.getMetric()
                                 .getQuantilesList();
        double[] sorted = pair.getRight()
                              .getMembers();
        Arrays.sort( sorted );
        double[] sortedErrors = Arrays.stream( sorted )
                                      .map( x -> x - pair.getLeft() )
                                      .toArray();

        // Compute the quantiles
        List<Double> box = probs.stream()
                                .mapToDouble( Double::doubleValue )
                                .map( Slicer.getQuantileFunction( sortedErrors ) )
                                .boxed()
                                .toList();

        double linkedValue = this.domainMapper.applyAsDouble( sorted );

        return Box.newBuilder()
                  .setLinkedValue( linkedValue )
                  .addAllQuantiles( box )
                  .build();
    }

    /**
     * Hidden constructor.
     */

    private BoxPlotErrorByForecast()
    {
        super();

        this.domainDimension = DEFAULT_DOMAIN_DIMENSION;
        this.domainMapper = DEFAULT_DOMAIN_MAPPER;

        this.metric = BoxplotMetric.newBuilder()
                                   .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
                                   .setLinkedValueType( LinkedValueType.valueOf( this.domainDimension.name() ) )
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
     * @param domainDimension the domain axis dimension
     * @throws MetricParameterException if the parameters are incorrect
     */

    private BoxPlotErrorByForecast( VectorOfDoubles probabilities, MetricDimension domainDimension )
    {
        this.validateProbabilities( probabilities );

        if ( Objects.isNull( domainDimension ) )
        {
            throw new MetricParameterException( "Cannot build the box plot of forecast errors by forecast value without "
                                                + "a dimension for the domain axis." );
        }

        this.domainDimension = domainDimension;

        switch ( domainDimension )
        {
            case ENSEMBLE_MEAN -> this.domainMapper = FunctionFactory.mean();
            case ENSEMBLE_MEDIAN -> this.domainMapper =
                    a -> Slicer.getQuantileFunction( a )
                               .applyAsDouble( 0.5 );
            default -> throw new MetricParameterException( "Unsupported dimension for the domain axis of the box plot: "
                                                           + "'"
                                                           + domainDimension
                                                           + "'." );
        }

        LinkedValueType linkedValueType = LinkedValueType.valueOf( this.domainDimension.name() );
        BoxplotMetric.Builder builder = BoxplotMetric.newBuilder()
                                                     .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
                                                     .setLinkedValueType( linkedValueType )
                                                     .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                                     .setMinimum( Double.NEGATIVE_INFINITY )
                                                     .setMaximum( Double.POSITIVE_INFINITY );

        Arrays.stream( probabilities.getDoubles() )
              .forEach( builder::addQuantiles );
        this.metric = builder.build();
    }

}
