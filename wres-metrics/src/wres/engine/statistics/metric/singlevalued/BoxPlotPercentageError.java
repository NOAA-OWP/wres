package wres.engine.statistics.metric.singlevalued;

import java.util.List;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.Slicer;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * A box plot of the errors associated with a pool of single-valued pairs where each error is expressed as a 
 * percentage of the left value.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BoxPlotPercentageError extends Diagram<SampleData<Pair<Double, Double>>, BoxplotStatisticOuter>
{

    /**
     * Default probabilities.
     */

    private static final List<Double> DEFAULT_PROBABILITIES = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

    /**
     * Empty box.
     */

    private static final List<Double> EMPTY_BOX = List.of( Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN );

    /**
     * Function for rounding the errors.
     */

    private static final DoubleUnaryOperator ROUNDER = v -> FunctionFactory.round().apply( v, 8 );

    /**
     * The canonical representation of the metric.
     */

    private final BoxplotMetric metric;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static BoxPlotPercentageError of()
    {
        return new BoxPlotPercentageError();
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public BoxplotStatisticOuter apply( SampleData<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        StatisticMetadata metOut = StatisticMetadata.of( s.getMetadata(),
                                                         s.getRawData().size(),
                                                         MeasurementUnit.of( "%" ),
                                                         this.getID(),
                                                         MetricConstants.MAIN );


        BoxplotStatistic.Builder builder = BoxplotStatistic.newBuilder()
                                                           .setMetric( this.getMetric() );

        // Empty output for empty input
        if ( s.getRawData().isEmpty() )
        {
            // Add an empty box: #62863
            builder.addStatistics( Box.newBuilder().addAllQuantiles( BoxPlotPercentageError.EMPTY_BOX ) );

            return BoxplotStatisticOuter.of( builder.build(), metOut );
        }

        // Get the sorted errors
        List<Double> probs = this.getMetric().getQuantilesList();
        double[] sortedPercentageErrors =
                s.getRawData()
                 .stream()
                 .mapToDouble( a -> ( ( a.getRight() - a.getLeft() ) / a.getLeft() ) * 100 )
                 .sorted()
                 .toArray();

        // Compute the quantiles of the errors at a rounded precision
        List<Double> box = probs.stream()
                                .mapToDouble( Double::doubleValue )
                                .map( Slicer.getQuantileFunction( sortedPercentageErrors ) )
                                .map( BoxPlotPercentageError.ROUNDER )
                                .boxed()
                                .collect( Collectors.toList() );

        BoxplotStatistic statistic = BoxplotStatistic.newBuilder()
                                                     .setMetric( this.getMetric() )
                                                     .addStatistics( Box.newBuilder().addAllQuantiles( box ) )
                                                     .build();

        return BoxplotStatisticOuter.of( statistic, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS;
    }

    /**
     * Returns the metric.
     * 
     * @return the metric
     */

    private BoxplotMetric getMetric()
    {
        return this.metric;
    }

    /**
     * Hidden constructor.
     */

    private BoxPlotPercentageError()
    {
        super();
        this.metric = BoxplotMetric.newBuilder()
                                   .setName( MetricName.BOX_PLOT_OF_PERCENTAGE_ERRORS )
                                   .setLinkedValueType( LinkedValueType.NONE )
                                   .setQuantileValueType( QuantileValueType.ERROR_PERCENT_OF_VERIFYING_VALUE )
                                   .addAllQuantiles( BoxPlotPercentageError.DEFAULT_PROBABILITIES )
                                   .setMinimum( Double.NEGATIVE_INFINITY )
                                   .setMaximum( Double.POSITIVE_INFINITY )
                                   .build();
    }

}
