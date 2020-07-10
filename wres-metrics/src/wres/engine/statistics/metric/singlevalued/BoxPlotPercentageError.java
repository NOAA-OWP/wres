package wres.engine.statistics.metric.singlevalued;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.BoxplotStatistic;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.FunctionFactory;

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

    private static final VectorOfDoubles DEFAULT_PROBABILITIES = VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

    /**
     * Empty box.
     */

    private static final VectorOfDoubles EMPTY_BOX =
            VectorOfDoubles.of( Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN );

    /**
     * Function for rounding the errors.
     */

    private static final DoubleUnaryOperator ROUNDER = v -> FunctionFactory.round().apply( v, 8 );

    /**
     * The probabilities.
     */

    private final VectorOfDoubles probabilities;

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
    public BoxplotStatisticOuter apply( final SampleData<Pair<Double, Double>> s )
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

        // Empty output for empty input
        if ( s.getRawData().isEmpty() )
        {
            // Add an empty box: #62863
            BoxplotStatistic emptyBox = BoxplotStatistic.of( DEFAULT_PROBABILITIES, EMPTY_BOX, metOut );
            return BoxplotStatisticOuter.of( Collections.singletonList( emptyBox ), metOut );
        }

        // Get the sorted percentage errors
        double[] sortedPercentageErrors =
                s.getRawData()
                 .stream()
                 .mapToDouble( a -> ( ( a.getRight() - a.getLeft() ) / a.getLeft() ) * 100 )
                 .sorted()
                 .toArray();

        // Compute the quantiles of the errors at a rounded precision
        double[] box =
                Arrays.stream( this.getProbabilities().getDoubles() )
                      .map( Slicer.getQuantileFunction( sortedPercentageErrors ) )
                      .map( ROUNDER )
                      .toArray();

        BoxplotStatistic statistic = BoxplotStatistic.of( this.getProbabilities(),
                                                          VectorOfDoubles.of( box ),
                                                          MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                                          metOut );

        return BoxplotStatisticOuter.of( Collections.singletonList( statistic ),
                                     metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS;
    }

    /**
     * Returns the probabilities.
     * 
     * @return the probabilities
     */

    private VectorOfDoubles getProbabilities()
    {
        return this.probabilities;
    }

    /**
     * Hidden constructor.
     */

    private BoxPlotPercentageError()
    {
        this.probabilities = DEFAULT_PROBABILITIES;
    }

}
