package wres.engine.statistics.metric.singlevalued;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * A box plot of the errors associated with a pool of single-valued pairs.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BoxPlotError extends Diagram<SampleData<Pair<Double,Double>>, BoxPlotStatistics>
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

    public static BoxPlotError of()
    {
        return new BoxPlotError();
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public BoxPlotStatistics apply( final SampleData<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        StatisticMetadata metOut = StatisticMetadata.of( s.getMetadata(),
                                                         this.getID(),
                                                         MetricConstants.MAIN,
                                                         this.hasRealUnits(),
                                                         s.getRawData().size(),
                                                         null );

        // Empty output for empty input
        if ( s.getRawData().isEmpty() )
        {
            // Add an empty box: #62863
            BoxPlotStatistic emptyBox = BoxPlotStatistic.of( DEFAULT_PROBABILITIES, EMPTY_BOX, metOut );
            return BoxPlotStatistics.of( Collections.singletonList( emptyBox ), metOut );
        }

        // Get the sorted errors
        double[] probs = this.getProbabilities().getDoubles();
        double[] sortedErrors =
                s.getRawData().stream().mapToDouble( a -> a.getRight() - a.getLeft() ).sorted().toArray();

        // Compute the quantiles of the errors at a rounded precision
        double[] box =
                Arrays.stream( probs )
                      .map( Slicer.getQuantileFunction( sortedErrors ) )
                      .map( ROUNDER )
                      .toArray();

        BoxPlotStatistic statistic = BoxPlotStatistic.of( this.getProbabilities(),
                                                          VectorOfDoubles.of( box ),
                                                          metOut );
        return BoxPlotStatistics.of( Collections.singletonList( statistic ),
                                     metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS;
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

    private BoxPlotError()
    {
        this.probabilities = DEFAULT_PROBABILITIES;
    }

}
