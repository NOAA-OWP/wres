package wres.engine.statistics.metric.ensemble;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.statistics.BoxplotStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

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
    public MetricConstants getID()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE;
    }

    /**
     * Creates a box from an ensemble pair.
     * 
     * @param metadata the box metadata
     * @return a box
     * @throws MetricCalculationException if the box cannot be constructed
     */

    @Override
    BoxplotStatistic getBox( Pair<Double,Ensemble> pair, StatisticMetadata metadata )
    {
        //Get the sorted errors
        double[] probs = probabilities.getDoubles();
        double[] sortedErrors =
                Arrays.stream( pair.getRight().getMembers() ).map( x -> x - pair.getLeft() ).sorted().toArray();
        //Compute the quantiles
        double[] box =
                Arrays.stream( probs ).map( Slicer.getQuantileFunction( sortedErrors ) ).toArray();
        return BoxplotStatistic.of( this.probabilities,
                                    VectorOfDoubles.of( box ),
                                    metadata,
                                    pair.getLeft(),
                                    MetricDimension.OBSERVED_VALUE );
    }

    /**
     * Hidden constructor.
     */

    private BoxPlotErrorByObserved()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param probabilities the probabilities
     * @throws MetricParameterException if the parameters are incorrect
     */

    private BoxPlotErrorByObserved( VectorOfDoubles probabilities ) throws MetricParameterException
    {
        super( probabilities );
    }

}
