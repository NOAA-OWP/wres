package wres.engine.statistics.metric.ensemble;

import java.util.Arrays;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * An concrete implementation of a {@link BoxPlot} that plots the ensemble forecast errors (right - left) against 
 * observed value. A box is constructed for the errors associated with each ensemble forecast where the errors (whiskers) 
 * are mapped to prescribed quantiles (probability thresholds).
 * 
 * @author james.brown@hydrosolved.com
 */

public class BoxPlotErrorByObserved extends BoxPlot
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
     * Creates a box from a {@link EnsemblePair}.
     * 
     * @return a box
     * @throws MetricCalculationException if the box cannot be constructed
     */

    @Override
    EnsemblePair getBox( EnsemblePair pair )
    {
        //Get the sorted errors
        double[] probs = probabilities.getDoubles();
        double[] sortedErrors = Arrays.stream( pair.getRight() ).map( x -> x - pair.getLeft() ).sorted().toArray();
        //Compute the quantiles
        double[] box =
                Arrays.stream( probs ).map( Slicer.getQuantileFunction( sortedErrors ) ).toArray();
        return EnsemblePair.of( pair.getLeft(), box );
    }

    /**
     * Returns the dimension associated with the left side of the pairing, i.e. the value against which each box is
     * plotted on the domain axis. 
     * 
     * @return the domain axis dimension
     */

    @Override
    MetricDimension getDomainAxisDimension()
    {
        return MetricDimension.OBSERVED_VALUE;
    }

    /**
     * Returns the dimension associated with the right side of the pairing, i.e. the values associated with the 
     * whiskers of each box. 
     * 
     * @return the range axis dimension
     */

    @Override
    MetricDimension getRangeAxisDimension()
    {
        return MetricDimension.FORECAST_ERROR;
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
