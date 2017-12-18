package wres.engine.statistics.metric.ensemble;

import java.util.Arrays;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * An concrete implementation of a {@link BoxPlot} that plots the ensemble forecast errors (right - left) against 
 * observed value. A box is constructed for the errors associated with each ensemble forecast where the errors (whiskers) 
 * are mapped to prescribed quantiles (probability thresholds).
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class BoxPlotErrorByObserved extends BoxPlot
{

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE;
    }

    /**
     * Creates a box from a {@link PairOfDoubleAndVectorOfDoubles}.
     * 
     * @return a box
     * @throws MetricCalculationException if the box cannot be constructed
     */

    @Override
    PairOfDoubleAndVectorOfDoubles getBox( PairOfDoubleAndVectorOfDoubles pair )
    {
        //Get the sorted errors
        double[] probs = probabilities.getDoubles();
        double[] sortedErrors = Arrays.stream( pair.getItemTwo() ).map( x -> x - pair.getItemOne() ).sorted().toArray();
        Slicer slicer = getDataFactory().getSlicer();       
        //Compute the quantiles
        double[] box = Arrays.stream( probs ).map( slicer.getQuantileFunction(sortedErrors) ).toArray();
        return getDataFactory().pairOf( pair.getItemOne(), box );
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
     * Builder for the {@link BoxPlotErrorByObserved}
     */

    public static class BoxPlotErrorByObservedBuilder extends BoxPlotBuilder
    {
        @Override
        protected Metric<EnsemblePairs, BoxPlotOutput> build() throws MetricParameterException
        {
            return new BoxPlotErrorByObserved( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if the parameters are incorrect
     */

    private BoxPlotErrorByObserved( BoxPlotErrorByObservedBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
