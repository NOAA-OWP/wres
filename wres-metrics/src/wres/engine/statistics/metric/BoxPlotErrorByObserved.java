package wres.engine.statistics.metric;

import wres.datamodel.BoxPlotOutput;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * An concrete implementation of a {@link BoxPlot} that plots the ensemble forecast errors (right - left) against 
 * observed value. A box is constructed for the errors associated with each ensemble forecast where the errors (whiskers) 
 * are mapped to prescribed quantiles (probability thresholds).
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class BoxPlotErrorByObserved extends BoxPlot
{

    @Override
    public MetricConstants getID()
    {
        return null;
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
        return null;
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

    static class BoxPlotErrorByObservedBuilder extends BoxPlotBuilder
    {

        @Override
        protected Metric<EnsemblePairs, BoxPlotOutput> build() throws MetricParameterException
        {
            return new BoxPlotErrorByObserved(this);
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
