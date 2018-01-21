package wres.engine.statistics.metric.ensemble;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * An concrete implementation of a {@link BoxPlot} that plots the ensemble forecast errors (right - left) against 
 * forecast value. A box is constructed for the errors associated with each ensemble forecast where the errors 
 * (whiskers) are mapped to prescribed quantiles (probability thresholds). The function used to map the forecast
 * value may be prescribed.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class BoxPlotErrorByForecast extends BoxPlot
{

    /**
     * The dimension associated with the domain axis, which corresponds to a function that is applied to the 
     * forecast values.
     */

    private final MetricDimension domainDimension;

    /**
     * The function used to map the forecast values for the domain axis.
     */

    private final ToDoubleFunction<VectorOfDoubles> domainMapper;

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE;
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
        double[] sorted = pair.getItemTwo();
        Arrays.sort( sorted );
        double[] sortedErrors = Arrays.stream( sorted ).map( x -> x - pair.getItemOne() ).toArray();
        DataFactory dFac = getDataFactory();
        Slicer slicer = dFac.getSlicer();
        //Compute the quantiles
        double[] box = Arrays.stream( probs ).map( slicer.getQuantileFunction( sortedErrors ) ).toArray();
        return dFac.pairOf( domainMapper.applyAsDouble( dFac.vectorOf( sorted ) ), box );
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
        return domainDimension;
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
     * Builder for the {@link BoxPlotErrorByForecast}
     */

    public static class BoxPlotErrorByForecastBuilder extends BoxPlotBuilder
    {

        /**
         * Default dimension for the domain.
         */

        private MetricDimension domainDimension = MetricDimension.ENSEMBLE_MEAN;

        @Override
        public Metric<EnsemblePairs, BoxPlotOutput> build() throws MetricParameterException
        {
            return new BoxPlotErrorByForecast( this );
        }

        /**
         * Sets the dimension for the domain axis.
         * 
         * @param domainDimension the domain axis dimension
         * @return the builder
         */
        BoxPlotErrorByForecastBuilder setDomainDimension( MetricDimension domainDimension )
        {
            this.domainDimension = domainDimension;
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if the parameters are incorrect
     */

    private BoxPlotErrorByForecast( BoxPlotErrorByForecastBuilder builder ) throws MetricParameterException
    {
        super( builder );
        domainDimension = builder.domainDimension;
        if ( Objects.isNull( domainDimension ) )
        {
            throw new MetricParameterException( "Cannot build the box plot of forecast errors by forecast value without "
                                                + "a dimension for the domain axis." );
        }
        switch ( domainDimension )
        {
            case ENSEMBLE_MEAN:
                domainMapper = FunctionFactory.mean();
                break;
            case ENSEMBLE_MEDIAN:
                domainMapper =
                        a -> getDataFactory().getSlicer().getQuantileFunction( a.getDoubles() ).applyAsDouble( 0.5 );
                break;
            default:
                throw new MetricParameterException( "Unsupported dimension for the domain axis of the box plot: "
                                                    + "'" + domainDimension + "'." );
        }
    }

}
