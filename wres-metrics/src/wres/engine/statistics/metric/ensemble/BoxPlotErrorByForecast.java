package wres.engine.statistics.metric.ensemble;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * An concrete implementation of a {@link BoxPlot} that plots the ensemble forecast errors (right - left) against 
 * forecast value. A box is constructed for the errors associated with each ensemble forecast where the errors 
 * (whiskers) are mapped to prescribed quantiles (probability thresholds). The function used to map the forecast
 * value may be prescribed.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BoxPlotErrorByForecast extends BoxPlot
{

    /**
     * Default dimension for the domain.
     */

    private static final MetricDimension DEFAULT_DOMAIN_DIMENSION = MetricDimension.ENSEMBLE_MEAN;

    /**
     * Default domain mappoer function.
     */

    private static final ToDoubleFunction<VectorOfDoubles> DEFAULT_DOMAIN_MAPPER = FunctionFactory.mean();

    /**
     * The dimension associated with the domain axis, which corresponds to a function that is applied to the 
     * forecast values.
     */

    private final MetricDimension domainDimension;

    /**
     * The function used to map the forecast values for the domain axis.
     */

    private final ToDoubleFunction<VectorOfDoubles> domainMapper;

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
            throws MetricParameterException
    {
        return new BoxPlotErrorByForecast( probabilities, domainDimension );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE;
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
        double[] sorted = pair.getRight();
        Arrays.sort( sorted );
        double[] sortedErrors = Arrays.stream( sorted ).map( x -> x - pair.getLeft() ).toArray();

        //Compute the quantiles
        double[] box =
                Arrays.stream( probs ).map( Slicer.getQuantileFunction( sortedErrors ) ).toArray();
        return EnsemblePair.of( domainMapper.applyAsDouble( VectorOfDoubles.of( sorted ) ), box );
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
     * Hidden constructor.
     * 
     * @param probabilities the probabilities
     * @param domainDimension the domain axis dimension
     */

    private BoxPlotErrorByForecast()
    {
        super();

        this.domainDimension = DEFAULT_DOMAIN_DIMENSION;

        this.domainMapper = DEFAULT_DOMAIN_MAPPER;
    }

    /**
     * Hidden constructor.
     * 
     * @param probabilities the probabilities
     * @param domainDimension the domain axis dimension
     * @throws MetricParameterException if the parameters are incorrect
     */

    private BoxPlotErrorByForecast( VectorOfDoubles probabilities, MetricDimension domainDimension )
            throws MetricParameterException
    {
        super( probabilities );

        if ( Objects.isNull( domainDimension ) )
        {
            throw new MetricParameterException( "Cannot build the box plot of forecast errors by forecast value without "
                                                + "a dimension for the domain axis." );
        }

        this.domainDimension = domainDimension;

        switch ( domainDimension )
        {
            case ENSEMBLE_MEAN:
                domainMapper = FunctionFactory.mean();
                break;
            case ENSEMBLE_MEDIAN:
                domainMapper =
                        a -> Slicer.getQuantileFunction( a.getDoubles() ).applyAsDouble( 0.5 );
                break;
            default:
                throw new MetricParameterException( "Unsupported dimension for the domain axis of the box plot: "
                                                    + "'"
                                                    + domainDimension
                                                    + "'." );
        }
    }

}
