package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons.CorrelationPearsonsBuilder;

/**
 * <p>Computes the Kling-Gupta Efficiency (KGE) and associated decomposition into correlation, bias and variability.</p>
 * 
 * <p>The KGE measures the skill of model predictions against observations in terms of the relative contributions from
 * correlation, bias and variability. The implementation details are described here:</p>
 * 
 * <p>Kling, H., Fuchs, M. and Paulin, M. (2012). Runoff conditions in the upper Danube basin under an ensemble of 
 * climate change scenarios. <i>Journal of Hydrology</i>, <b>424-425</b>, pp. 264-277, 
 * DOI:10.1016/j.jhydrol.2012.01.011</p>
 * 
 * TODO: add this to a {@link Collectable} with {@link CorrelationPearsons} and have both use a {DoubleScoreOutput}
 * that contains the relevant components for computing both, including the marginal means and variances and the 
 * covariances. Do the same for any other scores that uses these components.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class KlingGuptaEfficiency extends MeanSquareError<SingleValuedPairs>
{

    /**
     * Instance of {@link CorrelationPearsons}.
     */

    private final CorrelationPearsons rho;

    /**
     * Weighting for the correlation term.
     */

    private final double rhoWeight;

    /**
     * Weighting for the variability term.
     */

    private final double varWeight;

    /**
     * Weighting for the bias term.
     */

    private final double biasWeight;

    @Override
    public DoubleScoreOutput apply( final SingleValuedPairs s )
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }

        //TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        //template as the componentID in the metadata

        DataFactory dataFactory = getDataFactory();
        Slicer slicer = dataFactory.getSlicer();
        double result = Double.NaN;
        // Compute the components
        VectorOfDoubles leftValues = dataFactory.vectorOf( slicer.getLeftSide( s ) );
        VectorOfDoubles rightValues = dataFactory.vectorOf( slicer.getRightSide( s ) );
        double rhoVal = rho.apply( s ).getData();
        // Check for finite correlation
        if ( Double.isFinite( rhoVal ) )
        {
            double meanPred = FunctionFactory.mean().applyAsDouble( rightValues );
            double meanObs = FunctionFactory.mean().applyAsDouble( leftValues );
            double sdPred = FunctionFactory.standardDeviation().applyAsDouble( rightValues );
            double sdObs = FunctionFactory.standardDeviation().applyAsDouble( leftValues );
            double gamma = ( sdPred / meanPred ) / ( sdObs / meanObs );
            double beta = meanPred / meanObs;
            double left = Math.pow( rhoWeight * ( rhoVal - 1.0 ), 2 );
            double middle = Math.pow( varWeight * ( gamma - 1.0 ), 2 );
            double right = Math.pow( biasWeight * ( beta - 1.0 ), 2 );
            result = FunctionFactory.finiteOrNaN().applyAsDouble( 1.0 - Math.sqrt( left + middle + right ) );
        }
        //Metadata
        final MetricOutputMetadata metOut = getMetadata( s );
        return dataFactory.ofDoubleScoreOutput( result, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.KLING_GUPTA_EFFICIENCY;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class KlingGuptaEfficiencyBuilder
            extends
            MeanSquareErrorBuilder<SingleValuedPairs>
    {

        @Override
        public KlingGuptaEfficiency build() throws MetricParameterException
        {
            return new KlingGuptaEfficiency( this );
        }

    }

    /**
     * Prevent direct construction.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private KlingGuptaEfficiency( final KlingGuptaEfficiencyBuilder builder ) throws MetricParameterException
    {
        super( builder );
        CorrelationPearsonsBuilder rhoBuilder = new CorrelationPearsonsBuilder();
        rhoBuilder.setOutputFactory( getDataFactory() );
        rho = rhoBuilder.build();
        //Equal weighting of terms
        rhoWeight = 1.0;
        varWeight = 1.0;
        biasWeight = 1.0;
    }

}
