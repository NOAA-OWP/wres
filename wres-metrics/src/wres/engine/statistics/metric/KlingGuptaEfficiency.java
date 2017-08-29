package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOutput;

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
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class KlingGuptaEfficiency extends MeanSquareError<SingleValuedPairs>
{

    /**
     * Instance of {@link CorrelationPearsons} to use when the result is not otherwise available in a 
     * {@link MetricCollection}. 
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
    public VectorOutput apply( final SingleValuedPairs s )
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }

        //TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        //template as the componentID in the metadata

        DataFactory dataFactory = getDataFactory();
        Slicer slicer = dataFactory.getSlicer();
        double[] result = new double[1];
        //Compute the components
        double rhoVal = rho.apply( s ).getData();
        double meanPred = FunctionFactory.mean().applyAsDouble( dataFactory.vectorOf( slicer.getRightSide( s ) ) );
        double meanObs = FunctionFactory.mean().applyAsDouble( dataFactory.vectorOf( slicer.getLeftSide( s ) ) );
        double sdPred =
                FunctionFactory.standardDeviation().applyAsDouble( dataFactory.vectorOf( slicer.getRightSide( s ) ) );
        double sdObs =
                FunctionFactory.standardDeviation().applyAsDouble( dataFactory.vectorOf( slicer.getLeftSide( s ) ) );
        double gamma = ( sdPred / meanPred ) / ( sdObs / meanObs );
        double beta = meanPred / meanObs;
        double left = Math.pow( rhoWeight * rhoVal - 1, 2 );
        double middle = Math.pow( varWeight * gamma - 1, 2 );
        double right = Math.pow( biasWeight * beta - 1, 2 );
        result[0] = 1.0 - Math.sqrt( left + middle + right );
        //Metadata
        final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.NONE, null );
        return dataFactory.ofVectorOutput( result, metOut );
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

    static class KlingGuptaEfficiencyBuilder
            extends
            MeanSquareErrorBuilder<SingleValuedPairs>
    {

        @Override
        protected KlingGuptaEfficiency build()
        {
            return new KlingGuptaEfficiency( this );
        }

    }

    /**
     * Prevent direct construction.
     * 
     * @param builder the builder
     */

    private KlingGuptaEfficiency( final KlingGuptaEfficiencyBuilder builder )
    {
        super( builder );
        rho = MetricFactory.getInstance( getDataFactory() ).ofCorrelationPearsons();
        //Equal weighting of terms
        rhoWeight = 1.0;
        varWeight = 1.0;
        biasWeight = 1.0;
    }

}
