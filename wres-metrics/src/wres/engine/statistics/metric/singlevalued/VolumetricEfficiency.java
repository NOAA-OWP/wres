package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>The {@link VolumetricEfficiency} (VE) accumulates the observations (VO) and, separately, it accumulates the 
 * absolute errors of the predictions (VE). It then expresses the difference between the two as a fraction of the 
 * accumulated observations, i.e. VE = (VO - VE) / VO.</p> 
 * 
 * <p>A score of 1 denotes perfect efficiency and a score of 0 denotes a VE that matches the VO. The lower bound of 
 * the measure is <code>-Inf</code> and a score below zero indicates a VE that exceeds the VO.</p>
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public class VolumetricEfficiency extends DoubleErrorScore<SingleValuedPairs>
{

    @Override
    public ScalarOutput apply( final SingleValuedPairs s )
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        double vO = 0.0;
        double vE = 0.0;
        for ( PairOfDoubles nextPair : s.getData() )
        {
            vO += nextPair.getItemOne();
            vE += Math.abs( nextPair.getItemOne() - nextPair.getItemTwo() );
        }

        //Metadata
        final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.MAIN, null );
        //Compute the atomic errors in a stream
        return getDataFactory().ofScalarOutput( (vO - vE) / vO, metOut );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.VOLUMETRIC_EFFICIENCY;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class VolumetricEfficiencyBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        protected VolumetricEfficiency build() throws MetricParameterException
        {
            return new VolumetricEfficiency( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private VolumetricEfficiency( final VolumetricEfficiencyBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
