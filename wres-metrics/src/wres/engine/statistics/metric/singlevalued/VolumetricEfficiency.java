package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>The {@link VolumetricEfficiency} (VE) accumulates the absolute observations (VO) and, separately, it accumulates 
 * the absolute errors of the predictions (VP). It then expresses the difference between the two as a fraction of the 
 * accumulated observations, i.e. VE = (VO - VP) / VO.</p> 
 * 
 * <p>A score of 1 denotes perfect efficiency and a score of 0 denotes a VP that matches the VO. The lower bound of 
 * the measure is <code>-Inf</code> and a score below zero indicates a VP that exceeds the VO.</p>
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public class VolumetricEfficiency extends DoubleErrorScore<SingleValuedPairs>
{

    @Override
    public DoubleScoreOutput apply( final SingleValuedPairs s )
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        Double vO = 0.0;
        double vP = 0.0;
        for ( PairOfDoubles nextPair : s.getRawData() )
        {
            vO += Math.abs( nextPair.getItemOne() );
            vP += Math.abs( nextPair.getItemOne() - nextPair.getItemTwo() );
        }

        //Metadata
        final MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );
        //Compute the atomic errors in a stream
        if( vO.equals( 0.0 ) )
        {
            return getDataFactory().ofDoubleScoreOutput( Double.NaN, metOut );
        }
        return getDataFactory().ofDoubleScoreOutput( ( vO - vP ) / vO, metOut );
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
        public VolumetricEfficiency build() throws MetricParameterException
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
