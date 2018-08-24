package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;

/**
 * <p>The {@link VolumetricEfficiency} (VE) accumulates the absolute observations (VO) and, separately, it accumulates 
 * the absolute errors of the predictions (VP). It then expresses the difference between the two as a fraction of the 
 * accumulated observations, i.e. VE = (VO - VP) / VO.</p> 
 * 
 * <p>A score of 1 denotes perfect efficiency and a score of 0 denotes a VP that matches the VO. The lower bound of 
 * the measure is <code>-Inf</code> and a score below zero indicates a VP that exceeds the VO.</p>
 *
 * @author james.brown@hydrosolved.com
 */
public class VolumetricEfficiency extends DoubleErrorScore<SingleValuedPairs>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static VolumetricEfficiency of()
    {
        return new VolumetricEfficiency();
    }

    @Override
    public DoubleScoreStatistic apply( final SingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        Double vO = 0.0;
        double vP = 0.0;
        for ( SingleValuedPair nextPair : s.getRawData() )
        {
            vO += Math.abs( nextPair.getLeft() );
            vP += Math.abs( nextPair.getLeft() - nextPair.getRight() );
        }

        //Metadata
        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                    this.getID(),
                                    MetricConstants.MAIN,
                                    this.hasRealUnits(),
                                    s.getRawData().size(),
                                    null );
        //Compute the atomic errors in a stream
        if ( vO.equals( 0.0 ) )
        {
            return DoubleScoreStatistic.of( Double.NaN, metOut );
        }
        return DoubleScoreStatistic.of( ( vO - vP ) / vO, metOut );
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
     * Hidden constructor.
     */

    private VolumetricEfficiency()
    {
        super();
    }

}
