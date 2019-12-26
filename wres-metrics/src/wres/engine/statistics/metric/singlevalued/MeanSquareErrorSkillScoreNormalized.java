package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Collectable;

/**
 * Computes the Mean Square Error Skill Score Normalized to (0,1] (MSESSN), which is related to the 
 * {@link MeanSquareErrorSkillScore} (MSESS) as: MSESSN = 1.0 / (2.0 - MSESS). 
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanSquareErrorSkillScoreNormalized extends MeanSquareErrorSkillScore
        implements Collectable<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MeanSquareErrorSkillScoreNormalized of()
    {
        return new MeanSquareErrorSkillScoreNormalized();
    }

    @Override
    public DoubleScoreStatistic apply( SampleData<Pair<Double, Double>> s )
    {
        return this.aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED;
    }

    @Override
    public ScoreGroup getScoreOutputGroup()
    {
        return ScoreGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public DoubleScoreStatistic aggregate( DoubleScoreStatistic output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        
        StatisticMetadata meta = StatisticMetadata.of( output.getMetadata().getSampleMetadata(),
                                                       MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED,
                                                       MetricConstants.MAIN,
                                                       this.hasRealUnits(),
                                                       output.getMetadata().getSampleSize(),
                                                       null );

        return DoubleScoreStatistic.of( 1.0 / ( 2.0 - output.getData() ), meta );
    }

    @Override
    public DoubleScoreStatistic getInputForAggregation( SampleData<Pair<Double, Double>> input )
    {
        return super.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
    }

    /**
     * Hidden constructor.
     */

    MeanSquareErrorSkillScoreNormalized()
    {
        super();
    }

}
