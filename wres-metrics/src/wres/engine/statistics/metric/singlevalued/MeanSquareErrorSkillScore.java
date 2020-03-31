package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency. The perfect MSE-SS is 1.0.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanSquareErrorSkillScore extends DecomposableScore<SampleData<Pair<Double, Double>>>
        implements Collectable<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MeanSquareErrorSkillScore of()
    {
        return new MeanSquareErrorSkillScore();
    }

    /**
     * Instance of {@link SumOfSquareError}.
     */

    private final SumOfSquareError sse;

    @Override
    public DoubleScoreStatistic apply( final SampleData<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        if ( this.getScoreOutputGroup() != MetricGroup.NONE )
        {
            throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                  + "'." );
        }

        //TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        //template as the componentID in the metadata

        double result = Double.NaN;

        // Some data, proceed
        if ( !s.getRawData().isEmpty() )
        {
            double numerator = sse.apply( s ).getData();
            double denominator = 0.0;
            if ( s.hasBaseline() )
            {
                denominator = sse.apply( s.getBaselineData() ).getData();
            }
            else
            {
                double meanLeft =
                        FunctionFactory.mean().applyAsDouble( VectorOfDoubles.of( Slicer.getLeftSide( s ) ) );
                for ( Pair<Double, Double> next : s.getRawData() )
                {
                    denominator += Math.pow( next.getLeft() - meanLeft, 2 );
                }
            }
            result = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        // Metadata
        DatasetIdentifier baselineIdentifier = null;
        if ( s.hasBaseline() )
        {
            baselineIdentifier = s.getBaselineData().getMetadata().getIdentifier();
        }
        final StatisticMetadata metOut = StatisticMetadata.of( s.getMetadata(),
                                                               this.getID(),
                                                               MetricConstants.MAIN,
                                                               this.hasRealUnits(),
                                                               s.getRawData().size(),
                                                               baselineIdentifier );
        return DoubleScoreStatistic.of( result, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
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

    @Override
    public DoubleScoreStatistic aggregate( DoubleScoreStatistic output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        
        return output;
    }

    @Override
    public DoubleScoreStatistic getInputForAggregation( SampleData<Pair<Double, Double>> input )
    {
        return this.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
    }    
    
    /**
     * Hidden constructor.
     */

    MeanSquareErrorSkillScore()
    {
        super();
        sse = SumOfSquareError.of();
    }

}
