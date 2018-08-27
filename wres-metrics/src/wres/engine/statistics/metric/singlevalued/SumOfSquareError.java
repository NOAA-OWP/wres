package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Base class for decomposable scores that involve a sum-of-square errors.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SumOfSquareError extends DecomposableScore<SingleValuedPairs>
        implements Collectable<SingleValuedPairs, DoubleScoreStatistic, DoubleScoreStatistic>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static SumOfSquareError of()
    {
        return new SumOfSquareError();
    }

    @Override
    public DoubleScoreStatistic apply( SingleValuedPairs s )
    {
        return this.aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    @Override
    public DoubleScoreStatistic getInputForAggregation( SingleValuedPairs input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.MISSING_DOUBLE;

        // Data available
        if ( !input.getRawData().isEmpty() )
        {
            returnMe = input.getRawData().stream().mapToDouble( FunctionFactory.squareError() ).sum();
        }

        //Metadata
        StatisticMetadata metOut = StatisticMetadata.of( input.getMetadata(),
                                                         MetricConstants.SUM_OF_SQUARE_ERROR,
                                                         MetricConstants.MAIN,
                                                         this.hasRealUnits(),
                                                         input.getRawData().size(),
                                                         null );

        return DoubleScoreStatistic.of( returnMe, metOut );
    }

    @Override
    public DoubleScoreStatistic aggregate( DoubleScoreStatistic output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        // Set the output dimension
        StatisticMetadata meta = StatisticMetadata.of( output.getMetadata().getSampleMetadata(),
                                                       this.getID(),
                                                       MetricConstants.MAIN,
                                                       this.hasRealUnits(),
                                                       output.getMetadata().getSampleSize(),
                                                       null );

        return DoubleScoreStatistic.of( output.getData(), meta );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    SumOfSquareError()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    SumOfSquareError( ScoreGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
