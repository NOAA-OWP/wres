package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * As with the MSE, the Root Mean Square Error (RMSE) or Root Mean Square Deviation (RMSD) is a measure of accuracy.
 * However, the RMSE is expressed in the original (unsquared) units of the predictand and no decompositions are
 * available for the RMSE.
 * 
 * @author james.brown@hydrosolved.com
 */
public class RootMeanSquareError extends DoubleErrorScore<SingleValuedPairs>
        implements Collectable<SingleValuedPairs, DoubleScoreStatistic, DoubleScoreStatistic>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static RootMeanSquareError of()
    {
        return new RootMeanSquareError();
    }

    /**
     * Instance if {@link SumOfSquareError}.
     */

    private final SumOfSquareError sse;

    @Override
    public DoubleScoreStatistic apply( final SingleValuedPairs t )
    {
        return this.aggregate( this.getInputForAggregation( t ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.ROOT_MEAN_SQUARE_ERROR;
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
    public DoubleScoreStatistic aggregate( DoubleScoreStatistic output )
    {
        // Set the output dimension
        StatisticMetadata meta = StatisticMetadata.of( output.getMetadata().getSampleMetadata(),
                                                       this.getID(),
                                                       MetricConstants.MAIN,
                                                       this.hasRealUnits(),
                                                       output.getMetadata().getSampleSize(),
                                                       null );
        
        return DoubleScoreStatistic.of( Math.sqrt( output.getData() / output.getMetadata().getSampleSize() ), meta );
    }

    @Override
    public DoubleScoreStatistic getInputForAggregation( SingleValuedPairs input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        return sse.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    private RootMeanSquareError()
    {
        super( FunctionFactory.squareError() );

        sse = SumOfSquareError.of();
    }

}
