package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The Root Mean Square Error (RMSE) normalized by the standard deviation of the observations (SDO), also known as
 * the RMSE Standard Deviation Ratio (RSR): RSR = RMSE / SDO.
 * 
 * @author james.brown@hydrosolved.com
 */
public class RootMeanSquareErrorNormalized extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
{
    
    /**
     * Instance of a standard deviation.
     */
    
    private final ToDoubleFunction<VectorOfDoubles> stdev;
    
    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static RootMeanSquareErrorNormalized of()
    {
        return new RootMeanSquareErrorNormalized();
    }

    @Override
    public DoubleScoreStatistic apply( final SampleData<Pair<Double, Double>> t )
    {
        if ( Objects.isNull( t ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.DOUBLE;

        // Data available
        if ( !t.getRawData().isEmpty() )
        {

            double mseValue = super.apply( t ).getData();

            //Compute the observation standard deviation
            double[] obs = t.getRawData()
                            .stream()
                            .mapToDouble( Pair::getLeft )
                            .toArray();
            
            double stdevValue = this.stdev.applyAsDouble( VectorOfDoubles.of( obs ) );

            returnMe = Math.sqrt( mseValue ) / stdevValue;
        }

        //Metadata
        StatisticMetadata metOut = StatisticMetadata.of( t.getMetadata(),
                                                         this.getID(),
                                                         MetricConstants.MAIN,
                                                         this.hasRealUnits(),
                                                         t.getRawData().size(),
                                                         null );

        return DoubleScoreStatistic.of( returnMe, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.ROOT_MEAN_SQUARE_ERROR_NORMALIZED;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private RootMeanSquareErrorNormalized()
    {
        super( FunctionFactory.squareError(), FunctionFactory.mean() );
     
        stdev = FunctionFactory.standardDeviation();
    }

}
