package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;

/**
 * Computes the square of Pearson's product-moment correlation coefficient between the left and right sides of the
 * {SingleValuedPairs} input.
 * 
 * @author james.brown@hydrosolved.com
 */
public class CoefficientOfDetermination extends CorrelationPearsons
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static CoefficientOfDetermination of()
    {
        return new CoefficientOfDetermination();
    }

    @Override
    public DoubleScoreStatistic apply( SingleValuedPairs s )
    {
        return aggregate( getInputForAggregation( s ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.COEFFICIENT_OF_DETERMINATION;
    }

    @Override
    public DoubleScoreStatistic aggregate( DoubleScoreStatistic output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        StatisticMetadata meta = StatisticMetadata.of( output.getMetadata().getSampleMetadata(),
                                                       MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                       MetricConstants.MAIN,
                                                       this.hasRealUnits(),
                                                       output.getMetadata().getSampleSize(),
                                                       null );

        return DoubleScoreStatistic.of( Math.pow( output.getData(), 2 ), meta );
    }

    @Override
    public DoubleScoreStatistic getInputForAggregation( SingleValuedPairs input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        return super.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    /**
     * Hidden constructor.
     */

    private CoefficientOfDetermination()
    {
        super();
    }

}
