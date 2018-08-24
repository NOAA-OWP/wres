package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * Computes Pearson's product-moment correlation coefficient between the left and right sides of the {SingleValuedPairs}
 * input. Implements {@link Collectable} to avoid repeated calculations of derivative metrics, such as the
 * {@link CoefficientOfDetermination} when both appear in a {@link MetricCollection}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class CorrelationPearsons extends OrdinaryScore<SingleValuedPairs, DoubleScoreStatistic>
        implements Collectable<SingleValuedPairs, DoubleScoreStatistic, DoubleScoreStatistic>
{

    /**
     * Instance of {@link PearsonsCorrelation}.
     */

    private final PearsonsCorrelation correlation;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static CorrelationPearsons of()
    {
        return new CorrelationPearsons();
    }

    @Override
    public DoubleScoreStatistic apply( SingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        
        // Get the metadata
        Metadata metIn = s.getMetadata();
        StatisticMetadata meta = StatisticMetadata.of( metIn,
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                             MetricConstants.MAIN,
                                                             this.hasRealUnits(),
                                                             s.getRawData().size(),
                                                             null );

        double returnMe = Double.NaN;

        // Minimum sample size of 1
        if ( s.getRawData().size() > 1 )
        {
            returnMe = FunctionFactory.finiteOrMissing()
                                      .applyAsDouble( correlation.correlation( Slicer.getLeftSide( s ),
                                                                               Slicer.getRightSide( s ) ) );
        }
        return DoubleScoreStatistic.of( returnMe, meta );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
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
        return output;
    }

    @Override
    public DoubleScoreStatistic getInputForAggregation( SingleValuedPairs input )
    {
        return apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    /**
     * Hidden constructor.
     */

    CorrelationPearsons()
    {
        super();
        correlation = new PearsonsCorrelation();
    }

}
