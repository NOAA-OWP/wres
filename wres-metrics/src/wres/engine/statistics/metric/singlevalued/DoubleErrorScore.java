package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.DoubleErrorFunction;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * A generic implementation of an error score that cannot be decomposed. For scores that can be computed in a single-pass,
 * provide a {@link DoubleErrorFunction} to the constructor. This function is applied to each pair, and the average
 * score returned across all pairs.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class DoubleErrorScore<S extends SingleValuedPairs> extends OrdinaryScore<S, DoubleScoreStatistic>
{
    /**
     * The error function.
     */

    DoubleErrorFunction function;

    @Override
    public DoubleScoreStatistic apply( final S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        if ( Objects.isNull( function ) )
        {
            throw new MetricCalculationException( "Override or specify a non-null error function for the '" + toString()
                                                  + "'." );
        }
        //Metadata
        DatasetIdentifier id = null;
        if ( s.hasBaseline() && s.getMetadataForBaseline().hasIdentifier() )
        {
            id = s.getMetadataForBaseline().getIdentifier();
        }
        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                    this.getID(),
                                    MetricConstants.MAIN,
                                    this.hasRealUnits(),
                                    s.getRawData().size(),
                                    id );

        //Compute the atomic errors in a stream
        double doubleScore = MissingValues.MISSING_DOUBLE;
        if ( !s.getRawData().isEmpty() )
        {
            doubleScore = s.getRawData().stream().mapToDouble( function ).average().getAsDouble();
        }
        return DoubleScoreStatistic.of( doubleScore, metOut );
    }

    @Override
    public ScoreGroup getScoreOutputGroup()
    {
        return ScoreGroup.NONE;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    /**
     * Hidden constructor for a delegated implementation, i.e. where the concrete implementation overrides 
     * {@link #apply(SingleValuedPairs)}.
     */

    protected DoubleErrorScore()
    {
        super();

        this.function = null;
    }

    /**
     * Hidden constructor. If the input function is null, the concrete implementation must override 
     * {@link #apply(SingleValuedPairs)}.
     * 
     * @param function the error function
     */

    protected DoubleErrorScore( DoubleErrorFunction function )
    {
        super();

        // Function can be null if calculation is delegated
        this.function = function;
    }

}
