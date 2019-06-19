package wres.engine.statistics.metric;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;

/**
 * Constructs a {@link Metric} that returns the sample size.
 * 
 * @author james.brown@hydrosolved.com
 */
class SampleSize<S extends SampleData<?>> extends OrdinaryScore<S, DoubleScoreStatistic>
{

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SampleSize.class );
    
    /**
     * Returns an instance.
     * 
     * @param <S> the input type
     * @return an instance
     */

    public static <S extends SampleData<?>> SampleSize<S> of()
    {
        return new SampleSize<>();
    }

    @Override
    public DoubleScoreStatistic apply( S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        
        LOGGER.trace( "Found {} pairs in the input to the {} for '{}'.",
                      s.getRawData().size(),
                      this.getName(),
                      s.getMetadata() );
        
        return DoubleScoreStatistic.of( (double) s.getRawData().size(),
                                        StatisticMetadata.of( s.getMetadata(),
                                                              s.getRawData().size(),
                                                              MeasurementUnit.of( "COUNT" ),
                                                              this.getID(),
                                                              MetricConstants.MAIN ) );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.SAMPLE_SIZE;
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

    /**
     * Hidden constructor.
     */

    private SampleSize()
    {
        super();
    }

}
