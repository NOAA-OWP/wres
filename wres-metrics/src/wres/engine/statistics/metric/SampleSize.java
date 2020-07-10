package wres.engine.statistics.metric;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.MetricName;

/**
 * Constructs a {@link Metric} that returns the sample size.
 * 
 * @author james.brown@hydrosolved.com
 */
class SampleSize<S extends SampleData<?>> extends OrdinaryScore<S, DoubleScoreStatisticOuter>
{

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SampleSize.class );

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( Double.NEGATIVE_INFINITY )
                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                       .setOptimum( 0.0 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.SAMPLE_SIZE )
                             .build();

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
    public DoubleScoreStatisticOuter apply( S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        LOGGER.trace( "Found {} pairs in the input to the {} for '{}'.",
                      s.getRawData().size(),
                      this.getName(),
                      s.getMetadata() );

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( SampleSize.METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( ComponentName.MAIN )
                                                                                 .setValue( (double) s.getRawData()
                                                                                                      .size() ) )
                                    .build();

        return DoubleScoreStatisticOuter.of( score,
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
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
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
