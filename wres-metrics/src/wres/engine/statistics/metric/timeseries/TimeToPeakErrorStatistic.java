package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.apache.commons.math3.stat.descriptive.AbstractUnivariateStatistic;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * A summary statistic that operates on a {@link TimeToPeakError}.
 * 
 * TODO: consider implementing an API for summary statistics that work with {@link Duration}. Currently, this 
 * implementation works with {@link AbstractUnivariateStatistic}, which requires mapping between <code>double</code>
 * times in fixed units and {@link Duration}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public class TimeToPeakErrorStatistic extends OrdinaryScore<TimeSeriesOfSingleValuedPairs, DurationScoreOutput>
        implements Collectable<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, DurationScoreOutput>
{

    /**
     * The identifier for the summary statistic.
     */

    private final MetricConstants identifier;

    /**
     * The summary statistic.
     */

    private final AbstractUnivariateStatistic statistic;

    /**
     * A {@link TimeToPeakError} to compute the intermediate output.
     */

    private final TimeToPeakError timeToPeakError;

    @Override
    public DurationScoreOutput apply( TimeSeriesOfSingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        return aggregate( getCollectionInput( s ) );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return ScoreOutputGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public DurationScoreOutput aggregate( PairedOutput<Instant, Duration> output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        // Convert the input to double ms
        double[] input = output.getData().stream().mapToDouble( a -> a.getValue().toMillis() ).toArray();

        MetricOutputMetadata in = output.getMetadata();
        // Create output metadata with the identifier of the statistic as the component identifier
        MetricOutputMetadata meta = getDataFactory().getMetadataFactory().getOutputMetadata( in.getSampleSize(),
                                                                                             in.getDimension(),
                                                                                             in.getInputDimension(),
                                                                                             getID(),
                                                                                             identifier,
                                                                                             in.getIdentifier() );
        // Loss of precision here (albeit not consequential)
        Duration returnMe = Duration.ofMillis( (long) statistic.evaluate( input ) );
        return getDataFactory().ofDurationOutput( returnMe, meta );
    }

    @Override
    public PairedOutput<Instant, Duration> getCollectionInput( TimeSeriesOfSingleValuedPairs input )
    {
        return timeToPeakError.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.TIME_TO_PEAK_ERROR;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class TimeToPeakErrorStatisticBuilder
            extends OrdinaryScoreBuilder<TimeSeriesOfSingleValuedPairs, DurationScoreOutput>
    {

        /**
         * The identifier for the summary statistic.
         */

        private MetricConstants statistic;

        /**
         * Sets the statistic.
         * 
         * @param statistic the identifier
         * @return the builder
         */

        public TimeToPeakErrorStatisticBuilder setStatistic( MetricConstants statistic )
        {
            this.statistic = statistic;
            return this;
        }

        @Override
        public TimeToPeakErrorStatistic build() throws MetricParameterException
        {
            return new TimeToPeakErrorStatistic( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private TimeToPeakErrorStatistic( final TimeToPeakErrorStatisticBuilder builder ) throws MetricParameterException
    {
        super( builder );
        // Copy
        this.identifier = builder.statistic;
        // Validate
        if ( Objects.isNull( this.identifier ) )
        {
            throw new MetricParameterException( "Specify a non-null summary statistic." );
        }
        // Derive the statistic from the identifier
        try {
            this.statistic = FunctionFactory.ofStatistic( this.identifier );
        }
        catch (Exception e)
        {
            throw new MetricParameterException( "While constructing the timing error summary statistic: ", e);
        }        
        // Build the metric of which this is a collection
        timeToPeakError = MetricFactory.getInstance( getDataFactory() ).ofTimeToPeakError();
    }

}
