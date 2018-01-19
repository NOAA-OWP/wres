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
import wres.datamodel.outputs.DurationOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.Score;

/**
 * A summary statistic that operates on a {@link TimeToPeakError}.
 * 
 * TODO: consider implementing an API for summary statistics that work with {@link Duration}. Currently, this 
 * implementation works with {@link AbstractUnivariateStatistic}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class TimeToPeakErrorStatistic extends Metric<TimeSeriesOfSingleValuedPairs, DurationOutput>
        implements Score, Collectable<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, DurationOutput>
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
    public DurationOutput apply( TimeSeriesOfSingleValuedPairs s )
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
        return identifier;
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
    public DurationOutput aggregate( PairedOutput<Instant, Duration> output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        // Convert the input to double ms
        double[] input = output.getData().stream().mapToDouble( a -> a.getValue().toMillis() ).toArray();

        MetricOutputMetadata in = output.getMetadata();
        MetricOutputMetadata meta = getDataFactory().getMetadataFactory().getOutputMetadata( in.getSampleSize(),
                                                                                             in.getDimension(),
                                                                                             in.getInputDimension(),
                                                                                             getID(),
                                                                                             MetricConstants.MAIN,
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
            extends MetricBuilder<TimeSeriesOfSingleValuedPairs, DurationOutput>
    {

        /**
         * The identifier for the summary statistic.
         */

        private MetricConstants identifier;

        /**
         * The summary statistic.
         */

        private AbstractUnivariateStatistic statistic;

        /**
         * Sets the summary statistic.
         * 
         * @param statistic the statistic
         * @return the builder
         */

        public TimeToPeakErrorStatisticBuilder setStatistic( AbstractUnivariateStatistic statistic )
        {
            this.statistic = statistic;
            return this;
        }

        /**
         * Sets the metric identifier.
         * 
         * @param identifier the identifier
         * @return the builder
         */

        public TimeToPeakErrorStatisticBuilder setIdentifier( MetricConstants identifier )
        {
            this.identifier = identifier;
            return this;
        }

        @Override
        protected TimeToPeakErrorStatistic build() throws MetricParameterException
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
        this.identifier = builder.identifier;
        this.statistic = builder.statistic;
        // Validate
        if ( Objects.isNull( this.identifier ) )
        {
            throw new MetricParameterException( "Specify a non-null metric identifier." );
        }
        if ( Objects.isNull( this.statistic ) )
        {
            throw new MetricParameterException( "Specify a non-null summary statistic." );
        }
        // Build the metric of which this is a collection
        timeToPeakError = MetricFactory.getInstance( getDataFactory() ).ofTimeToPeakError();
    }

}
