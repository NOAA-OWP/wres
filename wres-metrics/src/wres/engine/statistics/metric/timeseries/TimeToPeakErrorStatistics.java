package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.ToDoubleFunction;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.VectorOfDoubles;
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
 * A collection of summary statistics that operate on a {@link TimeToPeakError}.
 * 
 * TODO: consider implementing an API for summary statistics that works directly with {@link Duration}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public class TimeToPeakErrorStatistics extends OrdinaryScore<TimeSeriesOfSingleValuedPairs, DurationScoreOutput>
        implements Collectable<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, DurationScoreOutput>
{

    /**
     * The summary statistics and associated identifiers
     */

    private final Map<MetricConstants, ToDoubleFunction<VectorOfDoubles>> statistics;

    /**
     * A {@link TimeToPeakError} to compute the intermediate output.
     */

    private final TimeToPeakError timeToPeakError;

    @Override
    public DurationScoreOutput apply( TimeSeriesOfSingleValuedPairs s )
    {
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

        // Map of outputs
        Map<MetricConstants, Duration> returnMe = new TreeMap<>();
        // Iterate through the statistics
        MetricConstants nextIdentifier = null;
        for ( Entry<MetricConstants, ToDoubleFunction<VectorOfDoubles>> next : statistics.entrySet() )
        {
            nextIdentifier = next.getKey();
            // Convert the input to double ms
            double[] input = output.getData().stream().mapToDouble( a -> a.getValue().toMillis() ).toArray();
            // Loss of precision here (albeit not consequential)
            returnMe.put( nextIdentifier,
                          Duration.ofMillis( (long) statistics.get( nextIdentifier )
                                                              .applyAsDouble( getDataFactory().vectorOf( input ) ) ) );
        }
        // Create output metadata with the identifier of the statistic as the component identifier
        MetricOutputMetadata in = output.getMetadata();
        MetricConstants singleIdentifier = null;
        // If the metric is defined with only one summary statistics, list this component in the metadata
        if ( statistics.size() == 1 )
        {
            singleIdentifier = nextIdentifier;
        }
        MetricOutputMetadata meta = getDataFactory().getMetadataFactory().getOutputMetadata( in.getSampleSize(),
                                                                                             in.getDimension(),
                                                                                             in.getInputDimension(),
                                                                                             getID(),
                                                                                             singleIdentifier,
                                                                                             in.getIdentifier(),
                                                                                             in.getTimeWindow() );
        return getDataFactory().ofDurationScoreOutput( returnMe, meta );
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

        private MetricConstants[] statistics;

        /**
         * Sets the statistic.
         * 
         * @param statistics the identifier
         * @return the builder
         */

        public TimeToPeakErrorStatisticBuilder setStatistic( MetricConstants... statistics )
        {
            this.statistics = statistics;
            return this;
        }

        @Override
        public TimeToPeakErrorStatistics build() throws MetricParameterException
        {
            return new TimeToPeakErrorStatistics( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private TimeToPeakErrorStatistics( final TimeToPeakErrorStatisticBuilder builder ) throws MetricParameterException
    {
        super( builder );
        if ( Objects.isNull( builder.statistics ) || builder.statistics.length == 0 )
        {
            throw new MetricParameterException( "Specify one or more summary statistics." );
        }
        // Copy
        MetricConstants[] input = Arrays.copyOf( builder.statistics, builder.statistics.length );
        this.statistics = new TreeMap<>();
        // Set and validate the copy
        try
        {
            for ( MetricConstants next : input )
            {
                statistics.put( next, FunctionFactory.ofStatistic( next ) );
            }
        }
        catch ( Exception e )
        {
            throw new MetricParameterException( "While constructing the timing error summary statistic: ", e );
        }
        // Build the metric of which this is a collection
        timeToPeakError = MetricFactory.getInstance( getDataFactory() ).ofTimeToPeakError();
    }

}
