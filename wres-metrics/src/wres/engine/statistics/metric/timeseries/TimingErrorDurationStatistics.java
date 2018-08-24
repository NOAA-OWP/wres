package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.statistics.DurationScoreOutput;
import wres.datamodel.statistics.PairedOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * A collection of summary statistics that operate on the outputs from {@link TimingError} and are expressed as 
 * {@link DurationScoreOutput}.
 * 
 * TODO: consider implementing an API for summary statistics that works directly with {@link Duration}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimingErrorDurationStatistics implements Function<PairedOutput<Instant, Duration>, DurationScoreOutput>
{

    /**
     * The summary statistics and associated identifiers
     */

    private final Map<MetricConstants, ToDoubleFunction<VectorOfDoubles>> statistics;

    /**
     * The unique identifier associated with these summary statistics.
     */

    private final MetricConstants identifier;

    /**
     * Returns an instance.
     * 
     * @param identifier the unique identifier for the summary statistics
     * @param statistics the list of statistics the compute
     * @throws MetricParameterException if one or more parameters is invalid
     * @return an instance
     */

    public static TimingErrorDurationStatistics of( MetricConstants identifier, Set<MetricConstants> statistics )
            throws MetricParameterException
    {
        return new TimingErrorDurationStatistics( identifier, statistics );
    }


    @Override
    public DurationScoreOutput apply( PairedOutput<Instant, Duration> pairs )
    {
        if ( Objects.isNull( pairs ) )
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

            // Some data available
            if ( !pairs.getData().isEmpty() )
            {
                // Convert the input to double ms
                double[] input = pairs.getData().stream().mapToDouble( a -> a.getValue().toMillis() ).toArray();

                // Some loss of precision here, not consequential
                returnMe.put( nextIdentifier,
                              Duration.ofMillis( (long) statistics.get( nextIdentifier )
                                                                  .applyAsDouble( VectorOfDoubles.of( input ) ) ) );
            }
            // No data available
            else
            {
                returnMe.put( nextIdentifier, MissingValues.MISSING_DURATION );
            }
        }

        // Create output metadata with the identifier of the statistic as the component identifier
        MetricOutputMetadata in = pairs.getMetadata();
        MetricConstants singleIdentifier = null;

        // If the metric is defined with only one summary statistic, list this component in the metadata
        if ( statistics.size() == 1 )
        {
            singleIdentifier = nextIdentifier;
        }
        final MetricConstants componentID = singleIdentifier;
        
        MetricOutputMetadata meta = MetricOutputMetadata.of( in, this.getID(), componentID );
        return DurationScoreOutput.of( returnMe, meta );
    }

    /**
     * Hidden constructor.
     * 
     * @param identifier the unique identifier for the summary statistics
     * @param statistics the list of statistics the compute
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private TimingErrorDurationStatistics( MetricConstants identifier, Set<MetricConstants> statistics )
            throws MetricParameterException
    {

        if ( Objects.isNull( identifier ) )
        {
            throw new MetricParameterException( "Specify a unique identifier from which to build the statistics." );
        }

        if ( Objects.isNull( statistics ) )
        {
            throw new MetricParameterException( "Specify a non-null container of summary statistics." );
        }

        this.identifier = identifier;

        // Copy locally
        Set<MetricConstants> input = new HashSet<>( statistics );

        // Validate
        if ( input.isEmpty() )
        {
            throw new MetricParameterException( "Specify one or more summary statistics." );
        }

        this.statistics = new TreeMap<>();

        // Set and validate the copy
        for ( MetricConstants next : input )
        {
            if ( Objects.isNull( next ) )
            {
                throw new MetricParameterException( "Cannot build the metric with a null statistic." );
            }
            this.statistics.put( next, FunctionFactory.ofStatistic( next ) );
        }

    }

    /**
     * Returns the unique identifier associated with these statistics.
     * 
     * @return the unique identifier
     */

    private MetricConstants getID()
    {
        return this.identifier;
    }

}
