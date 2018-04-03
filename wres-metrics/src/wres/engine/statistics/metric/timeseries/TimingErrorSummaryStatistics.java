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

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.Metric.MetricBuilder;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * A collection of summary statistics that operate on a {@link TimeToPeakError}.
 * 
 * TODO: consider implementing an API for summary statistics that works directly with {@link Duration}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimingErrorSummaryStatistics implements Function<PairedOutput<Instant, Duration>, DurationScoreOutput>
{

    /**
     * The data factory.
     */

    private final DataFactory dataFactory;

    /**
     * The summary statistics and associated identifiers
     */

    private final Map<MetricConstants, ToDoubleFunction<VectorOfDoubles>> statistics;
    
    /**
     * The unique identifier associated with these summary statistics.
     */

    private final MetricConstants identifier;
    
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
                                                                  .applyAsDouble( this.getDataFactory()
                                                                                      .vectorOf( input ) ) ) );
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
        MetricOutputMetadata meta = this.getDataFactory().getMetadataFactory().getOutputMetadata( in.getSampleSize(),
                                                                                                  in.getDimension(),
                                                                                                  in.getInputDimension(),
                                                                                                  this.getID(),
                                                                                                  singleIdentifier,
                                                                                                  in.getIdentifier(),
                                                                                                  in.getTimeWindow() );
        return this.getDataFactory().ofDurationScoreOutput( returnMe, meta );
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class TimingErrorSummaryStatisticsBuilder
    {

        /**
         * The data factory.
         */

        private DataFactory dataFactory;

        /**
         * The identifier for the summary statistic.
         */

        private Set<MetricConstants> statistics = new HashSet<>();
        
        /**
         * The unique identifier associated with these summary statistics.
         */

        private MetricConstants identifier;

        /**
         * Sets the statistic.
         * 
         * @param statistics the identifier
         * @return the builder
         */

        public TimingErrorSummaryStatisticsBuilder setStatistics( Set<MetricConstants> statistics )
        {
            this.statistics.addAll( statistics );
            return this;
        }
        
        /**
         * Sets the unique identifier for the summary statistics.
         * 
         * @param identifier the identifier
         * @return the builder
         */

        public TimingErrorSummaryStatisticsBuilder setID( MetricConstants identifier )
        {
            this.identifier = identifier;
            return this;
        }
        
        /**
         * Set the output factory.
         * 
         * @param dataFactory the output factory
         * @return the builder
         */

        public TimingErrorSummaryStatisticsBuilder setOutputFactory( final DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

        /**
         * Build.
         * 
         * @return the summary statistics
         * @throws MetricParameterException if one or more parameters is invalid
         */

        public TimingErrorSummaryStatistics build() throws MetricParameterException
        {
            return new TimingErrorSummaryStatistics( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    TimingErrorSummaryStatistics( final TimingErrorSummaryStatisticsBuilder builder ) throws MetricParameterException
    {
        if ( Objects.isNull( builder ) )
        {
            throw new MetricParameterException( "Cannot construct the metric with a null builder." );
        }

        this.dataFactory = builder.dataFactory;

        if ( Objects.isNull( this.dataFactory ) )
        {
            throw new MetricParameterException( "Specify a data factory with which to build the metric." );
        }
        
        this.identifier = builder.identifier;
        
        if ( Objects.isNull( this.identifier ) )
        {
            throw new MetricParameterException( "Specify a unique identifier from which to build the statistics." );
        }

        // Copy locally
        Set<MetricConstants> input = new HashSet<>( builder.statistics );

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
     * Returns the data factory.
     * 
     * @return the data factory
     */

    private DataFactory getDataFactory()
    {
        return this.dataFactory;
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
