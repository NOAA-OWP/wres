package wres.datamodel.sampledata.pairs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.stream.Collectors;

import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.time.BasicTimeSeries;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCollection;
import wres.datamodel.time.TimeSeriesCollectionBuilder;
import wres.datamodel.time.TimeSeriesHelper;

/**
 * <p>A {@link TimeSeriesCollection} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfSingleValuedPairs extends SingleValuedPairs implements TimeSeriesCollection<SingleValuedPair>
{

    /**
     * Warning for null input.
     */
    
    private static final String NULL_INPUT = "Specify non-null time-series input.";
    
    /**
     * Instance of base class for a time-series of pairs.
     */

    private final BasicTimeSeries<SingleValuedPair> main;

    /**
     * Instance of base class for a baseline time-series of pairs.
     */

    private final BasicTimeSeries<SingleValuedPair> baseline;

    @Override
    public TimeSeriesOfSingleValuedPairs getBaselineData()
    {
        if ( Objects.isNull( this.getRawDataForBaseline() ) )
        {
            return null;
        }
        
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();
        builder.setMetadata( this.getMetadataForBaseline() );
        
        for( TimeSeries<SingleValuedPair> next : baseline.referenceTimeIterator() )
        {
            builder.addTimeSeries( next );
        }
        
        return builder.build();
    }

    @Override
    public Iterable<Event<SingleValuedPair>> eventIterator()
    {
        return main.eventIterator();
    }

    @Override
    public Iterable<TimeSeries<SingleValuedPair>> referenceTimeIterator()
    {
        return main.referenceTimeIterator();
    }

    @Override
    public SortedSet<Instant> getReferenceTimes()
    {
        return Collections.unmodifiableSortedSet( main.getReferenceTimes() );
    }

    @Override
    public SortedSet<Duration> getDurations()
    {
        return Collections.unmodifiableSortedSet( main.getDurations() );
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this );
    }

    /**
     * A builder to build the metric input.
     */

    public static class TimeSeriesOfSingleValuedPairsBuilder extends SingleValuedPairsBuilder
            implements TimeSeriesCollectionBuilder<SingleValuedPair>
    {

        /**
         * The raw data.
         */

        private List<TimeSeries<SingleValuedPair>> data = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<TimeSeries<SingleValuedPair>> baselineData = null;
        
        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */
        @Override
        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeries( TimeSeries<SingleValuedPair> timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );
            
            this.data.add( timeSeries );
            
            this.addData( timeSeries.getEvents()
                                    .stream()
                                    .map( Event::getValue )
                                    .collect( Collectors.toList() ) );
            
            return this;
        }
        
        /**
         * Adds a time-series to the builder for a baseline dataset.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesForBaseline( TimeSeries<SingleValuedPair> timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

            if( Objects.isNull( this.baselineData ) )
            {
                this.baselineData = new ArrayList<>();
            }
            
            this.baselineData.add( timeSeries );
            
            this.addDataForBaseline( timeSeries.getEvents()
                                               .stream()
                                               .map( Event::getValue )
                                               .collect( Collectors.toList() ) );

            return this;
        }

        /**
         * Adds a time-series to the builder, including any baseline.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeries( TimeSeriesOfSingleValuedPairs timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

            for( TimeSeries<SingleValuedPair> next : timeSeries.referenceTimeIterator() )
            {
                this.addTimeSeries( next );
            }

            // Set the union of the current metadata and any previously added time-series
            List<SampleMetadata> mainMeta = new ArrayList<>();
            mainMeta.add( timeSeries.getMetadata() );
            if ( Objects.nonNull( this.mainMeta ) )
            {
                mainMeta.add( this.mainMeta );
            }
            this.setMetadata( SampleMetadata.unionOf( mainMeta ) );

            //Add the baseline data if required
            if ( timeSeries.hasBaseline() )
            {
                this.addTimeSeriesForBaseline( timeSeries.getBaselineData() );
            }

            this.setClimatology( timeSeries.getClimatology() );

            return this;
        }

        /**
         * Adds a time-series to the builder as a baseline dataset only. Any data associated with the 
         * {@link TimeSeriesOfSingleValuedPairs#getBaselineData()} of the input is ignored.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesForBaseline( TimeSeriesOfSingleValuedPairs timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );
            
            for( TimeSeries<SingleValuedPair> next : timeSeries.referenceTimeIterator() )
            {
                this.addTimeSeriesForBaseline( next );
            }

            // Set the union of the current metadata and any previously added time-series
            List<SampleMetadata> baselineMeta = new ArrayList<>();

            // Metadata, as with data, is taken from the main input
            baselineMeta.add( timeSeries.getMetadata() );
            if ( Objects.nonNull( this.baselineMeta ) )
            {
                baselineMeta.add( this.baselineMeta );
            }
            this.setMetadataForBaseline( SampleMetadata.unionOf( baselineMeta ) );

            return this;
        }

        /**
         * Builds a time-series.
         * 
         * @return a time-series
         */

        @Override
        public TimeSeriesOfSingleValuedPairs build()
        {
            return new TimeSeriesOfSingleValuedPairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    TimeSeriesOfSingleValuedPairs( final TimeSeriesOfSingleValuedPairsBuilder b )
    {
        super( b );
        this.main = BasicTimeSeries.of( b.data );
        // Baseline data?
        if ( Objects.nonNull( b.baselineData ) )
        {
            this.baseline = BasicTimeSeries.of( b.baselineData );
        }
        else
        {
            this.baseline = null;
        }
    }

}
