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
import wres.datamodel.time.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesHelper;

/**
 * Immutable implementation of a possibly irregular {@link TimeSeries} of {@link EnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfEnsemblePairs extends EnsemblePairs implements TimeSeries<EnsemblePair>
{

    /**
     * Warning for null input.
     */
    
    private static final String NULL_INPUT = "Specify non-null time-series input.";
    
    /**
     * Instance of base class for a time-series of pairs.
     */

    private final BasicTimeSeries<EnsemblePair> main;

    /**
     * Instance of base class for a time-series of baseline pairs.
     */

    private final BasicTimeSeries<EnsemblePair> baseline;

    @Override
    public TimeSeriesOfEnsemblePairs getBaselineData()
    {
        if ( Objects.isNull( this.getRawDataForBaseline() ) )
        {
            return null;
        }
        
        TimeSeriesOfEnsemblePairsBuilder builder = new TimeSeriesOfEnsemblePairsBuilder();
        builder.addTimeSeries( baseline ).setMetadata( getMetadataForBaseline() );
        return builder.build();
    }

    @Override
    public Iterable<Event<EnsemblePair>> timeIterator()
    {
        return main.timeIterator();
    }

    @Override
    public Iterable<TimeSeries<EnsemblePair>> basisTimeIterator()
    {
        return main.basisTimeIterator();
    }

    @Override
    public Iterable<TimeSeries<EnsemblePair>> durationIterator()
    {
        return main.durationIterator();
    }

    @Override
    public SortedSet<Instant> getBasisTimes()
    {
        return Collections.unmodifiableSortedSet( main.getBasisTimes() );
    }

    @Override
    public SortedSet<Duration> getDurations()
    {
        return Collections.unmodifiableSortedSet( main.getDurations() );
    }

    @Override
    public boolean hasMultipleTimeSeries()
    {
        return main.hasMultipleTimeSeries();
    }

    @Override
    public boolean isRegular()
    {
        return main.isRegular();
    }

    @Override
    public Duration getRegularDuration()
    {
        return main.getRegularDuration();
    }

    @Override
    public Instant getEarliestBasisTime()
    {
        return main.getEarliestBasisTime();
    }

    @Override
    public Instant getLatestBasisTime()
    {
        return main.getLatestBasisTime();
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this );
    }

    /**
     * A builder to build the metric input.
     */

    public static class TimeSeriesOfEnsemblePairsBuilder extends EnsemblePairsBuilder
            implements TimeSeriesBuilder<EnsemblePair>
    {

        /**
         * The raw data.
         */

        private List<List<Event<EnsemblePair>>> data = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<List<Event<EnsemblePair>>> baselineData = null;
        
        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */
        @Override
        public TimeSeriesOfEnsemblePairsBuilder addTimeSeries( TimeSeries<EnsemblePair> timeSeries )
        {
            TimeSeriesBuilder.super.addTimeSeries( timeSeries );
            return this;
        }

        /**
         * Adds a list of atomic time-series to the builder, each one stored against its basis time.
         * 
         * @param values the time-series, stored against their basis times
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeries( List<Event<EnsemblePair>> values )
        {
            Objects.requireNonNull( values, NULL_INPUT );

            this.data.add( values );
            addData( values.stream().map( Event::getValue ).collect( Collectors.toList() ) );
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

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesForBaseline( TimeSeries<EnsemblePair> timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

            List<Event<EnsemblePair>> values = new ArrayList<>();
            timeSeries.timeIterator().forEach( values::add );
            this.addTimeSeriesDataForBaseline( values );

            return this;
        }

        /**
         * Adds a list of atomic time-series to the builder for a baseline, each one stored against its basis time.
         * 
         * @param values the time-series, stored against their basis times
         * @return the builder
         */

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesDataForBaseline( List<Event<EnsemblePair>> values )
        {
            if ( Objects.nonNull( values ) )
            {
                if ( Objects.isNull( this.baselineData ) )
                {
                    this.baselineData = new ArrayList<>();
                }

                this.baselineData.add( values );
                this.addDataForBaseline( values.stream().map( Event::getValue ).collect( Collectors.toList() ) );
            }
            
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

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeries( TimeSeriesOfEnsemblePairs timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );
            
            List<Event<EnsemblePair>> nextSource = new ArrayList<>();
            timeSeries.timeIterator().forEach( nextSource::add );

            this.addTimeSeries( nextSource );

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
         * {@link TimeSeriesOfEnsemblePairs#getBaselineData()} of the input is ignored.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesForBaseline( TimeSeriesOfEnsemblePairs timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );
            
            List<Event<EnsemblePair>> nextSource = new ArrayList<>();
            timeSeries.timeIterator().forEach( nextSource::add );

            this.addTimeSeriesDataForBaseline( nextSource );

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
        public TimeSeriesOfEnsemblePairs build()
        {
            return new TimeSeriesOfEnsemblePairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    TimeSeriesOfEnsemblePairs( final TimeSeriesOfEnsemblePairsBuilder b )
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
