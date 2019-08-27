package wres.datamodel.sampledata.pairs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * A collection of {@link TimeSeries} of {@link EnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfEnsemblePairs extends EnsemblePairs implements Supplier<List<TimeSeries<EnsemblePair>>>
{

    /**
     * Warning for null input.
     */

    private static final String NULL_INPUT = "Specify non-null time-series input.";

    /**
     * Main pairs.
     */

    private final List<TimeSeries<EnsemblePair>> main;

    /**
     * Baseline pairs.
     */

    private final List<TimeSeries<EnsemblePair>> baseline;

    @Override
    public TimeSeriesOfEnsemblePairs getBaselineData()
    {
        if ( Objects.isNull( this.getRawDataForBaseline() ) )
        {
            return null;
        }

        TimeSeriesOfEnsemblePairsBuilder builder = new TimeSeriesOfEnsemblePairsBuilder();
        builder.setMetadata( this.getMetadataForBaseline() );

        for ( TimeSeries<EnsemblePair> next : baseline )
        {
            builder.addTimeSeries( next );
        }

        return builder.build();
    }

    @Override
    public List<TimeSeries<EnsemblePair>> get()
    {
        return main; // Immutable on construction
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        
        for ( TimeSeries<EnsemblePair> nextSeries : this.main )
        {
            for ( Event<EnsemblePair> nextEvent : nextSeries.getEvents() )
            {
                joiner.add( nextEvent.toString() );
            }
        }
    
        return joiner.toString();
    }

    /**
     * A builder to build the metric input.
     */

    public static class TimeSeriesOfEnsemblePairsBuilder extends EnsemblePairsBuilder
    {

        /**
         * The raw data.
         */

        private List<TimeSeries<EnsemblePair>> data = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<TimeSeries<EnsemblePair>> baselineData = new ArrayList<>();

        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws SampleDataException if the specified input is inconsistent with any existing input
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeries( TimeSeries<EnsemblePair> timeSeries )
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

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeriesForBaseline( TimeSeries<EnsemblePair> timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

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

        public TimeSeriesOfEnsemblePairsBuilder addTimeSeries( TimeSeriesOfEnsemblePairs timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

            for ( TimeSeries<EnsemblePair> next : timeSeries.get() )
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

            for ( TimeSeries<EnsemblePair> next : timeSeries.get() )
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
        this.main = Collections.unmodifiableList( b.data );
        this.baseline = Collections.unmodifiableList( b.baselineData );
    }

}
