package wres.datamodel.sampledata.pairs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

import org.apache.commons.lang3.tuple.Pair;

/**
 * <p>A collection of {@link TimeSeries} of paired values.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfPairs<L, R> implements SampleData<Pair<L, R>>, Supplier<List<TimeSeries<Pair<L, R>>>>
{

    /**
     * Warning for null input.
     */

    private static final String NULL_INPUT = "Specify non-null time-series input.";

    /**
     * Main pairs.
     */

    private final List<TimeSeries<Pair<L, R>>> main;

    /**
     * Baseline pairs.
     */

    private final List<TimeSeries<Pair<L, R>>> baseline;

    /**
     * Metadata associated with the verification pairs.
     */

    private final SampleMetadata mainMeta;

    /**
     * Climatological data.
     */

    private final VectorOfDoubles climatology;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final SampleMetadata baselineMeta;

    @Override
    public TimeSeriesOfPairs<L, R> getBaselineData()
    {
        // TODO: override hasBaseline to check the actual data
        // and then return an empty baseline in all cases. The check
        // should be the formal check. Returning a null from an API method
        // is coding graffiti
        
        if ( Objects.isNull( this.baseline ) )
        {
            return null;
        }

        TimeSeriesOfPairsBuilder<L, R> builder = new TimeSeriesOfPairsBuilder<>();
        builder.setMetadata( this.baselineMeta );

        for ( TimeSeries<Pair<L, R>> next : baseline )
        {
            builder.addTimeSeries( next );
        }

        return builder.build();
    }

    @Override
    public List<TimeSeries<Pair<L, R>>> get()
    {
        return main; //Immutable on construction
    }

    @Override
    public String toString()
    {
        StringJoiner first = new StringJoiner( "" );
        first.add( this.getClass().getSimpleName() + "@" + this.hashCode() + ": " );
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );

        for ( TimeSeries<Pair<L, R>> nextSeries : this.main )
        {
            joiner.add( nextSeries.toString() );
        }

        return first.merge( joiner ).toString();
    }

    @Override
    public List<Pair<L, R>> getRawData()
    {
        List<Pair<L, R>> rawData = new ArrayList<>();

        List<TimeSeries<Pair<L, R>>> data = this.get();

        for ( TimeSeries<Pair<L, R>> next : data )
        {
            rawData.addAll( next.getEvents()
                                .stream()
                                .map( Event::getValue )
                                .collect( Collectors.toList() ) );
        }

        return Collections.unmodifiableList( rawData );
    }

    @Override
    public SampleMetadata getMetadata()
    {
        return this.mainMeta;
    }

    @Override
    public VectorOfDoubles getClimatology()
    {
        return this.climatology;
    }

    /**
     * A builder to build the time-series.
     */

    public static class TimeSeriesOfPairsBuilder<L, R>
    {

        /**
         * The raw data.
         */

        private List<TimeSeries<Pair<L, R>>> main = new ArrayList<>();

        /**
         * The raw data for the baseline
         */

        private List<TimeSeries<Pair<L, R>>> baseline = new ArrayList<>();

        /**
         * Metadata associated with the verification pairs.
         */

        private SampleMetadata mainMeta;

        /**
         * Metadata associated with the baseline verification pairs (may be null).
         */

        private SampleMetadata baselineMeta;

        /**
         * Climatological data.
         */

        private VectorOfDoubles climatology;

        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfPairsBuilder<L, R> addTimeSeries( TimeSeries<Pair<L, R>> timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

            this.main.add( timeSeries );

            return this;
        }

        /**
         * Adds a time-series to the builder for a baseline dataset.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfPairsBuilder<L, R> addTimeSeriesForBaseline( TimeSeries<Pair<L, R>> timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

            this.baseline.add( timeSeries );

            return this;
        }
        
        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the time-series
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesOfPairsBuilder<L, R> addTimeSeries( TimeSeriesOfPairs<L, R> timeSeries )
        {
            Objects.requireNonNull( timeSeries, NULL_INPUT );

            this.main.addAll( timeSeries.get() );
            this.mainMeta = timeSeries.mainMeta;
            this.climatology = timeSeries.climatology;
            
            if( timeSeries.hasBaseline() )
            {
                TimeSeriesOfPairs<L, R> base = timeSeries.getBaselineData();
                this.baseline.addAll( base.get() );
                this.baselineMeta = timeSeries.baselineMeta;
            }
            
            return this;
        }        

        /**
         * Sets the metadata associated with the input.
         * 
         * @param mainMeta the metadata
         * @return the builder
         */

        public TimeSeriesOfPairsBuilder<L, R> setMetadata( SampleMetadata mainMeta )
        {
            this.mainMeta = mainMeta;

            return this;
        }

        /**
         * Sets the metadata associated with the baseline input.
         * 
         * @param baselineMeta the metadata for the baseline
         * @return the builder
         */

        public TimeSeriesOfPairsBuilder<L, R> setMetadataForBaseline( SampleMetadata baselineMeta )
        {
            this.baselineMeta = baselineMeta;

            return this;
        }

        /**
         * Sets a climatological dataset.
         * 
         * @param climatology the climatology
         * @return the builder
         */

        public TimeSeriesOfPairsBuilder<L, R> setClimatology( VectorOfDoubles climatology )
        {
            this.climatology = climatology;

            return this;
        }

        /**
         * Builds a time-series.
         * 
         * @return a time-series
         */

        public TimeSeriesOfPairs<L, R> build()
        {
            return new TimeSeriesOfPairs<>( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    TimeSeriesOfPairs( final TimeSeriesOfPairsBuilder<L, R> b )
    {
        //Ensure safe types
        this.main = Collections.unmodifiableList( b.main );
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;

        // Always set baseline metadata because null-status is validated
        this.baselineMeta = b.baselineMeta;

        // Baseline data?
        if ( Objects.nonNull( b.baselineMeta ) )
        {
            this.baseline = Collections.unmodifiableList( b.baseline );
        }
        else
        {
            this.baseline = null;
        }

        //Validate
        this.validateMainInput();
        this.validateBaselineInput();
        this.validateClimatologicalInput();
    }

    /**
     * Validates the main pairs and associated metadata after the constructor has copied it.
     * 
     * @throws SampleDataException if the input is invalid
     */

    private void validateMainInput()
    {

        if ( Objects.isNull( this.mainMeta ) )
        {
            throw new SampleDataException( "Specify non-null metadata for the time-series input." );
        }

        if ( Objects.isNull( this.main ) )
        {
            throw new SampleDataException( "Specify a non-null dataset for the time-series input." );
        }

        if ( this.main.contains( null ) )
        {
            throw new SampleDataException( "One or more of the time-series is null." );
        }

    }

    /**
     * Validates the baseline pairs and associated metadata after the constructor has copied it.
     * 
     * @throws SampleDataException if the baseline input is invalid
     */

    private void validateBaselineInput()
    {
        if ( Objects.isNull( this.baseline ) != Objects.isNull( this.baselineMeta ) )
        {
            throw new SampleDataException( "Specify a non-null baseline input and associated metadata or leave both "
                                           + "null. The null status of the data and metadata, respectively, is: ["
                                           + Objects.isNull( this.baseline )
                                           + ","
                                           + Objects.isNull( this.baselineMeta )
                                           + "]" );
        }

        if ( Objects.nonNull( baseline ) && baseline.contains( null ) )
        {
            throw new SampleDataException( "One or more of the baseline time-series is null." );
        }
    }

    /**
     * Validates the climatological input after the constructor has copied it.
     * 
     * @throws SampleDataException if the climatological input is invalid
     */

    private void validateClimatologicalInput()
    {
        // #65881: if a climatology is provided, it cannot be empty when some pairs exist
        if ( Objects.nonNull( this.getClimatology() ) && !this.getRawData().isEmpty() )
        {
            if ( this.getClimatology().size() == 0 )
            {
                throw new SampleDataException( "Cannot build the paired data with an empty climatology: add one or "
                                               + "more values." );
            }

            if ( !Arrays.stream( this.getClimatology().getDoubles() ).anyMatch( Double::isFinite ) )
            {
                throw new SampleDataException( "Must have at least one non-missing value in the climatological "
                                               + "input" );
            }
        }
    }

}
