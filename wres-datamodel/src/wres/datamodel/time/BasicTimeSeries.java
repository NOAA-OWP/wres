package wres.datamodel.time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import java.util.function.Supplier;

import wres.datamodel.sampledata.SampleDataException;

/**
 * Immutable base class for a time-series.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 */
public class BasicTimeSeries<T> implements Supplier<List<TimeSeries<T>>>
{

    /**
     * The raw data, which retains the composition of each individual time-series.
     */

    private final List<TimeSeries<T>> timeSeries;

    /**
     * Builds a time-series from the input.
     * 
     * @param <T> the type of event
     * @param timeSeries the input time-series data
     * @return a time-series
     */

    public static <T> BasicTimeSeries<T> of( List<TimeSeries<T>> timeSeries )
    {
        return new BasicTimeSeries<>( timeSeries );
    }

    /**
     * A default builder to build a time-series incrementally. Also see {@link BasicTimeSeries#of(List)}.
     */

    public static class BasicTimeSeriesBuilder<T>
    {

        /**
         * The raw data.
         */

        private List<TimeSeries<T>> data = new ArrayList<>();

        /**
         * Builds a time-series.
         * 
         * @return a time-series
         */
        
        public BasicTimeSeries<T> build()
        {
            return new BasicTimeSeries<>( this );
        }

        /**
         * Adds a time-series to the builder.
         * 
         * @param timeSeries the list of events
         * @return the builder
         * @throws NullPointerException if the input is null
         */
        
        public BasicTimeSeriesBuilder<T> addTimeSeries( TimeSeries<T> timeSeries )
        {
            this.data.add( timeSeries );

            return this;
        }

    }
    
    @Override
    public List<TimeSeries<T>> get()
    {
        return Collections.unmodifiableList( this.timeSeries );
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this.get() );
    }

    /**
     * Build the time-series.
     * 
     * @param timeSeries the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param referenceTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws SampleDataException if one or more inputs is invalid
     */

    BasicTimeSeries( final BasicTimeSeriesBuilder<T> builder )
    {
        this( builder.data );
    }

    /**
     * Build the time-series internally.
     * 
     * @param data the raw data
     * @param baselineData the raw data for the baseline (may be empty, cannot be null)
     * @param referenceTimeIterator a basis time iterator
     * @param durationIterator a duration iterator
     * @throws SampleDataException if one or more inputs is invalid
     */

    BasicTimeSeries( final List<TimeSeries<T>> data )
    {
        // Time-series cannot be null
        if ( Objects.isNull( data ) )
        {
            throw new SampleDataException( "The time-series input cannot be null." );
        }

        // Sets and validates       
        if ( data.contains( null ) )
        {
            throw new SampleDataException( "One or more time-series is null." );
        }

        this.timeSeries = Collections.unmodifiableList( data );
    }

}
