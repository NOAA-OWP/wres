package wres.datamodel.time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.sampledata.SampleDataException;

/**
 * Immutable base class for a time-series.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 */
public class BasicTimeSeries<T> implements TimeSeriesCollection<T>
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

    public static class BasicTimeSeriesBuilder<T> implements TimeSeriesCollectionBuilder<T>
    {

        /**
         * The raw data.
         */

        private List<TimeSeries<T>> data = new ArrayList<>();

        @Override
        public BasicTimeSeries<T> build()
        {
            return new BasicTimeSeries<>( this );
        }

        @Override
        public TimeSeriesCollectionBuilder<T> addTimeSeries( TimeSeries<T> timeSeries )
        {
            this.data.add( timeSeries );

            return this;
        }

    }
    
    @Override
    public List<TimeSeries<T>> getTimeSeries()
    {
        return Collections.unmodifiableList( this.timeSeries );
    }

    @Override
    public String toString()
    {
        return TimeSeriesHelper.toString( this );
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
