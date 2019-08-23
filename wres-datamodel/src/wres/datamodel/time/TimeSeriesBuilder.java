package wres.datamodel.time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.sampledata.SampleDataException;

/**
 * <p>A builder for {@link TimeSeries}. 
 * 
 * @param <T> the atomic type of data
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesBuilder<T>
{

    /**
     * Adds a time-series to the builder.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws SampleDataException if the specified input is inconsistent with any existing input
     * @throws NullPointerException if the input is null
     */

    default TimeSeriesBuilder<T> addTimeSeries( TimeSeries<T> timeSeries )
    {
        Objects.requireNonNull( timeSeries, "Specify non-null time-series input." );

        List<Event<T>> rawData = new ArrayList<>();
        timeSeries.eventIterator().forEach( rawData::add );       
        this.addTimeSeries( Collections.unmodifiableList( rawData ) );

        return this;
    }

    /**
     * Adds a time-series to the builder.
     * 
     * @param timeSeries the list of events
     * @return the builder
     * @throws NullPointerException if the input is null
     */

    TimeSeriesBuilder<T> addTimeSeries( List<Event<T>> timeSeries );

    /**
     * Builds a time-series.
     * 
     * @return a time-series
     */

    TimeSeries<T> build();

}
