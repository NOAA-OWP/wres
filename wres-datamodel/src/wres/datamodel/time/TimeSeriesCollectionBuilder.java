package wres.datamodel.time;

/**
 * <p>A builder for {@link TimeSeriesCollection}. 
 * 
 * @param <T> the atomic type of data
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesCollectionBuilder<T>
{

    /**
     * Adds a time-series to the builder.
     * 
     * @param timeSeries the list of events
     * @return the builder
     * @throws NullPointerException if the input is null
     */

    TimeSeriesCollectionBuilder<T> addTimeSeries( TimeSeries<T> timeSeries );

    /**
     * Builds a time-series.
     * 
     * @return a time-series
     */

    TimeSeriesCollection<T> build();

}
