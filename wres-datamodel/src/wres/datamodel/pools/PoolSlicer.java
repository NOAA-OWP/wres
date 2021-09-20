package wres.datamodel.pools;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.Slicer;
import wres.datamodel.time.TimeSeriesSlicer;

/**
 * A utility class for slicing/dicing and transforming pool-shaped datasets
 * 
 * @author James Brown
 * @see    Slicer
 * @see    TimeSeriesSlicer
 */

public class PoolSlicer
{

    /**
     * Counts the number of pairs in a pool of time-series.
     * 
     * @param pool the pool
     * @return the number of pairs
     * @throws NullPointerException if the input is null
     */

    public static <U> int getPairCount( Pool<TimeSeries<U>> pool )
    {
        Objects.requireNonNull( pool );

        return pool.get()
                   .stream()
                   .mapToInt( next -> next.getEvents().size() )
                   .sum();
    }

    /**
     * Unpacks a pool of time-series into their raw event values, eliminating the time-series view.
     * 
     * @param pool the pool
     * @return the unpacked pool
     * @throws NullPointerException if the input is null
     */

    public static <U> Pool<U> unpack( Pool<TimeSeries<U>> pool )
    {
        Objects.requireNonNull( pool );

        List<U> sampleData = pool.get()
                                 .stream()
                                 .flatMap( next -> next.getEvents().stream() )
                                 .map( Event::getValue )
                                 .collect( Collectors.toUnmodifiableList() );

        List<U> baselineSampleData = null;
        PoolMetadata baselineMetadata = null;

        if ( pool.hasBaseline() )
        {
            baselineSampleData = pool.getBaselineData()
                                     .get()
                                     .stream()
                                     .flatMap( next -> next.getEvents().stream() )
                                     .map( Event::getValue )
                                     .collect( Collectors.toUnmodifiableList() );

            baselineMetadata = pool.getBaselineData().getMetadata();
        }

        return Pool.of( sampleData,
                        pool.getMetadata(),
                        baselineSampleData,
                        baselineMetadata,
                        pool.getClimatology() );
    }

    /**
     * Do not construct.
     */
    private PoolSlicer()
    {
    }

}
