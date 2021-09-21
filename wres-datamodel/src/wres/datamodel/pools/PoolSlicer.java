package wres.datamodel.pools;

import java.util.List;
import java.util.Objects;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.pools.Pool.Builder;
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
     * Failure to supply a non-null predicate.
     */

    private static final String NULL_PREDICATE_EXCEPTION = "Specify a non-null predicate.";
    
    /**
     * Null mapper function error message.
     */

    private static final String NULL_MAPPER_EXCEPTION = "Specify a non-null function to map the input to an output.";
    
    /**
     * Null input error message.
     */
    private static final String NULL_INPUT_EXCEPTION = "Specify a non-null input.";
    
    /**
     * Transforms the input type to another type.
     * 
     * @param <S> the input type
     * @param <T> the output type
     * @param input the input
     * @param transformer the transformer
     * @return the transformed type
     * @throws NullPointerException if either input is null
     */
    
    public static <S, T> Pool<T> transform( Pool<S> input, Function<S, T> transformer )
    {
        Objects.requireNonNull( input, PoolSlicer.NULL_INPUT_EXCEPTION );
    
        Objects.requireNonNull( transformer, PoolSlicer.NULL_MAPPER_EXCEPTION );
    
        Builder<T> builder = new Builder<>();
    
        builder.setClimatology( input.getClimatology() )
               .setMetadata( input.getMetadata() );
    
        // Add the main series
        for ( S next : input.get() )
        {
            T transformed = transformer.apply( next );
            if ( Objects.nonNull( transformed ) )
            {
                builder.addData( transformed );
            }
        }
    
        // Add the baseline series if available
        if ( input.hasBaseline() )
        {
            Pool<S> baseline = input.getBaselineData();
    
            for ( S next : baseline.get() )
            {
                T transformed = transformer.apply( next );
                if ( Objects.nonNull( transformed ) )
                {
                    builder.addDataForBaseline( transformed );
                }
            }
    
            builder.setMetadataForBaseline( baseline.getMetadata() );
        }
    
        return builder.build();
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input.
     * 
     * @param <T> the type of data
     * @param input the data to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */
    
    public static <T> Pool<T> filter( Pool<T> input,
                                      Predicate<T> condition,
                                      DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, PoolSlicer.NULL_INPUT_EXCEPTION );
    
        Objects.requireNonNull( condition, PoolSlicer.NULL_PREDICATE_EXCEPTION );
    
        Builder<T> builder = new Builder<>();
    
        List<T> mainPairs = input.get();
        List<T> mainPairsSubset =
                mainPairs.stream().filter( condition ).collect( Collectors.toList() );
    
        builder.addData( mainPairsSubset ).setMetadata( input.getMetadata() );
    
        //Filter climatology as required
        if ( input.hasClimatology() )
        {
            VectorOfDoubles climatology = input.getClimatology();
    
            if ( Objects.nonNull( applyToClimatology ) )
            {
                climatology = Slicer.filter( input.getClimatology(), applyToClimatology );
            }
    
            builder.setClimatology( climatology );
        }
    
        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            Pool<T> baseline = input.getBaselineData();
            List<T> basePairs = baseline.get();
            List<T> basePairsSubset =
                    basePairs.stream().filter( condition ).collect( Collectors.toList() );
    
            builder.addDataForBaseline( basePairsSubset ).setMetadataForBaseline( baseline.getMetadata() );
        }
    
        return builder.build();
    }    
    
    /**
     * Counts the number of pairs in a pool of time-series.
     * 
     * @param <U> the type of time-series data
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
     * @param <U> the type of time-series data
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
