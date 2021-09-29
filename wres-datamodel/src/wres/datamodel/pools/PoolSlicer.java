package wres.datamodel.pools;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool.Builder;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;

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
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PoolSlicer.class );

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
     * @param pool the input
     * @param transformer the transformer
     * @return the transformed type
     * @throws NullPointerException if either input is null
     */

    public static <S, T> Pool<T> transform( Pool<S> pool, Function<S, T> transformer )
    {
        Objects.requireNonNull( pool, PoolSlicer.NULL_INPUT_EXCEPTION );
        Objects.requireNonNull( transformer, PoolSlicer.NULL_MAPPER_EXCEPTION );

        Pool.Builder<T> poolBuilder = new Pool.Builder<T>().setMetadata( pool.getMetadata() );

        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
        }

        // Preserve any small pools
        for ( Pool<S> next : pool.getMiniPools() )
        {
            Pool<T> transformed = PoolSlicer.transformInner( next, transformer );
            poolBuilder.addPool( transformed, false );
        }

        return poolBuilder.build();
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input.
     * 
     * @param <T> the type of data
     * @param pool the data to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static <T> Pool<T> filter( Pool<T> pool,
                                      Predicate<T> condition,
                                      DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( pool, PoolSlicer.NULL_INPUT_EXCEPTION );
        Objects.requireNonNull( condition, PoolSlicer.NULL_PREDICATE_EXCEPTION );

        Pool.Builder<T> poolBuilder = new Pool.Builder<T>().setMetadata( pool.getMetadata() );

        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
        }

        // Preserve any small pools
        for ( Pool<T> next : pool.getMiniPools() )
        {
            Pool<T> filtered = PoolSlicer.filterInner( next, condition, applyToClimatology );
            poolBuilder.addPool( filtered, false );
        }

        return poolBuilder.build();
    }

    /**
     * Applies an attribute-specific filter to the corresponding attribute-specific subset of pairs in the input and 
     * returns the union of those filtered subsets for a metadata attribute that is extracted from the pool metadata
     * with a mapper.
     * 
     * @param <S> the pooled data type
     * @param <T> the metadata attribute
     * @param pool the pool to filter
     * @param filters the filters to use
     * @param metaMapper the function to extract an attribute from the metadata
     * @return the union of the subsets, each subset filtered by an attribute-specific predicate
     * @throws NullPointerException if any input is null
     * @throws PoolException if the pool could not be filtered for any reason
     */

    public static <S, T extends Comparable<T>> Pool<S> filter( Pool<S> pool,
                                                               Map<T, Predicate<S>> filters,
                                                               Function<PoolMetadata, T> metaMapper )
    {
        Objects.requireNonNull( pool );
        Objects.requireNonNull( filters );
        Objects.requireNonNull( metaMapper );

        // Optimization for no data
        if ( pool.get().isEmpty() && ( !pool.hasBaseline() || pool.getBaselineData().get().isEmpty() )
             && ( !pool.hasClimatology() || pool.getClimatology().size() == 0 ) )
        {
            return pool;
        }

        // Small pools
        Map<T, Pool<S>> pools = PoolSlicer.decompose( metaMapper, pool );

        // Iterate the pools and apply the filters
        Set<T> keysWithoutAFilter = new HashSet<>();

        Pool.Builder<S> poolBuilder = new Pool.Builder<S>().setMetadata( pool.getMetadata() );

        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
        }

        for ( Map.Entry<T, Pool<S>> nextEntry : pools.entrySet() )
        {
            T nextKey = nextEntry.getKey();

            Pool<S> nextPool = nextEntry.getValue();
            Predicate<S> nextPredicate = filters.get( nextKey );

            if ( Objects.nonNull( nextPredicate ) )
            {
                Pool<S> filtered = PoolSlicer.filter( nextPool, nextPredicate, null );
                poolBuilder.addPool( filtered, false );
            }
            else
            {
                keysWithoutAFilter.add( nextKey );
            }
        }

        // Handle cases with no data or some missing data
        if ( keysWithoutAFilter.size() == pools.size() )
        {
            throw new PoolException( "Failed to filter pool " + pool.getMetadata()
                                     + ". After decomposing the pool into smaller pools by metadata attribute, failed "
                                     + "to identify a filter for any of these attribute instances: "
                                     + keysWithoutAFilter
                                     + "." );
        }
        else if ( !keysWithoutAFilter.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "When filtering pool {} into smaller pools by metadata attribute, failed to correlate some "
                         + "attributes with filters: {}. Consequently, no filtered pool was identified for any of "
                         + "these attribute instances and they will not be included in the filtered pool.",
                         pool.getMetadata(),
                         keysWithoutAFilter );
        }

        return poolBuilder.build();
    }

    /**
     * Applies an attribute-specific transformer  to the corresponding attribute-specific subset of pairs in the input 
     * and returns the union of those transformed subsets for a metadata attribute that is extracted from the pool 
     * metadata with a mapper.
     * 
     * @param <S> the pooled data type
     * @param <T> the metadata attribute
     * @param <U> the transformed pool data type
     * @param pool the pool to transform
     * @param transformers the transformers to use
     * @param metaMapper the function to extract an attribute from the metadata
     * @return the union of the subsets, each subset filtered by an attribute-specific predicate
     * @throws NullPointerException if any input is null
     * @throws PoolException if the pool could not be filtered for any reason
     */

    public static <S, T extends Comparable<T>, U> Pool<U> transform( Pool<S> pool,
                                                                     Map<T, Function<S, U>> transformers,
                                                                     Function<PoolMetadata, T> metaMapper )
    {
        Objects.requireNonNull( pool );
        Objects.requireNonNull( transformers );
        Objects.requireNonNull( metaMapper );

        // Small pools
        Map<T, Pool<S>> pools = PoolSlicer.decompose( metaMapper, pool );

        // Iterate the pools and apply the transformers
        Set<T> keysWithoutAFilter = new HashSet<>();

        Pool.Builder<U> poolBuilder = new Pool.Builder<U>().setMetadata( pool.getMetadata() );

        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
        }

        for ( Map.Entry<T, Pool<S>> nextEntry : pools.entrySet() )
        {
            T nextKey = nextEntry.getKey();

            Pool<S> nextPool = nextEntry.getValue();
            Function<S, U> nextTransformer = transformers.get( nextKey );

            if ( Objects.nonNull( nextTransformer ) )
            {
                Pool<U> transformed = PoolSlicer.transform( nextPool, nextTransformer );
                poolBuilder.addPool( transformed, false );
            }
            else
            {
                keysWithoutAFilter.add( nextKey );
            }
        }

        // Handle cases with no data or some missing data
        if ( keysWithoutAFilter.size() == pools.size() )
        {
            throw new PoolException( "Failed to transform pool " + pool.getMetadata()
                                     + ". After decomposing the pool into smaller pools by metadata attribute, failed "
                                     + "to identify a transformer for any of these attribute instances: "
                                     + keysWithoutAFilter
                                     + "." );
        }
        else if ( !keysWithoutAFilter.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "When filtering pool {} into smaller pools by metadata attribute, failed to correlate some "
                         + "attributes with transformers: {}. Consequently, no transformed pool was identified for any "
                         + "of these attribute instances and they will not be included in the transformed pool.",
                         pool.getMetadata(),
                         keysWithoutAFilter );
        }

        return poolBuilder.build();
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

        Pool.Builder<U> poolBuilder = new Pool.Builder<U>().setMetadata( pool.getMetadata() );

        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
        }

        // Preserve any small pools
        for ( Pool<TimeSeries<U>> nextMiniPool : pool.getMiniPools() )
        {
            Pool<U> nextUnpacked = PoolSlicer.unpackInner( nextMiniPool );
            poolBuilder.addPool( nextUnpacked, false );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Unpacked pool {}.", pool.getMetadata() );
        }

        return poolBuilder.build();
    }

    /**
     * Decomposes a pool into a collection of mini-pools based on the mini-pools available and a prescribed attribute
     * of the pool metadata.
     * 
     * @param <S> the key against which pools should be mapped
     * @param <T> the type of pooled data
     * @param metaMapper the metadata mapper
     * @param pool the pool
     * @return a decomposed list of mini-pools based on their metadata
     * @throws IllegalArgumentException if multiple pools map to the same key
     */

    public static <S extends Comparable<S>, T> Map<S, Pool<T>> decompose( Function<PoolMetadata, S> metaMapper,
                                                                          Pool<T> pool )
    {
        Objects.requireNonNull( metaMapper );
        Objects.requireNonNull( pool );

        // Use a sorted map implementation to preserve order
        Map<S, Pool<T>> returnMe = new TreeMap<>();

        for ( Pool<T> nextPool : pool.getMiniPools() )
        {
            S key = metaMapper.apply( nextPool.getMetadata() );

            if ( returnMe.containsKey( key ) )
            {
                throw new IllegalArgumentException( "Could not decompose the input pool because several mini-pools all "
                                                    + "map to the same key of '"
                                                    + key
                                                    + "'." );
            }

            returnMe.put( key, nextPool );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            Set<PoolMetadata> metadatas = returnMe.values()
                                                  .stream()
                                                  .map( Pool::getMetadata )
                                                  .collect( Collectors.toSet() );

            LOGGER.debug( "Decomposed pool {} into {} mini-pools as follows: {}.",
                          pool.getMetadata(),
                          returnMe.size(),
                          metadatas );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Returns a mapper for mapping {@link PoolMetadata} to a {@link FeatureTuple}. When applied, the function throws 
     * an exception if the pool does not contain precisely one {@link FeatureTuple}.
     * 
     * @return a mapper
     */

    public static Function<PoolMetadata, FeatureTuple> getFeatureMapper()
    {
        return metadata -> {
            Set<FeatureTuple> features = metadata.getFeatureTuples();

            if ( features.size() != 1 )
            {
                throw new PoolException( "Could not filter the outer pool " + metadata
                                         + " because the inner pool "
                                         + metadata
                                         + " does not compose a single "
                                         + "feature, rather these features: "
                                         + features
                                         + "." );
            }

            return features.iterator().next();
        };
    }

    /**
     * Returns <code>true</code> if the two metadatas are equal after ignoring the time windows, thresholds and 
     * features.
     * 
     * @param first the first metadata to test for conditional equality with the second
     * @param second the second metadata to test for conditional equality with the first
     * @return true if the metadatas are conditionally equal
     */

    public static boolean equalsWithoutTimeWindowOrThresholdsOrFeatures( PoolMetadata first, PoolMetadata second )
    {
        if ( Objects.isNull( first ) != Objects.isNull( second ) )
        {
            return false;
        }

        if ( Objects.isNull( first ) )
        {
            return true;
        }

        // Adjust the pools to remove the time window and thresholds and feature-ish information
        wres.statistics.generated.Pool adjustedPoolFirst = first.getPool()
                                                                .toBuilder()
                                                                .clearTimeWindow()
                                                                .clearEventThreshold()
                                                                .clearDecisionThreshold()
                                                                .clearGeometryTuples()
                                                                .clearRegionName()
                                                                .build();

        wres.statistics.generated.Pool adjustedPoolSecond = second.getPool()
                                                                  .toBuilder()
                                                                  .clearTimeWindow()
                                                                  .clearEventThreshold()
                                                                  .clearDecisionThreshold()
                                                                  .clearGeometryTuples()
                                                                  .clearRegionName()
                                                                  .build();

        return first.getEvaluation().equals( second.getEvaluation() )
               && adjustedPoolFirst.equals( adjustedPoolSecond );
    }

    /**
     * Returns the union of the supplied metadata. All components of the input must be equal in terms of 
     * {@link #equalsWithoutTimeWindowOrThresholdsOrFeatures(PoolMetadata, PoolMetadata)}.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if the input is null
     * @throws PoolMetadataException if the metadatas could not be merged
     */

    public static PoolMetadata unionOf( List<PoolMetadata> input )
    {
        String nullString = "Cannot find the union of null metadata.";

        Objects.requireNonNull( input, nullString );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot find the union of empty input." );
        }

        // Preserve order because the message models these as lists
        Set<TimeWindowOuter> unionWindows = new TreeSet<>();
        Set<FeatureTuple> unionFeatures = new TreeSet<>();
        Set<OneOrTwoThresholds> unionThresholds = new TreeSet<>();
        Set<String> unionRegionNames = new TreeSet<>();

        // Test entry
        PoolMetadata test = input.get( 0 );

        // Validate for equivalence with the first entry and add window to list
        for ( PoolMetadata next : input )
        {
            Objects.requireNonNull( next, nullString );

            if ( !PoolSlicer.equalsWithoutTimeWindowOrThresholdsOrFeatures( next, test ) )
            {
                throw new PoolMetadataException( "Only the time window and thresholds and features can differ when "
                                                 + "finding the union of metadata. The metadata for comparison was: "
                                                 + test
                                                 + ". The metadata being compared was: "
                                                 + next
                                                 + "." );
            }

            if ( next.hasTimeWindow() )
            {
                unionWindows.add( next.getTimeWindow() );
            }

            if ( next.hasThresholds() )
            {
                unionThresholds.add( next.getThresholds() );
            }

            unionFeatures.addAll( next.getFeatureTuples() );
            unionRegionNames.add( next.getPool().getRegionName() );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "While building the union metadata from {}, discovered these time windows in common: {}; and "
                          + "these features in common: {}; and these thresholds in common: {}; and these feature group"
                          + "names in common: {}. ",
                          unionWindows,
                          unionFeatures,
                          unionThresholds,
                          unionRegionNames );
        }

        TimeWindowOuter unionWindow = null;
        if ( !unionWindows.isEmpty() )
        {
            unionWindow = TimeWindowOuter.unionOf( unionWindows );
        }

        FeatureGroup featureGroup = null;

        if ( !unionFeatures.isEmpty() )
        {
            String regionName = "";
            if ( unionRegionNames.size() == 1 )
            {
                regionName = unionRegionNames.iterator().next();
            }

            featureGroup = FeatureGroup.of( regionName, unionFeatures );
        }

        OneOrTwoThresholds threshold = null;

        if ( unionThresholds.size() == 1 )
        {
            threshold = unionThresholds.iterator().next();
        }

        wres.statistics.generated.Pool unionPool = MessageFactory.parse( featureGroup,
                                                                         unionWindow,
                                                                         test.getTimeScale(),
                                                                         threshold,
                                                                         test.getPool().getIsBaselinePool() );

        return PoolMetadata.of( test.getEvaluation(), unionPool );
    }

    /**
     * Unpacks a pool of time-series into their raw event values, eliminating the time-series view.
     * 
     * @param <U> the type of time-series data
     * @param pool the pool
     * @return the unpacked pool
     * @throws NullPointerException if the input is null
     */

    private static <U> Pool<U> unpackInner( Pool<TimeSeries<U>> pool )
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
     * Transforms the input type to another type.
     * 
     * @param <S> the input type
     * @param <T> the output type
     * @param input the input
     * @param transformer the transformer
     * @return the transformed type
     */

    private static <S, T> Pool<T> transformInner( Pool<S> input, Function<S, T> transformer )
    {
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
     */

    private static <T> Pool<T> filterInner( Pool<T> input,
                                            Predicate<T> condition,
                                            DoublePredicate applyToClimatology )
    {
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
     * Do not construct.
     */
    private PoolSlicer()
    {
    }

}
