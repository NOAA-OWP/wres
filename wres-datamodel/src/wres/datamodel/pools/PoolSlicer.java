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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowSlicer;
import wres.datamodel.types.Climatology;
import wres.datamodel.Slicer;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.Pool.EnsembleAverageType;

/**
 * A utility class for slicing/dicing and transforming pool-shaped datasets
 * 
 * @author James Brown
 * @see Slicer
 * @see TimeSeriesSlicer
 */

public class PoolSlicer
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolSlicer.class );

    /** Failure to supply a non-null predicate. */
    private static final String NULL_PREDICATE_EXCEPTION = "Specify a non-null predicate.";

    /** Null mapper function error message. */
    private static final String NULL_MAPPER_EXCEPTION = "Specify a non-null function to map the input to an output.";

    /** Null input error message. */
    private static final String NULL_INPUT_EXCEPTION = "Specify a non-null input.";

    /** Null metadata mapper function. */
    private static final String NULL_META_MAPPER_EXCEPTION = "Specify a non-null function to transform the metadata.";

    /**
     * Transforms the input type to another type.
     * 
     * @see #transform(Pool, Function, UnaryOperator)
     * @param <S> the input type
     * @param <T> the output type
     * @param pool the input
     * @param transformer the transformer
     * @return the transformed type
     * @throws NullPointerException if either input is null
     */

    public static <S, T> Pool<T> transform( Pool<S> pool, Function<S, T> transformer )
    {
        return PoolSlicer.transform( pool, transformer, meta -> meta ); // No-op on metadata
    }

    /**
     * Transforms the input type to another type.
     * 
     * @see #transform(Pool, Function)
     * @param <S> the input type
     * @param <T> the output type
     * @param pool the input
     * @param transformer the transformer
     * @param metaTransformer the metadata transformer
     * @return the transformed type
     * @throws NullPointerException if any input is null
     */

    public static <S, T> Pool<T> transform( Pool<S> pool,
                                            Function<S, T> transformer,
                                            UnaryOperator<PoolMetadata> metaTransformer )
    {
        Objects.requireNonNull( pool, PoolSlicer.NULL_INPUT_EXCEPTION );
        Objects.requireNonNull( transformer, PoolSlicer.NULL_MAPPER_EXCEPTION );
        Objects.requireNonNull( metaTransformer, PoolSlicer.NULL_META_MAPPER_EXCEPTION );

        Pool.Builder<T> poolBuilder = new Pool.Builder<T>().setClimatology( pool.getClimatology() );

        // Preserve any small pools
        for ( Pool<S> next : pool.getMiniPools() )
        {
            Pool<T> transformed = PoolSlicer.transformInner( next, transformer, metaTransformer );
            poolBuilder.addPool( transformed );
        }

        // Set the overall metadata
        PoolMetadata unmapped = pool.getMetadata();
        PoolMetadata mapped = metaTransformer.apply( unmapped );
        poolBuilder.setMetadata( mapped );

        if ( pool.hasBaseline() )
        {
            PoolMetadata unmappedBaseline = pool.getBaselineData()
                                                .getMetadata();
            PoolMetadata mappedBaseline = metaTransformer.apply( unmappedBaseline );
            poolBuilder.setMetadataForBaseline( mappedBaseline );
        }

        return poolBuilder.build();
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs 
     * and, optionally, to the climatological data. Does not modify the metadata associated with the input.
     * 
     * @param <T> the type of data
     * @param pool the data to slice, not null
     * @param condition the condition on which to slice, not null
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if any required input is null
     */

    public static <T> Pool<T> filter( Pool<T> pool,
                                      Predicate<T> condition,
                                      DoublePredicate applyToClimatology )
    {
        return PoolSlicer.filter( pool, condition, applyToClimatology, meta -> meta );
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input. Additionally transforms the metadata to reflect the 
     * filtering of the pool.
     * 
     * @param <T> the type of data
     * @param pool the data to slice, not null
     * @param condition the condition on which to slice, not null
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @param metaTransformer the metadata transformer, not null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if any required input is null
     */

    public static <T> Pool<T> filter( Pool<T> pool,
                                      Predicate<T> condition,
                                      DoublePredicate applyToClimatology,
                                      UnaryOperator<PoolMetadata> metaTransformer )
    {
        Objects.requireNonNull( pool, PoolSlicer.NULL_INPUT_EXCEPTION );
        Objects.requireNonNull( condition, PoolSlicer.NULL_PREDICATE_EXCEPTION );
        Objects.requireNonNull( metaTransformer, PoolSlicer.NULL_META_MAPPER_EXCEPTION );

        Pool.Builder<T> poolBuilder = new Pool.Builder<>();

        // Filter climatology as required
        if ( pool.hasClimatology() )
        {
            Climatology climatology = pool.getClimatology();

            if ( Objects.nonNull( applyToClimatology ) )
            {
                climatology = Slicer.filter( pool.getClimatology(), applyToClimatology );
            }

            poolBuilder.setClimatology( climatology );
        }

        // Preserve any small pools
        for ( Pool<T> next : pool.getMiniPools() )
        {
            Pool<T> filtered = PoolSlicer.filterInner( next, condition, applyToClimatology, metaTransformer );
            poolBuilder.addPool( filtered );
        }

        // Set the overall metadata
        PoolMetadata unmapped = pool.getMetadata();
        PoolMetadata mapped = metaTransformer.apply( unmapped );
        poolBuilder.setMetadata( mapped );

        if ( pool.hasBaseline() )
        {
            PoolMetadata unmappedBaseline = pool.getBaselineData()
                                                .getMetadata();
            PoolMetadata mappedBaseline = metaTransformer.apply( unmappedBaseline );
            poolBuilder.setMetadataForBaseline( mappedBaseline );
        }

        return poolBuilder.build();
    }

    /**
     * Applies an attribute-specific filter to the corresponding attribute-specific subset of pooled data and returns 
     * the union of those filtered subsets. An example application may be to extract the pairs for a named feature 
     * within a pool of multiple features and create a pool for only that feature, applying a feature-specific filter 
     * to each feature-specific pool. Use the metadata transformer to generate metadata for each of the filtered pools.
     * 
     * @param <S> the pooled data type
     * @param <T> the metadata attribute
     * @param pools the pools to filter
     * @param filters the filters to use
     * @param composedMetadata the metadata for the composition
     * @param composedBaselineMetadata the metadata for the baseline composition
     * @param metaTransformer the metadata transformer, not null
     * @return the union of the subsets, each subset filtered by an attribute-specific predicate
     * @throws NullPointerException if any required input is null
     * @throws PoolException if the pool could not be filtered for any reason
     */

    public static <S, T extends Comparable<T>> Pool<S> filter( Map<T, Pool<S>> pools,
                                                               Map<T, Predicate<S>> filters,
                                                               PoolMetadata composedMetadata,
                                                               PoolMetadata composedBaselineMetadata,
                                                               UnaryOperator<PoolMetadata> metaTransformer )
    {
        Objects.requireNonNull( pools );
        Objects.requireNonNull( filters );
        Objects.requireNonNull( composedMetadata );
        Objects.requireNonNull( metaTransformer );

        // Return the empty pool
        if ( pools.isEmpty() )
        {
            PoolMetadata mapped = metaTransformer.apply( composedMetadata );
            Pool.Builder<S> poolBuilder = new Pool.Builder<S>().setMetadata( mapped );

            if ( Objects.nonNull( composedBaselineMetadata ) )
            {
                PoolMetadata mappedBaseline = metaTransformer.apply( composedBaselineMetadata );
                poolBuilder.setMetadataForBaseline( mappedBaseline );
            }

            return poolBuilder.build();
        }

        Pool.Builder<S> poolBuilder = new Pool.Builder<>();

        // Iterate the pools and apply the filters
        Set<T> keysWithoutFilter = new HashSet<>();

        for ( Map.Entry<T, Pool<S>> nextEntry : pools.entrySet() )
        {
            T nextKey = nextEntry.getKey();

            Pool<S> nextPool = nextEntry.getValue();
            Predicate<S> nextPredicate = filters.get( nextKey );

            if ( Objects.nonNull( nextPredicate ) )
            {
                Pool<S> filtered = PoolSlicer.filter( nextPool, nextPredicate, null, metaTransformer );
                poolBuilder.addPool( filtered );
            }
            else
            {
                keysWithoutFilter.add( nextKey );
            }
        }

        // Set the overall metadata
        PoolMetadata mapped = metaTransformer.apply( composedMetadata );
        poolBuilder.setMetadata( mapped );

        if ( Objects.nonNull( composedBaselineMetadata ) )
        {
            PoolMetadata mappedBaseline = metaTransformer.apply( composedBaselineMetadata );
            poolBuilder.setMetadataForBaseline( mappedBaseline );
        }

        // Handle cases with no data or some missing data
        if ( keysWithoutFilter.size() == pools.size() )
        {
            throw new PoolException( "Failed to apply attribute-specific filers to a pool. This probably occurred "
                                     + "because one of the smaller pools from which the pool was constructed had "
                                     + "incorrect metadata. Failed to identify a filter for any of these metadata "
                                     + "attribute instances: "
                                     + keysWithoutFilter
                                     + ". These filters were available: "
                                     + filters.keySet()
                                     + "." );
        }
        else if ( !keysWithoutFilter.isEmpty() && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "When filtering a pool into smaller pools by metadata attribute, failed to correlate some "
                          + "attributes with filters: {}. Consequently, no filtered pool was identified for any of "
                          + "these attribute instances and they will not be included in the evaluation.",
                          keysWithoutFilter );
        }

        return poolBuilder.build();
    }

    /**
     * Applies an attribute-specific transformer to the corresponding attribute-specific pools in the input 
     * and returns the union of those transformed subsets. Also transforms the metadata to reflect the transformation
     * of the pool.
     * 
     * @param <S> the pooled data type
     * @param <T> the metadata attribute
     * @param <U> the transformed pool data type
     * @param pools the pools to transform
     * @param transformers the transformers to use
     * @param composedMetadata the metadata for the composition
     * @param composedBaselineMetadata the metadata for the baseline composition
     * @param metaTransformer the function that transforms the pool metadata to reflect the transformation of the pool
     * @return the union of the subsets, each subset filtered by an attribute-specific predicate
     * @throws NullPointerException if any required input is null
     * @throws PoolException if the pool could not be filtered for any reason
     */

    public static <S, T extends Comparable<T>, U> Pool<U> transform( Map<T, Pool<S>> pools,
                                                                     Map<T, Function<S, U>> transformers,
                                                                     PoolMetadata composedMetadata,
                                                                     PoolMetadata composedBaselineMetadata,
                                                                     UnaryOperator<PoolMetadata> metaTransformer )
    {
        Objects.requireNonNull( pools );
        Objects.requireNonNull( transformers );
        Objects.requireNonNull( composedMetadata );
        Objects.requireNonNull( metaTransformer );

        // Return the empty pool
        if ( pools.isEmpty() )
        {
            PoolMetadata mapped = metaTransformer.apply( composedMetadata );
            Pool.Builder<U> poolBuilder = new Pool.Builder<U>().setMetadata( mapped );

            if ( Objects.nonNull( composedBaselineMetadata ) )
            {
                PoolMetadata mappedBaseline = metaTransformer.apply( composedBaselineMetadata );
                poolBuilder.setMetadataForBaseline( mappedBaseline );
            }

            return poolBuilder.build();
        }

        // Iterate the pools and apply the transformers
        Set<T> keysWithoutTransformer = new HashSet<>();

        Pool.Builder<U> poolBuilder = new Pool.Builder<>();

        for ( Map.Entry<T, Pool<S>> nextEntry : pools.entrySet() )
        {
            T nextKey = nextEntry.getKey();

            Pool<S> nextPool = nextEntry.getValue();
            Function<S, U> nextTransformer = transformers.get( nextKey );

            if ( Objects.nonNull( nextTransformer ) )
            {
                Pool<U> transformed = PoolSlicer.transform( nextPool, nextTransformer, metaTransformer );
                poolBuilder.addPool( transformed );
            }
            else
            {
                keysWithoutTransformer.add( nextKey );
            }
        }

        // Set the overall metadata
        PoolMetadata mapped = metaTransformer.apply( composedMetadata );
        poolBuilder.setMetadata( mapped );

        if ( Objects.nonNull( composedBaselineMetadata ) )
        {
            PoolMetadata mappedBaseline = metaTransformer.apply( composedBaselineMetadata );
            poolBuilder.setMetadataForBaseline( mappedBaseline );
        }

        // Handle cases with no data or some missing data
        if ( keysWithoutTransformer.size() == pools.size() )
        {
            throw new PoolException( "Failed to apply attribute-specific transformers to a pool. This probably "
                                     + "occurred because one of the smaller pools from which the pool was constructed "
                                     + "had incorrect metadata. Failed to identify a transformer for any of these "
                                     + "metadata attribute instances: "
                                     + keysWithoutTransformer
                                     + ". These transformers were available: "
                                     + transformers
                                     + "." );
        }
        else if ( !keysWithoutTransformer.isEmpty() && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "When transforming a pool into smaller pools by metadata attribute, failed to correlate some "
                          + "attributes with transformers: {}. Consequently, no transformed pool was identified for "
                          + "any of these attribute instances and they will not be included in the evaluation.",
                          keysWithoutTransformer );
        }

        return poolBuilder.build();
    }

    /**
     * Counts the number of time-series events in the main pool.
     * 
     * @param <U> the type of time-series data
     * @param pool the pool
     * @return the number of events
     * @throws NullPointerException if the input is null
     */

    public static <U> int getEventCount( Pool<TimeSeries<U>> pool )
    {
        Objects.requireNonNull( pool );

        return pool.get()
                   .stream()
                   .mapToInt( next -> next.getEvents()
                                          .size() )
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

        Pool.Builder<U> poolBuilder = new Pool.Builder<>();

        // Preserve any small pools
        for ( Pool<TimeSeries<U>> nextMiniPool : pool.getMiniPools() )
        {
            Pool<U> nextUnpacked = PoolSlicer.unpackInner( nextMiniPool );
            poolBuilder.addPool( nextUnpacked );
        }

        // Set the overall metadata
        poolBuilder.setMetadata( pool.getMetadata() )
                   .setClimatology( pool.getClimatology() );

        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
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
     * @param pool the pool
     * @param metaMapper the metadata mapper
     * @return a decomposed list of mini-pools based on their metadata
     * @throws IllegalArgumentException if multiple pools map to the same key
     */

    public static <S extends Comparable<S>, T> Map<S, Pool<T>> decompose( Pool<T> pool,
                                                                          Function<PoolMetadata, S> metaMapper )
    {
        Objects.requireNonNull( metaMapper );
        Objects.requireNonNull( pool );

        // Use a sorted map implementation to preserve order
        Map<S, Pool<T>> returnMe = new TreeMap<>();

        LOGGER.debug( "Encountered a pool with {} mini-pools.",
                      pool.getMiniPools()
                          .size() );

        for ( Pool<T> nextPool : pool.getMiniPools() )
        {
            S key;

            try
            {
                key = metaMapper.apply( nextPool.getMetadata() );
            }
            catch ( PoolException e )
            {
                throw new PoolException( "Encountered an error while attempting to extract the metadata for a "
                                         + "mini pool. This is one of "
                                         + pool.getMiniPools().size()
                                         + " mini pools that compose the overall pool. The metadata for the mini pool "
                                         + "is: "
                                         + nextPool.getMetadata()
                                         + ". The overall pool metadata is: "
                                         + pool.getMetadata()
                                         + ".",
                                         e );
            }

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
                throw new PoolException( "Could not obtain the expected feature tuple from the pool metadata because "
                                         + "the pool composes more than one feature ("
                                         + features.size()
                                         + ") where a single feature was expected. This may occur when a pool contains "
                                         + "several features but the pool was not built to preserve the "
                                         + "feature-specific 'mini-pools' using the appropriate builder option for the "
                                         + "outer pool or some other transformation of the outer pool resulted in a "
                                         + "loss of the 'mini-pool' context. The features encountered are: "
                                         + features
                                         + ". The pool metadata is: "
                                         + metadata
                                         + "." );
            }

            return features.iterator()
                           .next();
        };
    }

    /**
     * Returns the union of the supplied metadata. All components of the input must be equal in terms of 
     * {@link #equalsWithoutTimeWindowOrThresholdsOrFeaturesOrPoolIdOrEnsembleAverageType(PoolMetadata, PoolMetadata)}. 
     * Furthermore, there cannot be more than one {@link EnsembleAverageType} after ignoring 
     * {@link EnsembleAverageType#NONE}.
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

        // Preserve order
        Set<TimeWindowOuter> unionWindows = new TreeSet<>();
        Set<FeatureTuple> unionFeatures = new TreeSet<>();
        Set<OneOrTwoThresholds> unionThresholds = new TreeSet<>();
        Set<String> unionRegionNames = new TreeSet<>();
        Set<EnsembleAverageType> ensembleAverageType = new TreeSet<>();

        // Test entry
        PoolMetadata test = input.get( 0 );
        TimeScaleOuter timeScale = null;

        // Validate for equivalence with the first entry and add window to list
        for ( PoolMetadata next : input )
        {
            Objects.requireNonNull( next, nullString );

            // Check that the union can be formed in principle. Validation of the ensemble average type is deferred.
            if ( !PoolSlicer.equalsWithoutTimeWindowOrThresholdsOrFeaturesOrPoolIdOrEnsembleAverageType( next, test ) )
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

            if ( Objects.isNull( timeScale ) )
            {
                timeScale = next.getTimeScale();
            }

            unionFeatures.addAll( next.getFeatureTuples() );

            PoolSlicer.addRegionName( unionRegionNames, next );

            wres.statistics.generated.Pool pool = next.getPoolDescription();
            ensembleAverageType.add( pool.getEnsembleAverageType() );
        }

        LOGGER.debug( "While building the union metadata from {}, discovered these time windows in common: {}; and "
                      + "these features in common: {}; and these thresholds in common: {}; and these feature group"
                      + "names in common: {}. ",
                      input,
                      unionWindows,
                      unionFeatures,
                      unionThresholds,
                      unionRegionNames );

        // Copy to builder and clean attributes whose union has been formed
        wres.statistics.generated.Pool.Builder builder = test.getPoolDescription()
                                                             .toBuilder()
                                                             .clearTimeWindow()
                                                             .clearEventThreshold()
                                                             .clearDecisionThreshold()
                                                             .clearGeometryTuples()
                                                             .clearRegionName()
                                                             .clearGeometryGroup();

        if ( !unionWindows.isEmpty() )
        {
            TimeWindowOuter unionWindow = TimeWindowSlicer.union( unionWindows );
            builder.setTimeWindow( unionWindow.getTimeWindow() );
        }

        if ( !unionFeatures.isEmpty() )
        {
            String regionName = null;
            if ( unionRegionNames.size() == 1 )
            {
                regionName = unionRegionNames.iterator().next();
                builder.setRegionName( regionName );
            }

            GeometryGroup geoGroup = MessageFactory.getGeometryGroup( regionName, unionFeatures );
            builder.setGeometryGroup( geoGroup );
            builder.addAllGeometryTuples( geoGroup.getGeometryTuplesList() );
        }

        if ( unionThresholds.size() == 1 )
        {
            PoolSlicer.addThreshold( builder, unionThresholds.iterator().next() );
        }

        // Set the ensemble average type, which may throw an exception, since the check was deferred until now.
        PoolSlicer.setEnsembleAverageType( builder, ensembleAverageType );

        return PoolMetadata.of( test.getEvaluation(), builder.build() );
    }

    /**
     * Adds a region name to the set of region names.
     * @param regionNames the region names
     * @param pool the pool metadata
     */

    private static void addRegionName( Set<String> regionNames, PoolMetadata pool )
    {
        if ( !pool.getPoolDescription()
                  .getGeometryGroup()
                  .getRegionName()
                  .isBlank() )
        {
            regionNames.add( pool.getPoolDescription()
                                 .getGeometryGroup()
                                 .getRegionName() );
        }
    }

    /**
     * Adds a threshold to the builder.
     * @param builder the builder
     * @param threshold the threshold
     */
    private static void addThreshold( wres.statistics.generated.Pool.Builder builder, OneOrTwoThresholds threshold )
    {
        builder.setEventThreshold( threshold.first().getThreshold() );
        if ( threshold.hasTwo() )
        {
            builder.setDecisionThreshold( threshold.second().getThreshold() );
        }
    }

    /**
     * Sets the ensemble average type in the builder from the set of options. If there is more than one option after
     * filtering {@link EnsembleAverageType#NONE}, an exception is thrown because there is no union of inconsistent 
     * metadatas.
     * 
     * @param builder the builder
     * @param ensembleAverageTypes the ensemble average types
     */

    private static void setEnsembleAverageType( wres.statistics.generated.Pool.Builder builder,
                                                Set<EnsembleAverageType> ensembleAverageTypes )
    {
        Set<EnsembleAverageType> filtered = new TreeSet<>( ensembleAverageTypes );
        filtered.remove( EnsembleAverageType.NONE );

        if ( filtered.size() > 1 )
        {
            throw new PoolMetadataException( "Cannot find the union of metadatas with more than one ensemble average "
                                             + "type. Found: "
                                             + ensembleAverageTypes
                                             + "." );
        }

        if ( !filtered.isEmpty() )
        {
            EnsembleAverageType type = filtered.iterator()
                                               .next();
            builder.setEnsembleAverageType( type );
            LOGGER.debug( "While finding the union of pool metadatas, set the ensemble average type to {}.",
                          type );
        }
    }

    /**
     * Returns <code>true</code> if the two metadatas are equal after ignoring the time windows, thresholds,  
     * features and pool identifiers. In addition, the time scale will be ignored (lenient) if one of the time scales is 
     * null/unknown.
     * 
     * @param first the first metadata to test for conditional equality with the second
     * @param second the second metadata to test for conditional equality with the first
     * @return true if the metadatas are conditionally equal
     */

    private static boolean
            equalsWithoutTimeWindowOrThresholdsOrFeaturesOrPoolIdOrEnsembleAverageType( PoolMetadata first,
                                                                                        PoolMetadata second )
    {
        if ( Objects.isNull( first ) != Objects.isNull( second ) )
        {
            return false;
        }

        if ( Objects.isNull( first ) )
        {
            return true;
        }

        // Lenient about the time scale when it is missing from one
        boolean ignoreTimeScale = !first.hasTimeScale() || !second.hasTimeScale();

        // Adjust the pools to remove the information to be ignored
        wres.statistics.generated.Pool.Builder adjustedPoolFirst = first.getPoolDescription()
                                                                        .toBuilder()
                                                                        .clearPoolId()
                                                                        .clearTimeWindow()
                                                                        .clearEventThreshold()
                                                                        .clearDecisionThreshold()
                                                                        .clearGeometryGroup()
                                                                        .clearEnsembleAverageType()
                                                                        .clearGeometryTuples()
                                                                        .clearRegionName();

        wres.statistics.generated.Pool.Builder adjustedPoolSecond = second.getPoolDescription()
                                                                          .toBuilder()
                                                                          .clearPoolId()
                                                                          .clearTimeWindow()
                                                                          .clearEventThreshold()
                                                                          .clearDecisionThreshold()
                                                                          .clearGeometryGroup()
                                                                          .clearEnsembleAverageType()
                                                                          .clearGeometryTuples()
                                                                          .clearRegionName();

        if ( ignoreTimeScale )
        {
            adjustedPoolFirst.clearTimeScale();
            adjustedPoolSecond.clearTimeScale();
        }

        return first.getEvaluation().equals( second.getEvaluation() )
               && adjustedPoolFirst.build().equals( adjustedPoolSecond.build() );
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
                                 .toList();

        List<U> baselineSampleData = null;
        PoolMetadata baselineMetadata = null;

        if ( pool.hasBaseline() )
        {
            baselineSampleData = pool.getBaselineData()
                                     .get()
                                     .stream()
                                     .flatMap( next -> next.getEvents().stream() )
                                     .map( Event::getValue )
                                     .toList();

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
     * @param pool the input
     * @param transformer the transformer
     * @param metaTransformer the metadata transformer
     * @return the transformed type
     */

    private static <S, T> Pool<T> transformInner( Pool<S> pool,
                                                  Function<S, T> transformer,
                                                  UnaryOperator<PoolMetadata> metaTransformer )
    {
        PoolMetadata unmapped = pool.getMetadata();
        PoolMetadata mapped = metaTransformer.apply( unmapped );
        Pool.Builder<T> builder = new Pool.Builder<T>().setMetadata( mapped )
                                                       .setClimatology( pool.getClimatology() );

        // Add the main series
        for ( S next : pool.get() )
        {
            T transformed = transformer.apply( next );
            if ( Objects.nonNull( transformed ) )
            {
                builder.addData( transformed );
            }
        }

        // Add the baseline series if available
        if ( pool.hasBaseline() )
        {
            Pool<S> baseline = pool.getBaselineData();

            for ( S next : baseline.get() )
            {
                T transformed = transformer.apply( next );
                if ( Objects.nonNull( transformed ) )
                {
                    builder.addDataForBaseline( transformed );
                }
            }

            PoolMetadata unmappedBaseline = baseline.getMetadata();
            PoolMetadata mappedBaseline = metaTransformer.apply( unmappedBaseline );
            builder.setMetadataForBaseline( mappedBaseline );
        }

        return builder.build();
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input.
     * 
     * @param <T> the type of data
     * @param pool the data to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @param metaTransformer the metadata transformer
     * @return the subset of pairs that meet the condition
     */

    private static <T> Pool<T> filterInner( Pool<T> pool,
                                            Predicate<T> condition,
                                            DoublePredicate applyToClimatology,
                                            UnaryOperator<PoolMetadata> metaTransformer )
    {
        Pool.Builder<T> builder = new Pool.Builder<>();

        List<T> mainPairs = pool.get();
        List<T> mainPairsSubset =
                mainPairs.stream()
                         .filter( condition )
                         .toList();

        PoolMetadata unmapped = pool.getMetadata();
        PoolMetadata mapped = metaTransformer.apply( unmapped );
        builder.addData( mainPairsSubset )
               .setMetadata( mapped );

        // Filter climatology as required
        if ( pool.hasClimatology() )
        {
            Climatology climatology = pool.getClimatology();

            if ( Objects.nonNull( applyToClimatology ) )
            {
                climatology = Slicer.filter( pool.getClimatology(), applyToClimatology );
            }

            builder.setClimatology( climatology );
        }

        //Filter baseline as required
        if ( pool.hasBaseline() )
        {
            Pool<T> baseline = pool.getBaselineData();
            List<T> basePairs = baseline.get();
            List<T> basePairsSubset =
                    basePairs.stream()
                             .filter( condition )
                             .toList();

            PoolMetadata unmappedBaseline = baseline.getMetadata();
            PoolMetadata mappedBaseline = metaTransformer.apply( unmappedBaseline );
            builder.addDataForBaseline( basePairsSubset )
                   .setMetadataForBaseline( mappedBaseline );
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
