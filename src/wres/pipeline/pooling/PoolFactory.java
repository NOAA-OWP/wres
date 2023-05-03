package wres.pipeline.pooling;

import java.time.Duration;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.measure.Unit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.config.yaml.components.CrossPair;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Values;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.MissingValues;
import wres.datamodel.units.UnitMapper;
import wres.datamodel.units.Units;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesOfEnsembleUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairer.TimePairingType;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.generators.PersistenceGenerator;
import wres.io.project.Project;
import wres.io.retrieving.CachingSupplier;
import wres.io.retrieving.RetrieverFactory;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryGroup;

/**
 * A factory class for generating the pools of pairs associated with an evaluation.
 *
 * @author James Brown
 */

public class PoolFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolFactory.class );

    /** Part of a message string about data declaration that is re-used. */
    private static final String DATA_IS_DECLARED_AS = "data is declared as '";

    /** Part of a message string about disallowed declaration that is re-used. */
    private static final String WHICH_IS_NOT_ALLOWED = "', which is not allowed.";

    private static final String CANNOT_CREATE_POOLS_WITHOUT_A_RETRIEVER_FACTORY =
            "Cannot create pools without a retriever factory.";

    private static final String CANNOT_CREATE_POOLS_WITHOUT_LIST_OF_POOL_REQUESTS =
            "Cannot create pools without list of pool requests.";

    private static final String CANNOT_CREATE_POOLS_FROM_A_NULL_PROJECT = "Cannot create pools from a null project.";

    private static final String CANNOT_CREATE_POOLS_WITHOUT_POOL_PARAMETERS = "Cannot create pools without pool "
                                                                              + "parameters.";

    /** The project. */
    private final Project project;

    /** A unit mapper. **/
    private final UnitMapper unitMapper;

    /** Used to create a pool identifier. */
    private final AtomicLong poolId = new AtomicLong( 0 );

    /** Cache of unit mappers for re-use. **/
    private final Cache<String, DoubleUnaryOperator> converterCache =
            Caffeine.newBuilder()
                    .maximumSize( 10 )
                    .build();

    /**
     * Creates an instance from a {@link Project}.
     *
     * @param project the project
     * @return an instance
     * @throws NullPointerException if the project is null
     */

    public static PoolFactory of( Project project )
    {
        return new PoolFactory( project );
    }

    /**
     * <p>Create pools for single-valued data. This method will attempt to retrieve and re-use data that is common to
     * multiple pools. In particular, it will cache the climatological data for all pool requests that belong to a 
     * single feature group because the climatological data is shared across feature groups. The order returned is the
     * intended execution order for optimal heap usage because pools with data affinities are grouped together. The 
     * order returned may differ from the order of pool requests received.
     *
     * <p>TODO: further optimize to re-use datasets that are shared between multiple feature groups, other than
     * climatology, such as one feature that is part of several feature groups.
     *
     * @param poolRequests the pool requests, not null
     * @param retrieverFactory the retriever factory, not null
     * @param poolParameters the pool parameters
     * @return one pool supplier for each pool request, ordered in intended execution order (for optimal performance)
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>>>
    getSingleValuedPools( List<PoolRequest> poolRequests,
                          RetrieverFactory<Double, Double> retrieverFactory,
                          PoolParameters poolParameters )
    {
        Objects.requireNonNull( poolRequests, CANNOT_CREATE_POOLS_WITHOUT_LIST_OF_POOL_REQUESTS );
        Objects.requireNonNull( retrieverFactory, CANNOT_CREATE_POOLS_WITHOUT_A_RETRIEVER_FACTORY );
        Objects.requireNonNull( poolParameters, CANNOT_CREATE_POOLS_WITHOUT_POOL_PARAMETERS );

        // Optimize, if possible, by conducting feature-batched retrieval
        Map<FeatureGroup, OptimizedPoolRequests> optimizedGroups =
                this.getFeatureBatchedSingletons( poolRequests, poolParameters );

        List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Double>>>>> suppliers = new ArrayList<>();

        Project innerProject = this.getProject();

        boolean hasEqualBaselineAndClimatology = this.hasEqualBaselineAndClimatology();

        for ( Map.Entry<FeatureGroup, OptimizedPoolRequests> nextEntry : optimizedGroups.entrySet() )
        {
            FeatureGroup featureGroup = nextEntry.getKey();
            OptimizedPoolRequests optimized = nextEntry.getValue();
            List<PoolRequest> nextPoolRequests = optimized.optimizedRequests();

            LOGGER.debug( "Building pool suppliers for feature group {}, which contains {} pool requests.",
                          featureGroup,
                          nextPoolRequests.size() );

            // Create a retriever factory that caches the climatological and generated baseline data for all pool 
            // requests associated with the feature group (as required)
            RetrieverFactory<Double, Double> cachingFactory = retrieverFactory;
            if ( innerProject.hasProbabilityThresholds() || innerProject.hasGeneratedBaseline() )
            {
                LOGGER.debug( "Building a caching retriever factory to cache the retrieval of the climatological and "
                              + "generated baseline data (where applicable) across all pools within feature group {}.",
                              featureGroup );

                cachingFactory = new CachingRetrieverFactory<>( retrieverFactory,
                                                                innerProject.hasGeneratedBaseline(),
                                                                hasEqualBaselineAndClimatology,
                                                                Function.identity() );
            }

            List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Double>>>>> nextSuppliers =
                    this.getSingleValuedPoolsInner( innerProject, nextPoolRequests, cachingFactory );

            // Optimized? In that case, decompose the feature-batched pool suppliers into feature-specific pool 
            // suppliers
            if ( optimized.isOptimized() )
            {
                List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Double>>>>> decomposed =
                        this.decompose( nextSuppliers );
                suppliers.addAll( decomposed );
            }
            else
            {
                suppliers.addAll( nextSuppliers );
            }
        }

        return this.unpack( optimizedGroups, suppliers );
    }

    /**
     * <p>Create pools for ensemble data. This method will attempt to retrieve and re-use data that is common to multiple
     * pools. In particular, it will cache the climatological data for all pool requests that belong to a single 
     * feature group because the climatological data is shared across feature groups. The order returned is the intended 
     * execution order for optimal heap usage because pools with data affinities are grouped together. The order 
     * returned may differ from the order of pool requests received.
     *
     * <p>TODO: further optimize to re-use datasets that are shared between multiple feature groups, other than
     * climatology, such as one feature that is part of several feature groups.
     *
     * @param poolRequests the pool requests, not null
     * @param retrieverFactory the retriever factory, not null
     * @param poolParameters the pool parameters
     * @return one pool supplier for each pool request, ordered in intended execution order (for optimal performance)
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for ensemble data
     */

    public List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>>>
    getEnsemblePools( List<PoolRequest> poolRequests,
                      RetrieverFactory<Double, Ensemble> retrieverFactory,
                      PoolParameters poolParameters )
    {
        Objects.requireNonNull( poolRequests, CANNOT_CREATE_POOLS_WITHOUT_LIST_OF_POOL_REQUESTS );
        Objects.requireNonNull( retrieverFactory, CANNOT_CREATE_POOLS_WITHOUT_A_RETRIEVER_FACTORY );
        Objects.requireNonNull( poolParameters, CANNOT_CREATE_POOLS_WITHOUT_POOL_PARAMETERS );

        // Optimize, if possible, by conducting feature-batched retrieval. Each list of pool requests is associated
        // with the same feature group, which may be a composition of features to decompose
        Map<FeatureGroup, OptimizedPoolRequests> optimizedGroups =
                this.getFeatureBatchedSingletons( poolRequests, poolParameters );

        List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Ensemble>>>>> suppliers = new ArrayList<>();

        Project innerProject = this.getProject();

        for ( Map.Entry<FeatureGroup, OptimizedPoolRequests> nextEntry : optimizedGroups.entrySet() )
        {
            FeatureGroup featureGroup = nextEntry.getKey();
            OptimizedPoolRequests optimized = nextEntry.getValue();
            List<PoolRequest> nextPoolRequests = optimized.optimizedRequests();

            LOGGER.debug( "Building pool suppliers for feature group {}, which contains {} pool requests.",
                          featureGroup,
                          nextPoolRequests.size() );

            // Create a retriever factory that caches the climatological and generated baseline data for all pool 
            // requests associated with the feature group (as required)
            RetrieverFactory<Double, Ensemble> cachingFactory = retrieverFactory;
            if ( innerProject.hasProbabilityThresholds() || innerProject.hasGeneratedBaseline() )
            {
                LOGGER.debug( "Building a caching retriever factory to cache the retrieval of the climatological and "
                              + "generated baseline data (where applicable) across all pools within feature group {}.",
                              featureGroup );

                cachingFactory = new CachingRetrieverFactory<>( retrieverFactory,
                                                                innerProject.hasGeneratedBaseline(),
                                                                false,
                                                                null );
            }

            List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Ensemble>>>>> nextSuppliers =
                    this.getEnsemblePoolsInner( innerProject, nextPoolRequests, cachingFactory );

            // Optimized? In that case, decompose the feature-batched pools into feature-specific pools
            if ( optimized.isOptimized() )
            {
                List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Ensemble>>>>> decomposed =
                        this.decompose( nextSuppliers );
                suppliers.addAll( decomposed );
            }
            else
            {
                suppliers.addAll( nextSuppliers );
            }
        }

        return this.unpack( optimizedGroups, suppliers );
    }

    /**
     * Generates the {@link PoolRequest} in order to drive pool creation.
     *
     * @param evaluation the evaluation description
     * @return the pool requests
     * @throws NullPointerException if any input is null
     * @throws PoolCreationException if the pool could not be created for any other reason
     */

    public List<PoolRequest> getPoolRequests( Evaluation evaluation )
    {
        Objects.requireNonNull( evaluation );

        Project innerProject = this.getProject();
        EvaluationDeclaration declaration = innerProject.getDeclaration();

        Set<FeatureGroup> featureGroups = innerProject.getFeatureGroups();

        // Get the desired timescale
        TimeScaleOuter desiredTimeScale = innerProject.getDesiredTimeScale();

        // Get the time windows and sort them
        Set<TimeWindowOuter> timeWindows = DeclarationUtilities.getTimeWindows( declaration )
                                                               .stream()
                                                               .map( TimeWindowOuter::of )
                                                               .collect( Collectors.toCollection( TreeSet::new ) );

        return featureGroups.stream()
                            .flatMap( nextGroup -> this.getPoolRequests( evaluation,
                                                                         declaration,
                                                                         nextGroup,
                                                                         desiredTimeScale,
                                                                         timeWindows )
                                                       .stream() )
                            .toList();
    }

    /**
     * <p>Create pools for single-valued data. This method will attempt to retrieve and re-use data that is common to
     * multiple pools. Thus, it is generally better to provide a list of pool requests that represent connected pools, 
     * such as pools that all belong to the same feature group, rather than supplying a single pool or a long list of 
     * unconnected pools, such as pools that belong to many feature groups.
     *
     * <p>TODO: analyze the pool requests and find groups with shared data, i.e., automatically analyze/optimize. In
     * that case, there should be one call to this method per evaluation, which should then forward each group of
     * requests for batched creation of suppliers, as this method currently assumes the caller will do.
     *
     * @param project the project for which pools are required, not null
     * @param poolRequests the pool requests, not null
     * @param retrieverFactory the retriever factory, not null
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    private List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Double>>>>>
    getSingleValuedPoolsInner( Project project,
                               List<PoolRequest> poolRequests,
                               RetrieverFactory<Double, Double> retrieverFactory )
    {
        EvaluationDeclaration declaration = project.getDeclaration();

        // Check that the project declaration is consistent with a request for single-valued pools
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        this.validateRequestedPoolsAgainstDeclaration( project, false );

        long projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}'.",
                      projectId );

        // Create a default pairer for finite left and right values
        TimePairingType timePairingType = this.getTimePairingTypeFromDeclaration( declaration );

        LOGGER.debug( "Using a time-based pairing strategy of {} for the declaration {}.",
                      timePairingType,
                      declaration );

        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of( Double::isFinite,
                                                                                  Double::isFinite,
                                                                                  timePairingType );

        // Create a cross pairer, in case this is required by the declaration
        TimeSeriesCrossPairer<Double, Double> crossPairer = this.getCrossPairerOrNull( declaration );

        // Lenient upscaling?
        boolean leftLenient = project.isUpscalingLenient( DatasetOrientation.LEFT );
        boolean rightLenient = project.isUpscalingLenient( DatasetOrientation.RIGHT );
        boolean baselineLenient = project.isUpscalingLenient( DatasetOrientation.BASELINE );

        // Create a default upscaler for each side
        TimeSeriesUpscaler<Double> leftUpscaler = TimeSeriesOfDoubleUpscaler.of( leftLenient,
                                                                                 this.getUnitMapper()
                                                                                     .getUnitAliases() );

        TimeSeriesUpscaler<Double> rightUpscaler = TimeSeriesOfDoubleUpscaler.of( rightLenient,
                                                                                  this.getUnitMapper()
                                                                                      .getUnitAliases() );

        TimeSeriesUpscaler<Double> baselineUpscaler = TimeSeriesOfDoubleUpscaler.of( baselineLenient,
                                                                                     this.getUnitMapper()
                                                                                         .getUnitAliases() );

        // Create a feature-specific baseline generator function (e.g., persistence), if required
        Function<Set<Feature>, UnaryOperator<TimeSeries<Double>>> baselineGenerator = null;
        // Generated baseline declared?
        if ( project.hasGeneratedBaseline() )
        {
            LOGGER.debug( "While creating pools for project '{}', discovered a baseline "
                          + "to generate from a data source.",
                          projectId );

            baselineGenerator = this.getGeneratedBaseline( declaration.baseline(),
                                                           retrieverFactory,
                                                           rightUpscaler,
                                                           Double::isFinite );
        }

        // Create any required transformers for value constraints and units
        DoubleUnaryOperator valueTransformer = this.getSingleValuedTransformer( declaration.values() );
        UnaryOperator<TimeSeries<Double>> valueAndUnitTransformer =
                this.getSingleValuedTransformer( valueTransformer );

        // Currently only a seasonal filter, which applies equally to all sides
        Predicate<TimeSeries<Double>> filter = this.getSeasonalFilter( declaration.season() );

        // Get the time shifts
        Dataset left = project.getDeclaredDataset( DatasetOrientation.LEFT );
        Duration leftTimeShift = this.getTimeShift( left );
        Dataset right = project.getDeclaredDataset( DatasetOrientation.RIGHT );
        Duration rightTimeShift = this.getTimeShift( right );
        Dataset baseline = project.getDeclaredDataset( DatasetOrientation.BASELINE );
        Duration baselineTimeShift = this.getTimeShift( baseline );

        Duration pairFrequency = this.getPairFrequency( declaration );

        // Build and return the pool suppliers
        List<Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> rawSuppliers =
                new PoolsGenerator.Builder<Double, Double>().setProject( project )
                                                            .setRetrieverFactory( retrieverFactory )
                                                            .setPoolRequests( poolRequests )
                                                            .setBaselineGenerator( baselineGenerator )
                                                            .setLeftTransformer( valueAndUnitTransformer )
                                                            .setRightTransformer( valueAndUnitTransformer )
                                                            .setBaselineTransformer( valueAndUnitTransformer )
                                                            .setLeftFilter( filter )
                                                            .setRightFilter( filter )
                                                            .setBaselineFilter( filter )
                                                            .setLeftUpscaler( leftUpscaler )
                                                            .setRightUpscaler( rightUpscaler )
                                                            .setBaselineUpscaler( baselineUpscaler )
                                                            .setLeftTimeShift( leftTimeShift )
                                                            .setRightTimeShift( rightTimeShift )
                                                            .setBaselineTimeShift( baselineTimeShift )
                                                            .setPairer( pairer )
                                                            .setPairFrequency( pairFrequency )
                                                            .setCrossPairer( crossPairer )
                                                            .setClimateMapper( Double::doubleValue )
                                                            .setClimateAdmissibleValue( Double::isFinite )
                                                            .build()
                                                            .get();

        return this.getComposedSuppliers( poolRequests, rawSuppliers );
    }

    /**
     * Create pools for ensemble data. This method will attempt to retrieve and re-use data that is common to multiple 
     * pools. Thus, it is generally better to provide a list of pool requests that represent connected pools, such as 
     * pools that all belong to the same feature group, rather than supplying a single pool or a long list of 
     * unconnected pools, such as pools that belong to many feature groups. Specifically, it will cache the 
     * climatological data for all pool requests if climatological data is needed.
     *
     * @param project the project for which pools are required, not null
     * @param poolRequests the pool requests, not null
     * @param retrieverFactory the retriever factory, not null
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for ensemble data
     */

    private List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<Double, Ensemble>>>>>
    getEnsemblePoolsInner( Project project,
                           List<PoolRequest> poolRequests,
                           RetrieverFactory<Double, Ensemble> retrieverFactory )
    {
        EvaluationDeclaration declaration = project.getDeclaration();

        // Check that the project declaration is consistent with a request for ensemble pools
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        this.validateRequestedPoolsAgainstDeclaration( project, true );

        long projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}'.",
                      projectId );

        // Create a default pairer for finite left values and one or more finite right values
        TimePairingType timePairingType = this.getTimePairingTypeFromDeclaration( declaration );

        LOGGER.debug( "Using a time-based pairing strategy of {} for the input declaration {}.",
                      timePairingType,
                      declaration );

        TimeSeriesPairer<Double, Ensemble> pairer =
                TimeSeriesPairerByExactTime.of( Double::isFinite,
                                                en -> Arrays.stream( en.getMembers() )
                                                            .anyMatch( Double::isFinite ),
                                                timePairingType );

        // Create a cross pairer, in case this is required by the declaration
        TimeSeriesCrossPairer<Double, Ensemble> crossPairer = this.getCrossPairerOrNull( declaration );

        // Lenient upscaling?
        boolean leftLenient = project.isUpscalingLenient( DatasetOrientation.LEFT );
        boolean rightLenient = project.isUpscalingLenient( DatasetOrientation.RIGHT );
        boolean baselineLenient = project.isUpscalingLenient( DatasetOrientation.BASELINE );

        // Create a default upscaler for each side
        TimeSeriesUpscaler<Double> leftUpscaler = TimeSeriesOfDoubleUpscaler.of( leftLenient,
                                                                                 this.getUnitMapper()
                                                                                     .getUnitAliases() );

        TimeSeriesUpscaler<Ensemble> rightUpscaler = TimeSeriesOfEnsembleUpscaler.of( rightLenient,
                                                                                      this.getUnitMapper()
                                                                                          .getUnitAliases() );

        TimeSeriesUpscaler<Ensemble> baselineUpscaler = TimeSeriesOfEnsembleUpscaler.of( baselineLenient,
                                                                                         this.getUnitMapper()
                                                                                             .getUnitAliases() );
        // Left transformer
        DoubleUnaryOperator leftValueTransformer = this.getSingleValuedTransformer( declaration.values() );
        UnaryOperator<TimeSeries<Double>> leftValueAndUnitTransformer =
                this.getSingleValuedTransformer( leftValueTransformer );

        // Right transformer
        Dataset right = project.getDeclaredDataset( DatasetOrientation.RIGHT );
        UnaryOperator<Event<Ensemble>> rightValueTransformer = this.getEnsembleTransformer( leftValueTransformer,
                                                                                            right );
        UnaryOperator<TimeSeries<Ensemble>> rightValueAndUnitTransformer =
                this.getEnsembleTransformer( rightValueTransformer );

        // Baseline transformer
        Dataset baseline = project.getDeclaredDataset( DatasetOrientation.BASELINE );
        UnaryOperator<Event<Ensemble>> baselineValueTransformer = this.getEnsembleTransformer( leftValueTransformer,
                                                                                               baseline );

        UnaryOperator<TimeSeries<Ensemble>> baselineValueAndUnitTransformer =
                this.getEnsembleTransformer( baselineValueTransformer );

        // Currently only a seasonal filter, which applies equally to all sides
        Predicate<TimeSeries<Double>> singleValuedFilter = this.getSeasonalFilter( declaration.season() );
        Predicate<TimeSeries<Ensemble>> ensembleFilter = this.getSeasonalFilter( declaration.season() );

        // Get the time shifts
        Dataset left = project.getDeclaredDataset( DatasetOrientation.LEFT );
        Duration leftTimeShift = this.getTimeShift( left );
        Duration rightTimeShift = this.getTimeShift( right );
        Duration baselineTimeShift = this.getTimeShift( baseline );

        Duration pairFrequency = this.getPairFrequency( declaration );

        // Build and return the pool suppliers
        List<Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> rawSuppliers =
                new PoolsGenerator.Builder<Double, Ensemble>().setProject( project )
                                                              .setRetrieverFactory( retrieverFactory )
                                                              .setPoolRequests( poolRequests )
                                                              .setLeftTransformer( leftValueAndUnitTransformer )
                                                              .setRightTransformer( rightValueAndUnitTransformer )
                                                              .setBaselineTransformer( baselineValueAndUnitTransformer )
                                                              .setLeftUpscaler( leftUpscaler )
                                                              .setRightUpscaler( rightUpscaler )
                                                              .setBaselineUpscaler( baselineUpscaler )
                                                              .setLeftFilter( singleValuedFilter )
                                                              .setRightFilter( ensembleFilter )
                                                              .setBaselineFilter( ensembleFilter )
                                                              .setLeftTimeShift( leftTimeShift )
                                                              .setRightTimeShift( rightTimeShift )
                                                              .setBaselineTimeShift( baselineTimeShift )
                                                              .setPairer( pairer )
                                                              .setPairFrequency( pairFrequency )
                                                              .setCrossPairer( crossPairer )
                                                              .setClimateMapper( Double::doubleValue )
                                                              .setClimateAdmissibleValue( Double::isFinite )
                                                              .build()
                                                              .get();

        return this.getComposedSuppliers( poolRequests, rawSuppliers );
    }

    /**
     * Composes the raw suppliers with feature group information based on the pool requests, which are ordered 
     * identically to the suppliers.
     *
     * @param <T> the type of supplied data
     * @param poolRequests the pool requests
     * @param rawSuppliers the raw suppliers
     * @return the composed suppliers
     * @throws IllegalArgumentException if the number of pool requests and suppliers does not match
     */

    private <T> List<SupplierWithPoolRequest<T>> getComposedSuppliers( List<PoolRequest> poolRequests,
                                                                       List<Supplier<T>> rawSuppliers )
    {
        if ( poolRequests.size() != rawSuppliers.size() )
        {
            throw new IllegalArgumentException( "Expected as many pool requests as pool suppliers: ["
                                                + poolRequests.size()
                                                + ", "
                                                + rawSuppliers.size()
                                                + "]." );
        }

        List<SupplierWithPoolRequest<T>> composedSuppliers = new ArrayList<>();
        int count = poolRequests.size();
        for ( int i = 0; i < count; i++ )
        {
            PoolRequest poolRequest = poolRequests.get( i );
            Supplier<T> rawSupplier = rawSuppliers.get( i );
            SupplierWithPoolRequest<T> composedSupplier = SupplierWithPoolRequest.of( rawSupplier,
                                                                                      poolRequest,
                                                                                      poolRequest );
            composedSuppliers.add( composedSupplier );
        }

        return Collections.unmodifiableList( composedSuppliers );
    }

    /**
     * Unpacks each supplier and pairs it with the original request that produced it. The only difference between the
     * original request and the corresponding request attached to the supplier and obtained from 
     * {@link SupplierWithPoolRequest#poolRequest()} is the pool identifier and it is friendly to preserve these
     * identifiers, which are otherwise destroyed by feature-batching.
     *
     * @param <T> the type of data supplied
     * @param optimizedGroups the optimized pool requests
     * @param suppliers the suppliers
     * @return a list of requests to suppliers
     */

    private <T> List<Pair<PoolRequest, Supplier<T>>>
    unpack( Map<FeatureGroup, OptimizedPoolRequests> optimizedGroups,
            List<SupplierWithPoolRequest<T>> suppliers )
    {
        List<Pair<PoolRequest, Supplier<T>>> returnMe = new ArrayList<>();

        // Iterate through the suppliers
        for ( SupplierWithPoolRequest<T> nextSupplier : suppliers )
        {
            PoolRequest poolRequest = nextSupplier.poolRequest();
            PoolRequest optimizedPoolRequest = nextSupplier.optimizedPoolRequest();

            FeatureGroup nextOptimizedGroup = optimizedPoolRequest.getMetadata()
                                                                  .getFeatureGroup();
            FeatureGroup nextGroup = poolRequest.getMetadata()
                                                .getFeatureGroup();

            TimeWindowOuter timeWindow = poolRequest.getMetadata()
                                                    .getTimeWindow();

            OptimizedPoolRequests optimized = optimizedGroups.get( nextOptimizedGroup );
            PoolRequest original = optimized.getOriginalPoolRequest( nextGroup, timeWindow );

            Pair<PoolRequest, Supplier<T>> nextPair = Pair.of( original, nextSupplier );

            returnMe.add( nextPair );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Generates the {@link PoolRequest} associated with a particular {@link FeatureGroup} in order to drive pool 
     * creation.
     *
     * @param evaluation the evaluation description
     * @param declaration the project declaration
     * @param featureGroup the feature group
     * @param desiredTimeScale the desired time scale
     * @param timeWindows the time windows
     * @return the pool requests
     * @throws NullPointerException if any input is null
     * @throws PoolCreationException if the pool could not be created for any other reason
     */

    private List<PoolRequest> getPoolRequests( Evaluation evaluation,
                                               EvaluationDeclaration declaration,
                                               FeatureGroup featureGroup,
                                               TimeScaleOuter desiredTimeScale,
                                               Set<TimeWindowOuter> timeWindows )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( featureGroup );

        List<PoolRequest> poolRequests = new ArrayList<>();

        // Iterate the time windows, creating metadata for each
        for ( TimeWindowOuter timeWindow : timeWindows )
        {
            PoolMetadata mainMetadata = this.createMetadata( evaluation,
                                                             featureGroup,
                                                             timeWindow,
                                                             desiredTimeScale,
                                                             DatasetOrientation.RIGHT );

            // Create the basic metadata
            PoolMetadata baselineMetadata = null;
            if ( DeclarationUtilities.hasBaseline( declaration ) )
            {
                baselineMetadata = this.createMetadata( evaluation,
                                                        featureGroup,
                                                        timeWindow,
                                                        desiredTimeScale,
                                                        DatasetOrientation.BASELINE );
            }

            PoolRequest request = PoolRequest.of( mainMetadata, baselineMetadata );

            poolRequests.add( request );
        }

        return Collections.unmodifiableList( poolRequests );
    }

    /**
     * Returns an instance of a {@link TimeSeriesCrossPairer} or null if none is required.
     * @return a cross-pairer or null
     */

    private <L, R> TimeSeriesCrossPairer<L, R> getCrossPairerOrNull( EvaluationDeclaration declaration )
    {
        // Create a cross pairer, in case this is required by the declaration
        TimeSeriesCrossPairer<L, R> crossPairer = null;
        CrossPair crossPair = declaration.crossPair();
        if ( Objects.nonNull( crossPair ) )
        {
            crossPairer = TimeSeriesCrossPairer.of( crossPair );
        }

        return crossPairer;
    }

    /**
     * Returns a metadata representation of the input.
     *
     * @param evaluation the evaluation description
     * @param featureGroup the feature group
     * @param timeWindow the time window
     * @param desiredTimeScale the desired time scale
     * @param orientation the context for the data as it relates to the declaration
     * @return the metadata
     */

    private PoolMetadata createMetadata( wres.statistics.generated.Evaluation evaluation,
                                         FeatureGroup featureGroup,
                                         TimeWindowOuter timeWindow,
                                         TimeScaleOuter desiredTimeScale,
                                         DatasetOrientation orientation )
    {
        long innerPoolId = this.getNextPoolId();

        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      timeWindow, // Default to start with
                                                                      desiredTimeScale,
                                                                      null,
                                                                      orientation
                                                                      == DatasetOrientation.BASELINE,
                                                                      innerPoolId );

        return PoolMetadata.of( evaluation, pool );
    }

    /**
     * @return a sequential pool identifier
     */
    private long getNextPoolId()
    {
        // Updater for the pool identifier that avoids a long overflow
        LongUnaryOperator updater = next -> {
            if ( next + 1 < Long.MAX_VALUE )
            {
                return next + 1;
            }

            LOGGER.warn( "Resetting pool identification sequence to avoid long overflow." );

            return 1;
        };

        return this.getPoolIdGenerator()
                   .updateAndGet( updater );
    }

    /**
     * Returns a transformer that applies a unit conversion, followed by the input transformation.
     *
     * @param basicTransformer the transformer to apply after a unit conversion
     * @return a transformer that applies a unit conversion followed by the input transformer
     */

    private UnaryOperator<TimeSeries<Double>> getSingleValuedTransformer( DoubleUnaryOperator basicTransformer )
    {
        return toTransform -> {

            // Apply the unit mapping first, then the basic transformer
            String existingUnitString = toTransform.getMetadata()
                                                   .getUnit();
            String desiredUnitString = this.getUnitMapper()
                                           .getDesiredMeasurementUnitName();
            Map<String, String> aliases = this.getUnitMapper()
                                              .getUnitAliases();
            DoubleUnaryOperator innerUnitMapper = this.getUnitMapper( existingUnitString,
                                                                      desiredUnitString,
                                                                      aliases,
                                                                      toTransform.getTimeScale(),
                                                                      this.getProject()
                                                                          .getDesiredTimeScale() );

            UnaryOperator<TimeSeriesMetadata> metaMapper = metadata -> toTransform.getMetadata()
                                                                                  .toBuilder()
                                                                                  .setUnit( desiredUnitString )
                                                                                  .build();

            DoubleUnaryOperator transformer = basicTransformer.compose( innerUnitMapper );
            TimeSeries<Double> transformed = TimeSeriesSlicer.transform( toTransform,
                                                                         transformer::applyAsDouble,
                                                                         metaMapper );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Tranformed the values associated with time-series {}, producing a new time-series {}.",
                              toTransform.hashCode(),
                              transformed.hashCode() );
            }

            return transformed;
        };
    }

    /**
     * Returns a transformer that applies a unit conversion, followed by the input transformation.
     *
     * @param basicTransformer the transformer to apply after a unit conversion
     * @return a transformer that applies a unit conversion followed by the input transformer
     */

    private UnaryOperator<TimeSeries<Ensemble>>
    getEnsembleTransformer( UnaryOperator<Event<Ensemble>> basicTransformer )
    {
        return toTransform -> {
            // Apply the unit mapping first, then the basic transformer
            String existingUnitString = toTransform.getMetadata()
                                                   .getUnit();
            String desiredUnitString = this.getUnitMapper()
                                           .getDesiredMeasurementUnitName();
            Map<String, String> aliases = this.getUnitMapper()
                                              .getUnitAliases();

            DoubleUnaryOperator innerUnitMapper = this.getUnitMapper( existingUnitString,
                                                                      desiredUnitString,
                                                                      aliases,
                                                                      toTransform.getTimeScale(),
                                                                      this.getProject()
                                                                          .getDesiredTimeScale() );
            UnaryOperator<Event<Ensemble>> ensembleUnitMapper = this.getEnsembleUnitMapper( innerUnitMapper );
            UnaryOperator<Event<Ensemble>> transformer = event -> basicTransformer.compose( ensembleUnitMapper )
                                                                                  .apply( event );
            TimeSeries<Ensemble> transformed = TimeSeriesSlicer.transformByEvent( toTransform, transformer );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Tranformed the values associated with time-series {}, producing a new time-series {}.",
                              toTransform.hashCode(),
                              transformed.hashCode() );
            }

            return transformed;
        };
    }

    /**
     * @param existingUnitString the existing measurement unit string
     * @param desiredUnitString the desired measurement unit string
     * @param aliases a list of declared unit aliases
     * @return the unit mapper
     */

    private DoubleUnaryOperator getUnitMapper( String existingUnitString,
                                               String desiredUnitString,
                                               Map<String, String> aliases,
                                               TimeScaleOuter existingTimeScale,
                                               TimeScaleOuter desiredTimeScale )
    {
        DoubleUnaryOperator converter = this.converterCache.getIfPresent( existingUnitString );

        if ( Objects.isNull( converter ) )
        {
            // No conversion required
            if ( existingUnitString.equals( desiredUnitString ) )
            {
                converter = in -> in;
            }
            // Conversion required
            else
            {
                Unit<?> existingUnit = Units.getUnit( existingUnitString, aliases );
                Unit<?> desiredUnit = Units.getUnit( desiredUnitString, aliases );

                // Does the unit conversion involve a special time integration? If so, this is a type of upscaling 
                // because it involves a change in time scale function, as well as in the measurement units, and should 
                // be deferred to that activity. In other words, supply an identity converter here and log
                if ( Objects.nonNull( existingTimeScale )
                     && !existingTimeScale.equalsOrInstantaneous( desiredTimeScale )
                     && Units.isSupportedTimeIntegralConversion( existingUnit, desiredUnit ) )
                {
                    LOGGER.debug( "Encountered a request to convert {} to {}. Since these two units involve different "
                                  + "dimensions, this is a non-standard unit conversion that additionally involves a "
                                  + "time-integration. The time integration step is part of an upscaling operation and "
                                  + "is, therefore, deferred. It is assumed that the upscaler will also convert the "
                                  + "units. Based on that assumption, creating an identity converter instead.",
                                  existingUnitString,
                                  desiredUnitString );

                    converter = in -> in;
                }
                // Regular converter that does not depend on time integration
                else
                {
                    // Attempting to perform a unit conversion that cannot be solved without upscaling, so warn because 
                    // no upscaling was detected (see the clause immediately above)
                    if ( Units.isSupportedTimeIntegralConversion( existingUnit, desiredUnit ) )
                    {
                        LOGGER.warn( "Encountered a request to convert {} to {}. Since these two units involve "
                                     + "different dimensions, this is a non-standard unit conversion that additionally "
                                     + "involves a time-integration. This time integration problem can be solved by "
                                     + "upscaling, but no appropriate declaration was found to implement upscaling. "
                                     + "The existing time scale was {} and the desired time scale was {}. An error may "
                                     + "follow when attempting to convert between incommensurate units. If you "
                                     + "intended to solve this unit conversion problem by upscaling, please declare an "
                                     + "appropriate desiredTimeScale and try again.",
                                     existingUnitString,
                                     desiredUnitString,
                                     existingTimeScale,
                                     desiredTimeScale );
                    }

                    converter = this.getUnitMapper()
                                    .getUnitMapper( existingUnitString );

                    // Cache the converter for re-use
                    this.converterCache.put( existingUnitString, converter );
                }
            }
        }

        return converter;
    }

    /**
     * @param unitMapper the unit mapper
     * @return a function that maps the units of an ensemble event
     */

    private UnaryOperator<Event<Ensemble>> getEnsembleUnitMapper( DoubleUnaryOperator unitMapper )
    {
        return ensembleEvent -> {

            Ensemble ensemble = ensembleEvent.getValue();
            double[] values = ensemble.getMembers();
            double[] mappedValues = Arrays.stream( values )
                                          .map( unitMapper )
                                          .toArray();

            Ensemble convertedEnsemble = Ensemble.of( mappedValues, ensemble.getLabels() );

            return Event.of( ensembleEvent.getTime(), convertedEnsemble );
        };
    }

    /**
     * Returns a transformer for single-valued data if required.
     *
     * @param values the value declaration
     * @return a transformer or null
     */

    private DoubleUnaryOperator getSingleValuedTransformer( Values values )
    {
        if ( Objects.isNull( values ) )
        {
            return value -> value;
        }

        double assignToLowMiss = MissingValues.DOUBLE;
        double assignToHighMiss = MissingValues.DOUBLE;

        double minimum = Double.NEGATIVE_INFINITY;
        double maximum = Double.POSITIVE_INFINITY;

        if ( Objects.nonNull( values.belowMinimum() ) )
        {
            assignToLowMiss = values.belowMinimum();
        }

        if ( Objects.nonNull( values.aboveMaximum() ) )
        {
            assignToHighMiss = values.aboveMaximum();
        }

        if ( Objects.nonNull( values.minimum() ) )
        {
            minimum = values.minimum();
        }

        if ( Objects.nonNull( values.maximum() ) )
        {
            maximum = values.maximum();
        }

        // Effectively final constants for use 
        // within enclosing scope
        double assignLow = assignToLowMiss;
        double assignHigh = assignToHighMiss;

        double low = minimum;
        double high = maximum;

        return toTransform -> {

            // Low miss
            if ( toTransform < low )
            {
                return assignLow;
            }

            // High miss
            if ( toTransform > high )
            {
                return assignHigh;
            }

            // Within bounds
            return toTransform;
        };
    }

    /**
     * Returns a transformer for ensemble data if required.
     *
     * @param valueTransformer the value transformer to compose
     * @param dataset the data source declaration
     * @return a transformer or null
     */

    private UnaryOperator<Event<Ensemble>> getEnsembleTransformer( DoubleUnaryOperator valueTransformer,
                                                                   Dataset dataset )
    {
        // Return null to avoid iterating a no-op function
        if ( Objects.isNull( valueTransformer )
             && ( Objects.isNull( dataset )
                  || Objects.isNull( dataset.ensembleFilter() )
                  || dataset.ensembleFilter()
                            .members()
                            .isEmpty() ) )
        {
            return null;
        }

        List<String> inclusive = null;
        List<String> exclusive = null;

        if ( Objects.nonNull( dataset )
             && Objects.nonNull( dataset.ensembleFilter() ) )
        {
            EnsembleFilter ensembleFilter = dataset.ensembleFilter();
            if ( ensembleFilter.exclude() )
            {
                exclusive = ensembleFilter.members()
                                          .stream()
                                          .toList();
            }
            else
            {
                inclusive = ensembleFilter.members()
                                          .stream()
                                          .toList();
            }
        }

        List<String> finalInclusive = inclusive;
        List<String> finalExclusive = exclusive;

        return toTransform -> {

            Ensemble ensemble = toTransform.getValue();
            Labels ensLabels = ensemble.getLabels();

            // If filters were defined, they must select something
            if ( ( ( Objects.nonNull( finalInclusive ) && !finalInclusive.isEmpty() )
                   || ( Objects.nonNull( finalExclusive ) && !finalExclusive.isEmpty() ) )
                 && !ensLabels.hasLabels() )
            {
                throw new IllegalArgumentException( "When attempting to filter ensemble members, discovered some "
                                                    + "filters, but the ensemble members were not labelled, "
                                                    + "which is not allowed." );
            }

            double[] members = ensemble.getMembers();

            // Map the members
            if ( Objects.nonNull( valueTransformer ) )
            {
                members = Arrays.stream( members )
                                .map( valueTransformer )
                                .toArray();
            }

            Ensemble furtherFilter = Ensemble.of( members, ensLabels );
            furtherFilter = this.filterEnsembleMembers( furtherFilter, finalInclusive, finalExclusive );

            return Event.of( toTransform.getTime(), furtherFilter );
        };
    }

    /**
     * @param ensemble the ensemble to filter
     * @param finalInclusive the inclusive filters
     * @param finalExclusive the exclusive filters
     * @return the filtered ensemble
     */

    private Ensemble filterEnsembleMembers( Ensemble ensemble,
                                            List<String> finalInclusive,
                                            List<String> finalExclusive )
    {
        double[] members = ensemble.getMembers();
        Labels ensLabels = ensemble.getLabels();

        // Filter named members?
        if ( ensLabels.hasLabels() )
        {
            Map<String, Double> valuesToUse = new TreeMap<>();
            String[] labels = ensLabels.getLabels();

            // Iterate the members, map the units and discover the names and add to the map
            for ( int i = 0; i < members.length; i++ )
            {
                // Use this member?
                if ( this.getUseThisEnsembleName( labels[i], finalInclusive, finalExclusive ) )
                {
                    valuesToUse.put( labels[i], members[i] );
                }
            }

            // If inclusive filters were defined, they must select something
            if ( Objects.nonNull( finalInclusive ) && !finalInclusive.isEmpty() && valuesToUse.isEmpty() )
            {
                throw new IllegalArgumentException( "When attempting to filter ensemble members, discovered some "
                                                    + "inclusive filters, but no members matches these filters, "
                                                    + "which is not allowed. The filters were: "
                                                    + finalInclusive
                                                    + "." );
            }

            // Labels are cached centrally
            String[] names = valuesToUse.keySet()
                                        .toArray( new String[0] );
            ensLabels = Labels.of( names );
            members = valuesToUse.values()
                                 .stream()
                                 .mapToDouble( Double::doubleValue )
                                 .toArray();
        }

        return Ensemble.of( members, ensLabels );
    }

    /**
     * @param ensembleName the name to test
     * @param inclusive the inclusive names
     * @param exclusive the exclusive names
     * @return {@code true} if the ensemble identifier should be considered, otherwise false
     */

    private boolean getUseThisEnsembleName( String ensembleName,
                                            List<String> inclusive,
                                            List<String> exclusive )
    {
        boolean include = true;
        if ( Objects.nonNull( inclusive ) && !inclusive.isEmpty() )
        {
            include = inclusive.contains( ensembleName );
        }

        if ( Objects.nonNull( exclusive ) && !exclusive.isEmpty() )
        {
            include = include && !exclusive.contains( ensembleName );
        }

        return include;
    }

    /**
     * @param season the season
     * @return a seasonal filter
     */

    private <T> Predicate<TimeSeries<T>> getSeasonalFilter( Season season )
    {
        if ( Objects.isNull( season ) )
        {
            return in -> true;
        }

        MonthDay start = season.minimum();
        MonthDay end = season.maximum();

        return TimeSeriesSlicer.getSeasonFilter( start, end );
    }

    /**
     * Creates a feature-specific baseline generator, if required. Pay close attention to the sided-ness of the feature
     * names in this context because there are two different sources of data: 1) the source data from which the 
     * baseline is generated; and 2) the template time-series data that is mimicked. For example, when generating a 
     * persistence baseline, the former is a source of observation-like data and the latter may be a source of forecast-
     * like data, each of which has a different feature name.
     *
     * @param baseline the baseline declaration
     * @param retrieverFactory the factory to acquire a data source for a generated baseline
     * @param upscaler an upscaler, which is optional unless the generated series requires upscaling
     * @param admissibleValue a guard for admissible values of the generated baseline
     * @return a function that takes a set of features and returns a unary operator that generates a baseline
     */

    private <L, R> Function<Set<Feature>, UnaryOperator<TimeSeries<R>>> getGeneratedBaseline( BaselineDataset baseline,
                                                                                              RetrieverFactory<L, R> retrieverFactory,
                                                                                              TimeSeriesUpscaler<R> upscaler,
                                                                                              Predicate<R> admissibleValue )
    {
        Objects.requireNonNull( baseline );
        Objects.requireNonNull( retrieverFactory );

        // Has a generated baseline, only one supported for now: persistence
        if ( DeclarationUtilities.hasGeneratedBaseline( baseline ) )
        {
            LOGGER.trace( "Creating a persistence generator for data source {}.", baseline );

            // Default lag of 1
            int lag = 1;

            if ( Objects.nonNull( baseline.persistence() ) )
            {
                lag = baseline.persistence();
                LOGGER.debug( "Discovered a persistence baseline with a lag of {}.", lag );
            }

            // Map from the input data type to the required type
            int finalLag = lag;

            // Here the feature names supplied must be consistent with the source data from which the baseline is 
            // generated, not the template time-series that is mimicked
            return features -> {

                Supplier<Stream<TimeSeries<R>>> persistenceSource =
                        () -> retrieverFactory.getBaselineRetriever( features )
                                              .get();

                return PersistenceGenerator.of( persistenceSource,
                                                upscaler,
                                                admissibleValue,
                                                finalLag,
                                                this.getProject()
                                                    .getMeasurementUnit() );
            };
        }
        // Other types are not supported
        else
        {
            throw new UnsupportedOperationException( "While attempting to generate a baseline: unrecognized "
                                                     + "type of baseline to generate." );
        }
    }

    /**
     * Returns the type of time-based pairing to perform given the declared type of the datasets in the project.
     *
     * @param declaration the declaration
     * @return the type of time-based pairing to perform
     */

    private TimePairingType getTimePairingTypeFromDeclaration( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        TimePairingType returnMe = TimePairingType.REFERENCE_TIME_AND_VALID_TIME;

        if ( !DeclarationUtilities.isForecast( declaration.left() )
             || !DeclarationUtilities.isForecast( declaration.right() ) )
        {
            returnMe = TimePairingType.VALID_TIME_ONLY;
        }

        return returnMe;
    }

    /**
     * Checks the project declaration and throws an exception when the declared type is inconsistent with the type of
     * pools requested. 
     *
     * @param project the project
     * @param ensemble is true if ensemble pools were requested, false for single-valued pools
     * @throws IllegalArgumentException if the requested pools are inconsistent with the declaration
     */

    private void validateRequestedPoolsAgainstDeclaration( Project project, boolean ensemble )
    {
        Dataset right = project.getDeclaredDataset( DatasetOrientation.RIGHT );
        Dataset baseline = project.getDeclaredDataset( DatasetOrientation.BASELINE );
        DataType rightType = right.type();

        // Right
        if ( ensemble && rightType != DataType.ENSEMBLE_FORECASTS )
        {
            throw new IllegalArgumentException( "Requested pools for ensemble data, but the right "
                                                + DATA_IS_DECLARED_AS
                                                + rightType
                                                + WHICH_IS_NOT_ALLOWED );
        }
        else if ( !ensemble && rightType == DataType.ENSEMBLE_FORECASTS )
        {
            throw new IllegalArgumentException( "Requested pools for single-valued data, but the right "
                                                + DATA_IS_DECLARED_AS
                                                + rightType
                                                + WHICH_IS_NOT_ALLOWED );
        }

        // Baseline?
        if ( Objects.nonNull( baseline ) )
        {
            DataType baselineType = baseline.type();

            if ( ensemble && baselineType != DataType.ENSEMBLE_FORECASTS )
            {
                throw new IllegalArgumentException( "Requested pools for ensemble data, but the baseline "
                                                    + DATA_IS_DECLARED_AS
                                                    + baselineType
                                                    + WHICH_IS_NOT_ALLOWED );
            }
            else if ( !ensemble && baselineType == DataType.ENSEMBLE_FORECASTS )
            {
                throw new IllegalArgumentException( "Requested pools for single-valued data, but the baseline "
                                                    + DATA_IS_DECLARED_AS
                                                    + baselineType
                                                    + WHICH_IS_NOT_ALLOWED );
            }
        }
    }

    /**
     * Optimizes the input requests for feature-batched retrieval when the number of singleton feature groups is larger
     * than a minimum size.
     * @param poolRequests the pool requests to optimize
     * @param poolParameters the pool parameters
     * @return the optimized requests with a flag indicating whether optimization was performed
     */

    private Map<FeatureGroup, OptimizedPoolRequests>
    getFeatureBatchedSingletons( List<PoolRequest> poolRequests,
                                 PoolParameters poolParameters )
    {
        // Use a predictable order for the feature groups, since it aids with debugging/consistency across evaluations, 
        // i.e., an ordered map
        Map<FeatureGroup, OptimizedPoolRequests> returnMe = new TreeMap<>();

        // Create one collection of pool requests for each feature group
        Map<FeatureGroup, List<PoolRequest>> groups =
                poolRequests.stream()
                            .collect( Collectors.groupingBy( e -> e.getMetadata().getFeatureGroup() ) );

        // Create some collections that will be populated/cleared for each feature batch in turn
        Set<FeatureGroup> nextGroups = new HashSet<>();
        List<PoolRequest> nextOptimizedRequests = new ArrayList<>(); // The next batch of optimized requests
        List<PoolRequest> nextOriginalRequests = new ArrayList<>(); // The next batch of original requests

        // Are the pool requests suitable for feature batching?
        boolean shouldBatch = this.isFeatureBatchingAllowed( groups, poolParameters );

        // Group size for retrieval: either the batched retrieval size or the total number of features, if less
        int groupSize = Math.min( groups.size(), poolParameters.getFeatureBatchSize() );

        int newPoolRequestCount = 0;
        int originalPoolRequestCount = 0;

        // Loop the pool requests and gather them into groups for feature-batched retrieval, if the conditions are met
        for ( Map.Entry<FeatureGroup, List<PoolRequest>> nextEntry : groups.entrySet() )
        {
            FeatureGroup nextGroup = nextEntry.getKey();
            List<PoolRequest> nextPools = nextEntry.getValue();
            originalPoolRequestCount += nextPools.size();

            // More features than the minimum, batching is allowed and this is a singleton group, so batch it
            if ( shouldBatch && nextGroup.isSingleton() )
            {
                // Start a new batch
                if ( nextOptimizedRequests.isEmpty() )
                {
                    nextOptimizedRequests.addAll( nextPools );
                }

                nextGroups.add( nextGroup );
                nextOriginalRequests.addAll( nextPools );

                // Either reached the batch size or there are fewer features in total than the batch size, so complete
                // the batch
                if ( nextGroups.size() == groupSize )
                {
                    LOGGER.debug( "Created a batch of {} singleton feature groups for efficient retrieval. The "
                                  + "singleton groups to be treated as a batch are: {}.",
                                  groupSize,
                                  nextGroups );

                    FeatureGroup nextComposedGroup = this.getComposedFeatureGroup( nextGroups );
                    List<PoolRequest> nextAdjustedPools = this.setFeatureGroup( nextComposedGroup,
                                                                                nextOptimizedRequests );

                    OptimizedPoolRequests optimized = new OptimizedPoolRequests( nextAdjustedPools,
                                                                                 nextOriginalRequests );

                    returnMe.put( nextComposedGroup, optimized );
                    newPoolRequestCount += nextAdjustedPools.size();

                    // Clear the group
                    nextGroups.clear();
                    nextOptimizedRequests.clear();
                    nextOriginalRequests.clear();
                }
            }
            // Proceed without feature-batching
            else
            {
                LOGGER.debug( "Not performing feature batched retrieval for feature group {}.", nextGroup );

                OptimizedPoolRequests optimized = new OptimizedPoolRequests( nextPools, nextPools );
                returnMe.put( nextGroup, optimized );
                newPoolRequestCount += nextPools.size();
            }
        }

        // Is there a left over feature batch with fewer than this.FEATURE_BATCHED_RETRIEVAL_SIZE features?
        if ( !nextGroups.isEmpty() )
        {
            FeatureGroup nextComposedGroup = this.getComposedFeatureGroup( nextGroups );
            List<PoolRequest> nextAdjustedPools = this.setFeatureGroup( nextComposedGroup,
                                                                        nextOptimizedRequests );
            OptimizedPoolRequests optimized = new OptimizedPoolRequests( nextAdjustedPools,
                                                                         nextOriginalRequests );
            returnMe.put( nextComposedGroup, optimized );
            newPoolRequestCount += nextAdjustedPools.size();

            LOGGER.debug( "Created a batch of {} singleton feature groups for efficient retrieval. The "
                          + "singleton groups to be treated as a batch are: {}.",
                          nextGroups.size(),
                          nextGroups );
        }

        if ( shouldBatch && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Created {} optimized pool requests from {} original pool requests.",
                          newPoolRequestCount,
                          originalPoolRequestCount );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Determines whether feature-batching is allowed, based on the pool requests.
     *
     * @param requests the pool requests
     * @param poolParameters the pool parameters
     * @return true if feature-batching is allowed, otherwise false
     */

    private boolean isFeatureBatchingAllowed( Map<FeatureGroup, List<PoolRequest>> requests,
                                              PoolParameters poolParameters )
    {
        // Determine the number of singleton groups
        long singletonCount = requests.entrySet()
                                      .stream()
                                      .filter( next -> next.getKey()
                                                           .isSingleton() )
                                      .count();

        boolean shouldBatch = singletonCount > poolParameters.getFeatureBatchThreshold()
                              && poolParameters.getFeatureBatchSize() > 1
                              && this.arePoolsSuitableForFeatureBatching( requests );

        if ( LOGGER.isWarnEnabled() && shouldBatch )
        {
            LOGGER.warn( "Discovered {} singleton feature groups in the evaluation whose pool boundaries are consistent"
                         + " with each other and are, therefore, eligible for feature-batched retrieval. This exceeds "
                         + "the minimum of {} singleton groups at which feature-batching occurs. Optimizing the "
                         + "evaluation by conducting feature-batched retrieval in groups of {} features. This will "
                         + "consume more memory, but should improve retrieval speed. If an OutOfMemoryError occurs, "
                         + "consider reducing the number of features per batch.",
                         singletonCount,
                         poolParameters.getFeatureBatchThreshold(),
                         poolParameters.getFeatureBatchSize() );
        }

        return shouldBatch;
    }

    /**
     * Tests whether the pools are suitable for feature-batched retrieval; they are suitable if an identical set of 
     * pools is associated with every feature group, except for the feature group information itself.
     *
     * @param requests the pool requests
     * @return whether the pools are suitable for feature-batched retrieval
     */
    private boolean arePoolsSuitableForFeatureBatching( Map<FeatureGroup, List<PoolRequest>> requests )
    {
        List<PoolRequest> lastPools = null;
        for ( Map.Entry<FeatureGroup, List<PoolRequest>> nextEntry : requests.entrySet() )
        {
            List<PoolRequest> pools = nextEntry.getValue();
            List<PoolRequest> updatedPools = new ArrayList<>();

            // Clear the feature and pool numbering sequence
            for ( PoolRequest next : pools )
            {
                Evaluation evaluation = next.getMetadata()
                                            .getEvaluation();

                wres.statistics.generated.Pool clean = next.getMetadata()
                                                           .getPool()
                                                           .toBuilder()
                                                           .clearPoolId()
                                                           .clearGeometryGroup()
                                                           .clearGeometryTuples()
                                                           .clearRegionName()
                                                           .build();

                PoolMetadata updated = PoolMetadata.of( evaluation, clean );

                PoolMetadata updatedBaseline = null;
                if ( next.hasBaseline() )
                {
                    wres.statistics.generated.Pool cleanBase = next.getMetadataForBaseline()
                                                                   .getPool()
                                                                   .toBuilder()
                                                                   .clearPoolId()
                                                                   .clearGeometryGroup()
                                                                   .clearGeometryTuples()
                                                                   .clearRegionName()
                                                                   .build();

                    updatedBaseline = PoolMetadata.of( evaluation, cleanBase );
                }

                PoolRequest updatedRequest = PoolRequest.of( updated, updatedBaseline );
                updatedPools.add( updatedRequest );
            }

            // Equal, absent the expected differences?
            if ( Objects.nonNull( lastPools ) && !lastPools.equals( updatedPools ) )
            {
                return false;
            }

            lastPools = updatedPools;
        }

        return true;
    }

    /**
     * Updates the pool requests to use the specified feature group.
     *
     * @param featureGroup the feature group
     * @param poolRequests the pool requests to updated with the supplied feature group
     * @return the pool requests, each containing the new the feature group
     */

    private List<PoolRequest> setFeatureGroup( FeatureGroup featureGroup, List<PoolRequest> poolRequests )
    {
        List<PoolRequest> adjustedRequests = new ArrayList<>();

        for ( PoolRequest nextRequest : poolRequests )
        {
            PoolMetadata main = nextRequest.getMetadata();
            PoolMetadata baseline = nextRequest.getMetadataForBaseline();

            main = PoolMetadata.of( main, featureGroup );

            if ( Objects.nonNull( baseline ) )
            {
                baseline = PoolMetadata.of( baseline, featureGroup );
            }

            PoolRequest adjusted = PoolRequest.of( main, baseline );
            adjustedRequests.add( adjusted );
        }

        return Collections.unmodifiableList( adjustedRequests );
    }

    /**
     * Decomposes a list of pool suppliers that supply the pairs associated with all feature tuples in the specified 
     * feature groups into suppliers that supply the pairs for each feature tuple separately.
     *
     * @param <L> the left-ish data type
     * @param <R> the right-ish data type
     * @param toDecompose the pools suppliers to decompose
     * @return the decomposed pool suppliers, one for every pool and feature in the input
     */

    private <L, R> List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>>>
    decompose( List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>>> toDecompose )
    {
        // Decomposed suppliers
        List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>>> flattened = new ArrayList<>();

        for ( SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>> nextSupplier : toDecompose )
        {
            PoolRequest nextRequest = nextSupplier.poolRequest();
            FeatureGroup nextGroup = nextRequest.getMetadata()
                                                .getFeatureGroup();

            // Create decomposed pool requests here and use them below.
            Map<FeatureTuple, PoolRequest> decomposedRequests = this.decompose( nextRequest );

            // Decompose/map the pools by feature and cache that map for re-use
            Supplier<Map<FeatureTuple, Pool<TimeSeries<Pair<L, R>>>>> decomposed =
                    () -> PoolSlicer.decompose( nextSupplier.get(), PoolSlicer.getFeatureMapper() );
            Supplier<Map<FeatureTuple, Pool<TimeSeries<Pair<L, R>>>>> cachedDecomposed =
                    CachingSupplier.of( decomposed );

            // Create a supplier that extracts the required feature tuple from the multi-feature cache
            for ( FeatureTuple nextTuple : nextGroup.getFeatures() )
            {
                PoolRequest nextDecomposedRequest = decomposedRequests.get( nextTuple );

                Supplier<Pool<TimeSeries<Pair<L, R>>>> nextInnerSupplier = () -> {

                    Map<FeatureTuple, Pool<TimeSeries<Pair<L, R>>>> decomposedMap = cachedDecomposed.get();

                    // The expected feature tuple should be contained within the map
                    if ( !decomposedMap.containsKey( nextTuple ) )
                    {
                        LOGGER.debug( "While decomposing the pools for feature group {}, found no pools associated "
                                      + "with feature tuple {} among a map with these keys: {}.",
                                      nextGroup,
                                      nextTuple,
                                      decomposedMap.keySet() );

                        // Empty pool
                        return Pool.of( List.of(), nextDecomposedRequest.getMetadata() );
                    }

                    // Extract the pool for the current feature
                    Pool<TimeSeries<Pair<L, R>>> pool = decomposedMap.get( nextTuple );
                    PoolMetadata nextMain = nextDecomposedRequest.getMetadata();

                    // Baseline?
                    if ( pool.hasBaseline() )
                    {
                        Pool<TimeSeries<Pair<L, R>>> baselinePool = pool.getBaselineData();
                        PoolMetadata nextBase = nextDecomposedRequest.getMetadataForBaseline();

                        return Pool.of( pool.get(), nextMain, baselinePool.get(), nextBase, pool.getClimatology() );
                    }

                    // No baseline
                    return Pool.of( pool.get(), nextMain, null, null, pool.getClimatology() );
                };

                SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>> supplier =
                        SupplierWithPoolRequest.of( nextInnerSupplier,
                                                    nextDecomposedRequest,
                                                    nextRequest );

                flattened.add( supplier );
            }
        }

        LOGGER.debug( "Decomposed {} pools with one or more feature tuples into {} pools that each contained precisely "
                      + "one feature tuple.",
                      toDecompose.size(),
                      flattened.size() );

        // Return the flat list of suppliers in optimal execution order
        return this.sortForExecution( flattened );
    }

    /**
     * Sorts a list of feature-batched pools for optimal execution. It is assumed that all of the features in the 
     * supplied list belong to the same feature batch and, therefore, have strong data affinities. The sorting places
     * the suppliers in order of feature-tuple and then time window order to minimize heap usage. Pools should 
     * complete in an order that ensures common datasets become eligible for gc as soon as possible.
     *
     * @param <L> the left-ish data type
     * @param <R> the right-ish data type
     * @param toSort the suppliers to sort
     * @return the suppliers in optimal execution order
     */

    private <L, R> List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>>>
    sortForExecution( List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>>> toSort )
    {
        List<SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>>> mod = new ArrayList<>( toSort );
        Comparator<SupplierWithPoolRequest<Pool<TimeSeries<Pair<L, R>>>>> c = ( a, b ) -> {
            PoolMetadata left = a.poolRequest()
                                 .getMetadata();

            PoolMetadata right = b.poolRequest()
                                  .getMetadata();

            int compare = left.getFeatureGroup().compareTo( right.getFeatureGroup() );

            if ( compare != 0 )
            {
                return compare;
            }

            return left.getTimeWindow().compareTo( right.getTimeWindow() );
        };

        mod.sort( c );

        return Collections.unmodifiableList( mod );
    }

    /**
     * Decomposes a feature-batched pool request that contains several features into separate pool requests.
     *
     * @param poolRequest the pool request to decompose
     * @return the decomposed pool requests
     */

    private Map<FeatureTuple, PoolRequest> decompose( PoolRequest poolRequest )
    {
        PoolMetadata main = poolRequest.getMetadata();
        PoolMetadata base = poolRequest.getMetadataForBaseline();

        Map<FeatureTuple, PoolRequest> returnMe = new HashMap<>();
        for ( FeatureTuple nextFeature : main.getFeatureGroup().getFeatures() )
        {
            GeometryGroup geoGroup = MessageFactory.getGeometryGroup( nextFeature.toStringShort(), nextFeature );
            FeatureGroup singleton = FeatureGroup.of( geoGroup );
            wres.statistics.generated.Pool poolInner = main.getPool();
            wres.statistics.generated.Pool poolInnerWithId = poolInner.toBuilder()
                                                                      .clearPoolId()
                                                                      .build();
            PoolMetadata withId = PoolMetadata.of( main.getEvaluation(), poolInnerWithId );
            PoolMetadata mainAdj = PoolMetadata.of( withId, singleton );
            PoolMetadata baseAdj = null;

            if ( poolRequest.hasBaseline() )
            {
                baseAdj = PoolMetadata.of( base, singleton );
            }

            PoolRequest requestAdj = PoolRequest.of( mainAdj, baseAdj );
            returnMe.put( nextFeature, requestAdj );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Create a composed feature group from the collection of singleton groups.
     * @param singletons the singleton groups
     * @throws NullPointerException if the set of singletons is null
     */
    private FeatureGroup getComposedFeatureGroup( Set<FeatureGroup> singletons )
    {
        Objects.requireNonNull( singletons );

        if ( singletons.size() == 1 )
        {
            return singletons.iterator()
                             .next();
        }
        else
        {
            Set<FeatureTuple> features = singletons.stream()
                                                   .flatMap( next -> next.getFeatures()
                                                                         .stream() )
                                                   .collect( Collectors.toSet() );
            GeometryGroup geoGroup = MessageFactory.getGeometryGroup( null, features );
            return FeatureGroup.of( geoGroup );
        }
    }

    /**
     * @param dataset the data source declaration
     * @return the declared time shift or null if no time shift is declared
     */
    private Duration getTimeShift( Dataset dataset )
    {
        Duration timeShift = Duration.ZERO; // Benign time shift

        if ( Objects.nonNull( dataset )
             && Objects.nonNull( dataset.timeShift() ) )
        {
            timeShift = dataset.timeShift();
        }

        return timeShift;
    }

    /**
     * Gets the frequency of the pairs, if declared.
     *
     * @param declaration the declaration
     * @return the pair frequency or null
     */

    private Duration getPairFrequency( EvaluationDeclaration declaration )
    {
        Duration frequency = null;

        // Obtain from the declaration if available
        if ( Objects.nonNull( declaration.pairFrequency() ) )
        {
            frequency = declaration.pairFrequency();
        }

        return frequency;
    }

    /**
     * @return the project instance
     */

    private Project getProject()
    {
        return this.project;
    }

    /**
     * @return the unit mapper
     */

    private UnitMapper getUnitMapper()
    {
        return this.unitMapper;
    }

    /**
     * @return the pool identifier generator.
     */

    private AtomicLong getPoolIdGenerator()
    {
        return this.poolId;
    }

    /**
     * @return whether the climatological and baseline data sources are equal and can be de-duplicated on retrieval
     */

    private boolean hasEqualBaselineAndClimatology()
    {
        Project localProject = this.getProject();
        if ( !localProject.hasGeneratedBaseline() )
        {
            return false;
        }

        if ( !Objects.equals( localProject.getVariableName( DatasetOrientation.LEFT ),
                              localProject.getVariableName( DatasetOrientation.BASELINE ) ) )
        {
            return false;
        }

        // Are the declared data sources equal?
        // A common assumption throughout WRES is that sources can be de-duplicated on the basis of declaration and that
        // any runtime differences, based on when reading/ingest calls an external source for a snapshot, should be 
        // ignored - in other words, the first snapshot wins, because this allows for de-duplication
        Dataset left = project.getDeclaredDataset( DatasetOrientation.LEFT );
        Dataset baseline = project.getDeclaredDataset( DatasetOrientation.BASELINE );
        return Objects.equals( left.sources(), baseline.sources() );
    }

    /**
     * A possibly optimized collection of pool requests. There are up to N optimized pool requests and M corresponding
     * original requests. The original pool requests are optimized if M > N. There is one original request for each
     * geographic feature group and time window and an interface is provided to acquire the original request that
     * corresponds to a prescribed feature group and time window. Where M > N, the supply that corresponds to each
     * optimized request will ultimately need to be decomposed into the separate supplies that correspond to the
     * original pool requests.
     * @param optimizedRequests  The optimized pool request.
     * @param originalRequests  The original pool requests.
     */

    private record OptimizedPoolRequests( List<PoolRequest> optimizedRequests, List<PoolRequest> originalRequests )
    {
        /**
         * @return true if the pool request is an optimized request
         */

        private boolean isOptimized()
        {
            return this.originalRequests.size() > this.optimizedRequests.size();
        }

        /**
         * @param feature the feature group
         * @param timeWindow the time window
         * @return the original request that corresponds to the prescribed feature
         * @throws NullPointerException if either input is null
         * @throws IllegalArgumentException if precisely one request was not found
         */

        private PoolRequest getOriginalPoolRequest( FeatureGroup feature, TimeWindowOuter timeWindow )
        {
            Objects.requireNonNull( feature );
            Objects.requireNonNull( timeWindow );
            List<PoolRequest> requests = this.originalRequests.stream()
                                                              .filter( next -> feature.equals( next.getMetadata()
                                                                                                   .getFeatureGroup() )
                                                                               && timeWindow.equals( next.getMetadata()
                                                                                                         .getTimeWindow() ) )
                                                              .toList();

            if ( requests.size() != 1 )
            {
                Set<FeatureGroup> availableGroups = this.originalRequests.stream()
                                                                         .map( next -> next.getMetadata()
                                                                                           .getFeatureGroup() )
                                                                         .collect( Collectors.toSet() );

                throw new IllegalArgumentException( "Failed to identify the original pool request that corresponds to "
                                                    + "the prescribed feature group, "
                                                    + feature
                                                    + ". Discovered "
                                                    + requests.size()
                                                    + " pool requests associated with that feature group. The "
                                                    + "available feature groups are: "
                                                    + availableGroups
                                                    + "." );
            }

            return requests.get( 0 );
        }

        /**
         * @param optimizedRequests the optimized requests, required and not empty
         * @param originalRequests, the original requests, required and not empty
         * @throws NullPointerException if any input is null
         * @throws IllegalArgumentException if either set of requests is empty or the features in the original
         *            requests don't match the features in the optimized requests
         */

        private OptimizedPoolRequests( List<PoolRequest> optimizedRequests, List<PoolRequest> originalRequests )
        {
            Objects.requireNonNull( optimizedRequests );
            Objects.requireNonNull( originalRequests );

            if ( optimizedRequests.isEmpty() )
            {
                throw new IllegalArgumentException( "Cannot create an empty collection of optimized pool requests." );
            }

            if ( originalRequests.isEmpty() )
            {
                throw new IllegalArgumentException( "Cannot create a collection of optimized pool requests with an "
                                                    + "empty list of original requests from which the optimized "
                                                    + "requests were derived." );
            }

            Set<FeatureTuple> optimizedFeatures =
                    optimizedRequests.stream()
                                     .flatMap( next -> next.getMetadata().getFeatureTuples().stream() )
                                     .collect( Collectors.toSet() );

            Set<FeatureTuple> originalFeatures =
                    originalRequests.stream()
                                    .flatMap( next -> next.getMetadata().getFeatureTuples().stream() )
                                    .collect( Collectors.toSet() );

            if ( !optimizedFeatures.equals( originalFeatures ) )
            {
                throw new IllegalArgumentException( "The features in the optimized list of pool requests do not match "
                                                    + "the features in the original list of pool requests. The "
                                                    + "optimized list contains features: "
                                                    + optimizedFeatures
                                                    + ", whereas the original list contains features: "
                                                    + originalFeatures
                                                    + "." );
            }

            // Create immutable copies
            this.optimizedRequests = List.copyOf( optimizedRequests );
            this.originalRequests = List.copyOf( originalRequests );

            LOGGER.debug( "Created a collection of {} optimized pool requests across {} features from {} original "
                          + "requests across {} features.",
                          optimizedRequests.size(),
                          optimizedFeatures.size(),
                          originalRequests.size(),
                          originalFeatures.size() );
        }
    }

    /**
     * A supplier that keeps track of the {@link PoolRequest} that it is fulfilling.
     * @param delegated delegated supplier.
     * @param poolRequest the pool request.
     * @param optimizedPoolRequest the optimized pool request, else the {@link #poolRequest} where no optimization was
     *                             conducted.
     */

    private record SupplierWithPoolRequest<T>( Supplier<T> delegated, PoolRequest poolRequest,
                                               PoolRequest optimizedPoolRequest ) implements Supplier<T>
    {

        @Override
        public T get()
        {
            return this.delegated.get();
        }

        /**
         * Create an instance.
         * @param delegated the delegated supplier
         * @param poolRequest the pool request
         * @param optimizedPoolRequest either the same as the poolRequest or a composed/optimized pool request
         */

        private static <T> SupplierWithPoolRequest<T> of( Supplier<T> delegated,
                                                          PoolRequest poolRequest,
                                                          PoolRequest optimizedPoolRequest )
        {
            return new SupplierWithPoolRequest<>( delegated, poolRequest, optimizedPoolRequest );
        }

        /**
         * @param delegated the delegated supplier
         * @param poolRequest the pool request
         */
        private SupplierWithPoolRequest
        {
            Objects.requireNonNull( delegated );
            Objects.requireNonNull( poolRequest );
            Objects.requireNonNull( optimizedPoolRequest );
        }
    }

    /**
     * Creates an instance from a {@link Project}.
     *
     * @param project the project
     * @throws NullPointerException if the project is null
     */

    private PoolFactory( Project project )
    {
        Objects.requireNonNull( project, CANNOT_CREATE_POOLS_FROM_A_NULL_PROJECT );
        this.project = project;

        // Create a unit mapper
        String desiredMeasurementUnit = project.getMeasurementUnit();
        EvaluationDeclaration declaration = project.getDeclaration();

        this.unitMapper = UnitMapper.of( desiredMeasurementUnit,
                                         declaration.unitAliases() );
    }
}