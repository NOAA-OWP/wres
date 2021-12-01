package wres.io.pooling;

import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.CrossPair;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SourceTransformationType;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.RemoveMemberByValidYear;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesCrossPairer.MatchMode;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesOfEnsembleUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairer.TimePairingType;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.generators.PersistenceGenerator;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.CachingRetriever;
import wres.io.retrieval.CachingSupplier;
import wres.io.retrieval.RetrieverFactory;
import wres.statistics.generated.Evaluation;

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

    /** Used to create a pool identifier. */
    private static final AtomicLong POOL_ID = new AtomicLong( 0 );

    private static final String CANNOT_CREATE_POOLS_WITHOUT_A_RETRIEVER_FACTORY =
            "Cannot create pools without a retriever factory.";

    private static final String CANNOT_CREATE_POOLS_WITHOUT_LIST_OF_POOL_REQUESTS =
            "Cannot create pools without list of pool requests.";

    private static final String CANNOT_CREATE_POOLS_FROM_A_NULL_PROJECT = "Cannot create pools from a null project.";

    private static final String CANNOT_CREATE_POOLS_WITHOUT_POOL_PARAMETERS = "Cannot create pools without pool "
                                                                              + "parameters.";

    /**
     * Create pools for single-valued data. This method will attempt to retrieve and re-use data that is common to 
     * multiple pools. In particular, it will cache the climatological data for all pool requests that belong to a 
     * single feature group because the climatological data is shared across feature groups.
     * 
     * TODO: further optimize to re-use datasets that are shared between multiple feature groups, other than 
     * climatology, such as one feature that is part of several feature groups.
     * 
     * @param project the project for which pools are required, not null
     * @param poolRequests the pool requests, not null
     * @param retrieverFactory the retriever factory, not null
     * @param poolParameters the pool parameters
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public static List<Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> getSingleValuedPools( Project project,
                                                                                               List<PoolRequest> poolRequests,
                                                                                               RetrieverFactory<Double, Double> retrieverFactory,
                                                                                               PoolParameters poolParameters )
    {
        Objects.requireNonNull( project, CANNOT_CREATE_POOLS_FROM_A_NULL_PROJECT );
        Objects.requireNonNull( poolRequests, CANNOT_CREATE_POOLS_WITHOUT_LIST_OF_POOL_REQUESTS );
        Objects.requireNonNull( retrieverFactory, CANNOT_CREATE_POOLS_WITHOUT_A_RETRIEVER_FACTORY );
        Objects.requireNonNull( poolParameters, CANNOT_CREATE_POOLS_WITHOUT_POOL_PARAMETERS );

        // Create one collection of pools for each feature group in the requests
        Map<FeatureGroup, List<PoolRequest>> groups =
                poolRequests.stream()
                            .collect( Collectors.groupingBy( e -> e.getMetadata().getFeatureGroup() ) );

        // Optimize, if possible, by conducting feature-batched retrieval
        Map<DecomposableFeatureGroup, List<PoolRequest>> optimizedGroups =
                PoolFactory.getFeatureBatchedSingletons( groups, poolParameters );

        List<Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> suppliers = new ArrayList<>();

        for ( Map.Entry<DecomposableFeatureGroup, List<PoolRequest>> nextEntry : optimizedGroups.entrySet() )
        {
            DecomposableFeatureGroup nextGroup = nextEntry.getKey();
            FeatureGroup featureGroup = nextGroup.getComposedGroup();
            List<PoolRequest> nextPoolRequests = nextEntry.getValue();

            LOGGER.debug( "Building pool suppliers for feature group {}, which contains {} pool requests.",
                          featureGroup,
                          nextPoolRequests.size() );

            // Create a retriever factory that caches the climatological data for all pool requests if needed
            RetrieverFactory<Double, Double> cachingFactory = retrieverFactory;
            if ( project.hasProbabilityThresholds() || project.hasGeneratedBaseline() )
            {
                LOGGER.debug( "Building a caching retriever factory to cache the retrieval of the climatological data "
                              + "across all pools within feature group {}.",
                              featureGroup );

                cachingFactory = new ClimatologyCachedRetrieverFactory<>( retrieverFactory );
            }

            List<Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> nextSuppliers =
                    PoolFactory.getSingleValuedPoolsInner( project, nextPoolRequests, cachingFactory );

            // Optimized? In that case, decompose the feature-batched pools into feature-specific pools
            if ( nextGroup.isComposed() )
            {
                nextSuppliers = PoolFactory.decompose( nextGroup.getGroups(), nextSuppliers );
            }

            suppliers.addAll( nextSuppliers );
        }

        return Collections.unmodifiableList( suppliers );
    }

    /**
     * Create pools for ensemble data. This method will attempt to retrieve and re-use data that is common to multiple 
     * pools. In particular, it will cache the climatological data for all pool requests that belong to a single 
     * feature group because the climatological data is shared across feature groups.
     * 
     * TODO: further optimize to re-use datasets that are shared between multiple feature groups, other than 
     * climatology, such as one feature that is part of several feature groups.
     * 
     * @param project the project for which pools are required, not null
     * @param poolRequests the pool requests, not null
     * @param retrieverFactory the retriever factory, not null
     * @param poolParameters the pool parameters
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for ensemble data
     */

    public static List<Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> getEnsemblePools( Project project,
                                                                                             List<PoolRequest> poolRequests,
                                                                                             RetrieverFactory<Double, Ensemble> retrieverFactory,
                                                                                             PoolParameters poolParameters )
    {
        Objects.requireNonNull( project, CANNOT_CREATE_POOLS_FROM_A_NULL_PROJECT );
        Objects.requireNonNull( poolRequests, CANNOT_CREATE_POOLS_WITHOUT_LIST_OF_POOL_REQUESTS );
        Objects.requireNonNull( retrieverFactory, CANNOT_CREATE_POOLS_WITHOUT_A_RETRIEVER_FACTORY );
        Objects.requireNonNull( poolParameters, CANNOT_CREATE_POOLS_WITHOUT_POOL_PARAMETERS );

        // Create one collection of pools for each feature group in the requests
        Map<FeatureGroup, List<PoolRequest>> groups =
                poolRequests.stream()
                            .collect( Collectors.groupingBy( e -> e.getMetadata().getFeatureGroup() ) );

        // Optimize, if possible, by conducting feature-batched retrieval
        Map<DecomposableFeatureGroup, List<PoolRequest>> optimizedGroups =
                PoolFactory.getFeatureBatchedSingletons( groups, poolParameters );

        List<Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> suppliers = new ArrayList<>();

        for ( Map.Entry<DecomposableFeatureGroup, List<PoolRequest>> nextEntry : optimizedGroups.entrySet() )
        {
            DecomposableFeatureGroup nextGroup = nextEntry.getKey();
            FeatureGroup featureGroup = nextGroup.getComposedGroup();
            List<PoolRequest> nextPoolRequests = nextEntry.getValue();

            LOGGER.debug( "Building pool suppliers for feature group {}, which contains {} pool requests.",
                          featureGroup,
                          nextPoolRequests.size() );

            // Create a retriever factory that caches the climatological data for all pool requests if needed
            RetrieverFactory<Double, Ensemble> cachingFactory = retrieverFactory;
            if ( project.hasProbabilityThresholds() || project.hasGeneratedBaseline() )
            {
                LOGGER.debug( "Building a caching retriever factory to cache the retrieval of the climatological data "
                              + "across all pools within feature group {}.",
                              featureGroup );

                cachingFactory = new ClimatologyCachedRetrieverFactory<>( retrieverFactory );
            }

            List<Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> nextSuppliers =
                    PoolFactory.getEnsemblePoolsInner( project, nextPoolRequests, cachingFactory );

            // Optimized? In that case, decompose the feature-batched pools into feature-specific pools
            if ( nextGroup.isComposed() )
            {
                nextSuppliers = PoolFactory.decompose( nextGroup.getGroups(), nextSuppliers );
            }

            suppliers.addAll( nextSuppliers );
        }

        return Collections.unmodifiableList( suppliers );
    }

    /**
     * Generates the {@link PoolRequest} associated with a particular {@link Project} in order to drive pool creation.
     * 
     * @param evaluation the evaluation description
     * @param project the project
     * @return the pool requests
     * @throws NullPointerException if any input is null
     * @throws PoolCreationException if the pool could not be created for any other reason
     */

    public static List<PoolRequest> getPoolRequests( Evaluation evaluation,
                                                     Project project )
    {
        Objects.requireNonNull( project );

        Set<FeatureGroup> featureGroups = project.getFeatureGroups();
        ProjectConfig projectConfig = project.getProjectConfig();

        return featureGroups.stream()
                            .flatMap( nextGroup -> PoolFactory.getPoolRequests( evaluation, projectConfig, nextGroup )
                                                              .stream() )
                            .collect( Collectors.toUnmodifiableList() );
    }

    /**
     * Create pools for single-valued data. This method will attempt to retrieve and re-use data that is common to 
     * multiple pools. Thus, it is generally better to provide a list of pool requests that represent connected pools, 
     * such as pools that all belong to the same feature group, rather than supplying a single pool or a long list of 
     * unconnected pools, such as pools that belong to many feature groups.
     * 
     * TODO: analyze the pool requests and find groups with shared data, i.e., automatically analyze/optimize. In that
     * case, there should be one call to this method per evaluation, which should then forward each group of requests 
     * for batched creation of suppliers, as this method currently assumes the caller will do.
     * 
     * @param project the project for which pools are required, not null
     * @param poolRequests the pool requests, not null
     * @param retrieverFactory the retriever factory, not null
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    private static List<Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> getSingleValuedPoolsInner( Project project,
                                                                                                     List<PoolRequest> poolRequests,
                                                                                                     RetrieverFactory<Double, Double> retrieverFactory )
    {
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        DataSourceConfig baselineConfig = inputsConfig.getBaseline();

        // Check that the project declaration is consistent with a request for single-valued pools
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, false );

        long projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}'.",
                      projectId );

        // Create a default pairer for finite left and right values
        TimePairingType timePairingType = PoolFactory.getTimePairingTypeFromInputsConfig( inputsConfig );

        LOGGER.debug( "Using a time-based pairing strategy of {} for the input declaration {}.",
                      timePairingType,
                      inputsConfig );

        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of( Double::isFinite,
                                                                                  Double::isFinite,
                                                                                  timePairingType );

        // Create a cross pairer, in case this is required by the declaration
        TimeSeriesCrossPairer<Double, Double> crossPairer = PoolFactory.getCrossPairerOrNull( pairConfig );

        // Create a default upscaler
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();

        // Create a feature-specific baseline generator function (e.g., persistence), if required
        Function<Set<FeatureKey>, UnaryOperator<TimeSeries<Double>>> baselineGenerator = null;
        // Generated baseline declared?
        if ( project.hasGeneratedBaseline() )
        {
            LOGGER.debug( "While creating pools for project '{}', discovered a baseline "
                          + "to generate from a data source.",
                          projectId );

            baselineGenerator = PoolFactory.getGeneratedBaseline( baselineConfig,
                                                                  retrieverFactory,
                                                                  upscaler,
                                                                  Double::isFinite );
        }

        // Create any required transformers for value constraints
        // Left transformer is a straightforward value transformer
        DoubleUnaryOperator leftTransformer = PoolFactory.getSingleValuedTransformer( pairConfig.getValues() );
        // Right transformer may consider the encapsulating event
        UnaryOperator<Event<Double>> rightTransformer =
                next -> Event.of( next.getTime(), leftTransformer.applyAsDouble( next.getValue() ) );

        // Build and return the pool suppliers
        return new PoolsGenerator.Builder<Double, Double>().setProject( project )
                                                           .setRetrieverFactory( retrieverFactory )
                                                           .setPoolRequests( poolRequests )
                                                           .setBaselineGenerator( baselineGenerator )
                                                           .setLeftTransformer( leftTransformer::applyAsDouble )
                                                           .setRightTransformer( rightTransformer )
                                                           .setLeftUpscaler( upscaler )
                                                           .setRightUpscaler( upscaler )
                                                           .setPairer( pairer )
                                                           .setCrossPairer( crossPairer )
                                                           .setClimateMapper( Double::doubleValue )
                                                           .setClimateAdmissibleValue( Double::isFinite )
                                                           .build()
                                                           .get();
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

    private static List<Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> getEnsemblePoolsInner( Project project,
                                                                                                   List<PoolRequest> poolRequests,
                                                                                                   RetrieverFactory<Double, Ensemble> retrieverFactory )
    {
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();

        // Check that the project declaration is consistent with a request for ensemble pools
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, true );

        long projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}'.",
                      projectId );

        // Create a default pairer for finite left values and one or more finite right values
        TimePairingType timePairingType = PoolFactory.getTimePairingTypeFromInputsConfig( inputsConfig );

        LOGGER.debug( "Using a time-based pairing strategy of {} for the input declaration {}.",
                      timePairingType,
                      inputsConfig );

        TimeSeriesPairer<Double, Ensemble> pairer =
                TimeSeriesPairerByExactTime.of( Double::isFinite,
                                                en -> Arrays.stream( en.getMembers() )
                                                            .anyMatch( Double::isFinite ),
                                                timePairingType );

        // Create a cross pairer, in case this is required by the declaration
        TimeSeriesCrossPairer<Double, Ensemble> crossPairer = PoolFactory.getCrossPairerOrNull( pairConfig );

        // Create a default upscaler for left-ish data
        TimeSeriesUpscaler<Double> leftUpscaler = TimeSeriesOfDoubleUpscaler.of();
        TimeSeriesUpscaler<Ensemble> rightUpscaler = TimeSeriesOfEnsembleUpscaler.of();

        // Left transformer
        DoubleUnaryOperator leftTransformer = PoolFactory.getSingleValuedTransformer( pairConfig.getValues() );

        // Right transformer
        MonthDay removeMemberByValidYearRight = PoolFactory.getRemoveMemberByValidYear( inputsConfig.getRight() );
        UnaryOperator<Event<Ensemble>> rightTransformer = PoolFactory.getEnsembleTransformer( leftTransformer,
                                                                                              removeMemberByValidYearRight );

        //Baseline transformer
        MonthDay removeMemberByValidYearBaseline = PoolFactory.getRemoveMemberByValidYear( inputsConfig.getBaseline() );
        UnaryOperator<Event<Ensemble>> baselineTransformer = PoolFactory.getEnsembleTransformer( leftTransformer,
                                                                                                 removeMemberByValidYearBaseline );

        // Build and return the pool suppliers
        return new PoolsGenerator.Builder<Double, Ensemble>().setProject( project )
                                                             .setRetrieverFactory( retrieverFactory )
                                                             .setPoolRequests( poolRequests )
                                                             .setLeftTransformer( leftTransformer::applyAsDouble )
                                                             .setRightTransformer( rightTransformer )
                                                             .setBaselineTransformer( baselineTransformer )
                                                             .setLeftUpscaler( leftUpscaler )
                                                             .setRightUpscaler( rightUpscaler )
                                                             .setPairer( pairer )
                                                             .setCrossPairer( crossPairer )
                                                             .setClimateMapper( Double::doubleValue )
                                                             .setClimateAdmissibleValue( Double::isFinite )
                                                             .build()
                                                             .get();
    }

    /**
     * Generates the {@link PoolRequest} associated with a particular {@link FeatureGroup} in order to drive pool 
     * creation.
     * 
     * @param evaluation the evaluation description
     * @param projectConfig the project declaration
     * @param featureGroup the feature group
     * @return the pool requests
     * @throws NullPointerException if any input is null
     * @throws PoolCreationException if the pool could not be created for any other reason
     */

    private static List<PoolRequest> getPoolRequests( Evaluation evaluation,
                                                      ProjectConfig projectConfig,
                                                      FeatureGroup featureGroup )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( featureGroup );

        PairConfig pairConfig = projectConfig.getPair();

        // Get the desired time scale
        TimeScaleOuter desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

        // Get the time windows and sort them
        Set<TimeWindowOuter> timeWindows =
                new TreeSet<>( TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig ) );

        List<PoolRequest> poolRequests = new ArrayList<>();

        // Iterate the time windows, creating metadata for each
        for ( TimeWindowOuter timeWindow : timeWindows )
        {
            PoolMetadata mainMetadata = PoolFactory.createMetadata( evaluation,
                                                                    featureGroup,
                                                                    timeWindow,
                                                                    desiredTimeScale,
                                                                    LeftOrRightOrBaseline.RIGHT );

            // Create the basic metadata
            PoolMetadata baselineMetadata = null;
            if ( ConfigHelper.hasBaseline( projectConfig ) )
            {
                baselineMetadata = PoolFactory.createMetadata( evaluation,
                                                               featureGroup,
                                                               timeWindow,
                                                               desiredTimeScale,
                                                               LeftOrRightOrBaseline.BASELINE );
            }

            PoolRequest request = PoolRequest.of( mainMetadata, baselineMetadata );

            poolRequests.add( request );
        }

        return Collections.unmodifiableList( poolRequests );
    }

    /**
     * Returns an instance of a {@link TimeSeriesCrossPairer} or null if none is required.
     * 
     * 
     * @return a cross-pairer or null
     */

    private static <L, R> TimeSeriesCrossPairer<L, R> getCrossPairerOrNull( PairConfig pairConfig )
    {
        // Create a cross pairer, in case this is required by the declaration
        TimeSeriesCrossPairer<L, R> crossPairer = null;
        CrossPair crossPair = pairConfig.getCrossPair();
        if ( Objects.nonNull( crossPair ) )
        {
            MatchMode matchMode = crossPair.isExact() ? MatchMode.EXACT : MatchMode.FUZZY;
            crossPairer = TimeSeriesCrossPairer.of( matchMode );
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
     * @param leftOrRightOrBaseline the context for the data as it relates to the declaration
     * @return the metadata
     */

    private static PoolMetadata createMetadata( wres.statistics.generated.Evaluation evaluation,
                                                FeatureGroup featureGroup,
                                                TimeWindowOuter timeWindow,
                                                TimeScaleOuter desiredTimeScale,
                                                LeftOrRightOrBaseline leftOrRightOrBaseline )
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

        // Next identifier
        long nextId = PoolFactory.POOL_ID.updateAndGet( updater );

        wres.statistics.generated.Pool pool = MessageFactory.parse( featureGroup,
                                                                    timeWindow, // Default to start with
                                                                    desiredTimeScale,
                                                                    null,
                                                                    leftOrRightOrBaseline == LeftOrRightOrBaseline.BASELINE,
                                                                    nextId );

        return PoolMetadata.of( evaluation, pool );
    }

    /**
     * Returns a transformer for single-valued data if required.
     * 
     * @param valueConfig the value declaration 
     * @return a transformer or null
     */

    private static DoubleUnaryOperator getSingleValuedTransformer( DoubleBoundsType valueConfig )
    {
        if ( Objects.isNull( valueConfig ) )
        {
            return value -> value;
        }

        double assignToLowMiss = MissingValues.DOUBLE;
        double assignToHighMiss = MissingValues.DOUBLE;

        double minimum = Double.NEGATIVE_INFINITY;
        double maximum = Double.POSITIVE_INFINITY;

        if ( Objects.nonNull( valueConfig.getDefaultMinimum() ) )
        {
            assignToLowMiss = valueConfig.getDefaultMinimum();
        }

        if ( Objects.nonNull( valueConfig.getDefaultMaximum() ) )
        {
            assignToHighMiss = valueConfig.getDefaultMaximum();
        }

        if ( Objects.nonNull( valueConfig.getMinimum() ) )
        {
            minimum = valueConfig.getMinimum();
        }

        if ( Objects.nonNull( valueConfig.getMaximum() ) )
        {
            maximum = valueConfig.getMaximum();
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
     * Returns a transformer for ensemble data if required. Applies up to two separate transforms. If the 
     * {@code valueTransformer} is not {code null}, then the ensemble member values are re-mapped with that
     * transformer. Separately, if the prescribed {@code removeMemberByValidYear} is not {@code null}, then any
     * ensemble member whose label corresponds to the valid year that begins on that monthday will be removed. 
     * See the {@link TimeSeriesSlicer#filter(Event, java.time.MonthDay)} for the second transformer.
     * 
     * @param valueTransformer the value transformer to compose
     * @return a transformer or null
     */

    private static UnaryOperator<Event<Ensemble>> getEnsembleTransformer( DoubleUnaryOperator valueTransformer,
                                                                          MonthDay removeMemberByValidYear )
    {
        // Return null to avoid iterating a no-op function
        if ( Objects.isNull( valueTransformer ) && Objects.isNull( removeMemberByValidYear ) )
        {
            return null;
        }

        // At least one function must be iterated, so create no-op as a baseline
        UnaryOperator<Event<Ensemble>> valueFunction = event -> event;
        UnaryOperator<Event<Ensemble>> validYearFunction = event -> event;

        // Create the value function
        if ( Objects.nonNull( valueTransformer ) )
        {
            valueFunction = toTransform -> {

                Ensemble ensemble = toTransform.getValue();
                double[] members = ensemble.getMembers();
                double[] transformed = Arrays.stream( members )
                                             .map( valueTransformer )
                                             .toArray();

                Labels labels = ensemble.getLabels();

                return Event.of( toTransform.getTime(), Ensemble.of( transformed, labels ) );
            };
        }

        // Create the valid year function
        if ( Objects.nonNull( removeMemberByValidYear ) )
        {
            validYearFunction = event -> TimeSeriesSlicer.filter( event, removeMemberByValidYear );
        }

        // Compose them (this should be way easier, but compose and andThen are defined in the Function interface!)
        final UnaryOperator<Event<Ensemble>> outer = valueFunction;
        final UnaryOperator<Event<Ensemble>> inner = validYearFunction;

        return event -> outer.apply( inner.apply( event ) );
    }

    /**
     * Returns a monthday on which a year begins when removing an ensemble member whose label matches the valid year.
     * Returns {@code null} if no member should be removed.
     * 
     * @param rightOrBaseline the right or baseline data source configuration
     * @return the monthday on which a year begins when removing an ensemble member whose label matches the valid year 
     *            or null if no filtering is required
     */

    private static MonthDay getRemoveMemberByValidYear( DataSourceConfig rightOrBaseline )
    {
        // Return null to avoid iterating a no-op function
        if ( Objects.isNull( rightOrBaseline ) || Objects.isNull( rightOrBaseline.getRemoveMemberByValidYear() ) )
        {
            return null;
        }

        RemoveMemberByValidYear remove = rightOrBaseline.getRemoveMemberByValidYear();

        return MonthDay.of( remove.getEarliestMonth(), remove.getEarliestDay() );
    }

    /**
     * Creates a feature-specific baseline generator, if required.
     * 
     * @param baselineConfig the baseline declaration
     * @param retrieverFactory the factory to acquire a data source for a generated baseline
     * @param upscaler an upscaler, which is optional unless the generated series requires upscaling
     * @param baselineMeta the baseline metadata to assist with logging
     * @param admissibleValue a guard for admissible values of the generated baseline
     * @return a function that takes a set of features and returns a unary operator that generates a baseline
     */

    private static <L, R> Function<Set<FeatureKey>, UnaryOperator<TimeSeries<R>>>
            getGeneratedBaseline( DataSourceConfig baselineConfig,
                                  RetrieverFactory<L, R> retrieverFactory,
                                  TimeSeriesUpscaler<R> upscaler,
                                  Predicate<R> admissibleValue )
    {
        Objects.requireNonNull( baselineConfig );
        Objects.requireNonNull( retrieverFactory );

        // Persistence is supported
        if ( baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE )
        {
            LOGGER.trace( "Creating a persistence generator for data source {}.", baselineConfig );

            // Map from the input data type to the required type
            return features -> {
                Supplier<Stream<TimeSeries<R>>> persistenceSource =
                        () -> retrieverFactory.getBaselineRetriever( features ).get();

                // Order 1 by default. If others are supported later, add these                              
                return PersistenceGenerator.of( persistenceSource,
                                                upscaler,
                                                admissibleValue );
            };
        }
        // Other types are not supported
        else
        {
            throw new UnsupportedOperationException( "While attempting to generate a baseline: unrecognized "
                                                     + "type of baseline to generate, '"
                                                     + baselineConfig.getTransformation()
                                                     + "'." );
        }
    }

    /**
     * Returns the type of time-based pairing to perform given the declared input type of the datasets in the project.
     * 
     * @param inputsConfig the inputs declaration
     * @return the type of time-based pairing to perform
     */

    private static TimePairingType getTimePairingTypeFromInputsConfig( Inputs inputsConfig )
    {
        Objects.requireNonNull( inputsConfig );

        TimePairingType returnMe = TimePairingType.REFERENCE_TIME_AND_VALID_TIME;

        if ( !ConfigHelper.isForecast( inputsConfig.getLeft() ) || !ConfigHelper.isForecast( inputsConfig.getRight() ) )
        {
            returnMe = TimePairingType.VALID_TIME_ONLY;
        }

        return returnMe;
    }

    /**
     * Checks the project declaration and throws an exception when the declared type is inconsistent with the type of
     * pools requested. 
     * 
     * @param inputsConfig the input declaration
     * @param ensemble is true if ensemble pools were requested, false for single-valued pools
     * @throws IllegalArgumentException if the requested pools are inconsistent with the declaration
     */

    private static void validateRequestedPoolsAgainstDeclaration( Inputs inputsConfig, boolean ensemble )
    {
        DataSourceConfig rightConfig = inputsConfig.getRight();
        DataSourceConfig baselineConfig = inputsConfig.getBaseline();
        DatasourceType rightType = rightConfig.getType();

        // Right
        if ( ensemble && rightType != DatasourceType.ENSEMBLE_FORECASTS )
        {
            throw new IllegalArgumentException( "Requested pools for ensemble data, but the right "
                                                + DATA_IS_DECLARED_AS
                                                + rightType
                                                + WHICH_IS_NOT_ALLOWED );
        }
        else if ( !ensemble && rightType == DatasourceType.ENSEMBLE_FORECASTS )
        {
            throw new IllegalArgumentException( "Requested pools for single-valued data, but the right "
                                                + DATA_IS_DECLARED_AS
                                                + rightType
                                                + WHICH_IS_NOT_ALLOWED );
        }

        // Baseline?
        if ( Objects.nonNull( baselineConfig ) )
        {
            DatasourceType baselineType = baselineConfig.getType();

            if ( ensemble && baselineType != DatasourceType.ENSEMBLE_FORECASTS )
            {
                throw new IllegalArgumentException( "Requested pools for ensemble data, but the baseline "
                                                    + DATA_IS_DECLARED_AS
                                                    + baselineType
                                                    + WHICH_IS_NOT_ALLOWED );
            }
            else if ( !ensemble && baselineType == DatasourceType.ENSEMBLE_FORECASTS )
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
     * @param requests the requests to optimize
     * @param poolParameters the pool parameters
     * @return the optimized requests with a flag indicating whether optimization was performed
     */

    private static Map<DecomposableFeatureGroup, List<PoolRequest>>
            getFeatureBatchedSingletons( Map<FeatureGroup, List<PoolRequest>> requests,
                                         PoolParameters poolParameters )
    {
        Map<DecomposableFeatureGroup, List<PoolRequest>> returnMe = new HashMap<>();

        Set<FeatureGroup> nextGroups = new HashSet<>();
        List<PoolRequest> nextPoolRequests = new ArrayList<>();

        // Are the pool requests suitable for feature batching?
        boolean shouldBatch = PoolFactory.isFeatureBatchingAllowed( requests, poolParameters );

        // Group size for retrieval: either the batched retrieval size or the total number of features, if less
        int groupSize = requests.size() < poolParameters.getFeatureBatchSize() ? requests.size()
                                                                               : poolParameters.getFeatureBatchSize();

        // Loop the pool requests and gather them into groups for feature-batched retrieval, if the conditions are met
        for ( Map.Entry<FeatureGroup, List<PoolRequest>> nextEntry : requests.entrySet() )
        {
            FeatureGroup nextGroup = nextEntry.getKey();
            List<PoolRequest> nextPools = nextEntry.getValue();

            // More features than the minimum, batching is allowed and this is a singleton group, so batch it
            if ( shouldBatch && nextGroup.isSingleton() )
            {
                if ( nextPoolRequests.isEmpty() )
                {
                    nextPoolRequests.addAll( nextPools );
                }

                nextGroups.add( nextGroup );

                // Either reached the batch size or there are fewer features in total than the batch size
                if ( nextGroups.size() == groupSize )
                {
                    LOGGER.debug( "Created a batch of {} singleton feature groups for efficient retrieval. The "
                                  + "singleton groups to be treated as a batch are: {}.",
                                  groupSize,
                                  nextGroups );

                    DecomposableFeatureGroup group = new DecomposableFeatureGroup( nextGroups, true );
                    FeatureGroup nextComposedGroup = group.getComposedGroup();
                    List<PoolRequest> nextAdjustedPools = PoolFactory.setFeatureGroup( nextComposedGroup,
                                                                                       nextPoolRequests );
                    returnMe.put( group, nextAdjustedPools );

                    // Clear the group
                    nextGroups.clear();
                    nextPoolRequests.clear();
                }
            }
            // Proceed without feature-batching
            else
            {
                LOGGER.debug( "Not performing feature batched retrieval for feature group {}.", nextGroup );

                DecomposableFeatureGroup group = new DecomposableFeatureGroup( Set.of( nextGroup ), false );
                returnMe.put( group, nextPools );
            }
        }

        // Is there a left over feature batch with fewer than PoolFactory.FEATURE_BATCHED_RETRIEVAL_SIZE features?
        if ( !nextGroups.isEmpty() )
        {
            DecomposableFeatureGroup group = new DecomposableFeatureGroup( nextGroups, true );
            FeatureGroup nextComposedGroup = group.getComposedGroup();
            List<PoolRequest> nextAdjustedPools = PoolFactory.setFeatureGroup( nextComposedGroup, nextPoolRequests );
            returnMe.put( group, nextAdjustedPools );
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

    private static boolean isFeatureBatchingAllowed( Map<FeatureGroup, List<PoolRequest>> requests,
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
                              && PoolFactory.arePoolsSuitableForFeatureBatching( requests );

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
    private static boolean arePoolsSuitableForFeatureBatching( Map<FeatureGroup, List<PoolRequest>> requests )
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

    private static List<PoolRequest> setFeatureGroup( FeatureGroup featureGroup, List<PoolRequest> poolRequests )
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
     * @param singletons the singletons to use when decomposing the pool
     * @param toDecompose the pools suppliers to decompose
     * @return the decomposed pool suppliers, one for every pool and feature in the input
     */

    private static <L, R> List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> decompose( Set<FeatureGroup> singletons,
                                                                                  List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> toDecompose )
    {
        // Organize the suppliers by feature group
        Map<FeatureGroup, List<Supplier<Pool<TimeSeries<Pair<L, R>>>>>> returnMe = new HashMap<>();
        singletons.forEach( nextFeature -> returnMe.put( nextFeature, new ArrayList<>() ) );

        for ( Supplier<Pool<TimeSeries<Pair<L, R>>>> nextSupplier : toDecompose )
        {
            // Cache the result from the outer supplier, which contains the pairs for all features
            Supplier<Pool<TimeSeries<Pair<L, R>>>> nextSupplierCached = CachingSupplier.of( nextSupplier );

            // Create a supplier that extracts the required feature tuple
            for ( FeatureGroup nextGroup : singletons )
            {
                if ( !nextGroup.isSingleton() )
                {
                    throw new IllegalStateException( "Expected a singleton feature group, but found: " + nextGroup
                                                     + "." );
                }

                // The single feature tuple in the group
                FeatureTuple nextTuple = nextGroup.getFeatures()
                                                  .iterator()
                                                  .next();

                Supplier<Pool<TimeSeries<Pair<L, R>>>> nextInnerSupplier = () -> {
                    Pool<TimeSeries<Pair<L, R>>> composed = nextSupplierCached.get();

                    // Decompose by feature tuple
                    Map<FeatureTuple, Pool<TimeSeries<Pair<L, R>>>> decomposed =
                            PoolSlicer.decompose( PoolSlicer.getFeatureMapper(), composed );

                    // The expected feature tuple should be contained within the map
                    if ( !decomposed.containsKey( nextTuple ) )
                    {
                        LOGGER.debug( "While decomposing the pools for feature group {}, found no pools associated "
                                      + "with feature tuple {}.",
                                      nextGroup,
                                      nextTuple );

                        // Empty pool
                        return Pool.of( List.of(), PoolMetadata.of() );
                    }

                    Pool<TimeSeries<Pair<L, R>>> pool = decomposed.get( nextTuple );
                    PoolMetadata meta = PoolMetadata.of( pool.getMetadata(), nextGroup );

                    if ( pool.hasBaseline() )
                    {
                        Pool<TimeSeries<Pair<L, R>>> baselinePool = pool.getBaselineData();
                        PoolMetadata baselineMeta = PoolMetadata.of( baselinePool.getMetadata(), nextGroup );

                        return Pool.of( pool.get(), meta, baselinePool.get(), baselineMeta, pool.getClimatology() );
                    }

                    return Pool.of( pool.get(), meta, null, null, pool.getClimatology() );
                };

                List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> nextList = returnMe.get( nextGroup );
                nextList.add( nextInnerSupplier );
            }
        }

        // Return the flat list of suppliers in feature-group order
        return returnMe.values()
                       .stream()
                       .flatMap( List::stream )
                       .collect( Collectors.toUnmodifiableList() );
    }

    /**
     * Implementation of a {@link RetrieverFactory} that delegates all calls to a factory supplied on construction, 
     * but wraps calls to the climatological data in a {@link CachingRetriever} before returning it.
     *  
     * @param <L> the left data type
     * @param <R> the right data type
     */

    private static class ClimatologyCachedRetrieverFactory<L, R> implements RetrieverFactory<L, R>
    {

        /** The factory to delegate to for implementations. */
        private final RetrieverFactory<L, R> delegate;

        @Override
        public Supplier<Stream<TimeSeries<L>>> getClimatologyRetriever( Set<FeatureKey> features )
        {
            // Cache the delegated call
            Supplier<Stream<TimeSeries<L>>> delegated = this.delegate.getLeftRetriever( features );
            return CachingRetriever.of( delegated );
        }

        @Override
        public Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<FeatureKey> features )
        {
            return this.delegate.getLeftRetriever( features );
        }

        @Override
        public Supplier<Stream<TimeSeries<R>>> getBaselineRetriever( Set<FeatureKey> features )
        {
            return this.delegate.getBaselineRetriever( features );
        }

        @Override
        public Supplier<Stream<TimeSeries<L>>> getLeftRetriever( Set<FeatureKey> features, TimeWindowOuter timeWindow )
        {
            return this.delegate.getLeftRetriever( features, timeWindow );
        }

        @Override
        public Supplier<Stream<TimeSeries<R>>> getRightRetriever( Set<FeatureKey> features, TimeWindowOuter timeWindow )
        {
            return this.delegate.getRightRetriever( features, timeWindow );
        }

        @Override
        public Supplier<Stream<TimeSeries<R>>> getBaselineRetriever( Set<FeatureKey> features,
                                                                     TimeWindowOuter timeWindow )
        {
            return this.delegate.getBaselineRetriever( features, timeWindow );
        }

        /**
         * @param delegate the factory to delegate to for implementations
         */
        private ClimatologyCachedRetrieverFactory( RetrieverFactory<L, R> delegate )
        {
            this.delegate = delegate;
        }
    }

    /**
     * A collection of singleton feature groups for which feature-batched retrieval may be performed. If conducting
     * feature-batched retrieval, then the pools associated with these groups must be decomposed after they are 
     * constructed, because they will contain the pairs for all singleton features.
     */

    private static class DecomposableFeatureGroup implements Comparable<DecomposableFeatureGroup>
    {
        /** The singleton feature groups. */
        private final Set<FeatureGroup> singletons;

        /** Is true if the feature groups should be decomposed because feature-batched retrieval was performed. */
        private final boolean isComposed;

        /** The composed group. */
        private final FeatureGroup composed;

        @Override
        public boolean equals( Object o )
        {
            if ( ! ( o instanceof DecomposableFeatureGroup ) )
            {
                return false;
            }

            if ( o == this )
            {
                return true;
            }

            DecomposableFeatureGroup in = (DecomposableFeatureGroup) o;

            // No need to check composed group as that is derived from the singleton state
            return Objects.equals( this.isComposed, in.isComposed )
                   && Objects.equals( this.singletons, in.singletons );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.singletons, this.isComposed );
        }

        @Override
        public int compareTo( DecomposableFeatureGroup o )
        {
            Objects.requireNonNull( o );

            int compare = Boolean.compare( this.isComposed, o.isComposed );
            if ( compare != 0 )
            {
                return compare;
            }

            compare = Integer.compare( this.singletons.size(), o.singletons.size() );
            if ( compare != 0 )
            {
                return compare;
            }

            Iterator<FeatureGroup> in = o.singletons.iterator();
            for ( FeatureGroup singleton : this.singletons )
            {
                compare = singleton.compareTo( in.next() );

                if ( compare != 0 )
                {
                    return compare;
                }
            }

            return 0;
        }

        @Override
        public String toString()
        {
            return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                                .append( "singletons", this.singletons )
                                                                                .append( "isComposed", this.isComposed )
                                                                                .toString();
        }

        /**
         * @return whether the feature groups were batched for retrieval and should be decomposed
         */

        private boolean isComposed()
        {
            return this.isComposed;
        }

        /**
         * @return the singleton feature groups
         */

        private Set<FeatureGroup> getGroups()
        {
            return this.singletons;
        }

        /**
         * @return the composed feature group, containing all the singleton features.
         */

        private FeatureGroup getComposedGroup()
        {
            return this.composed;
        }

        /**
         * Create an instance.
         * @param singletons the singleton groups
         * @param isComposed is true if the features are composed for feature-batched retrieval
         * @throws NullPointerException if the set of singletons is null
         */
        private DecomposableFeatureGroup( Set<FeatureGroup> singletons, boolean isComposed )
        {
            Objects.requireNonNull( singletons );

            this.singletons = new HashSet<>( singletons );
            this.isComposed = isComposed;
            Set<FeatureTuple> features = this.singletons.stream()
                                                        .flatMap( next -> next.getFeatures()
                                                                              .stream() )
                                                        .collect( Collectors.toSet() );

            this.composed = FeatureGroup.of( features );
        }
    }

    /**
     * Do not construct.
     */

    private PoolFactory()
    {
    }

}
