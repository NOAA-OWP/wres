package wres.io.pooling;

import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.CrossPair;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.Feature;
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
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
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
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.UnitMapper;
import wres.statistics.generated.Evaluation;

/**
 * A factory class for generating the pools of pairs associated with an evaluation.
 * 
 * @author James Brown
 */

public class PoolFactory
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PoolFactory.class );


    /**
     * Part of a message string about data declaration that is re-used.
     */

    private static final String DATA_IS_DECLARED_AS = "data is declared as '";

    /**
     * Part of a message string about disallowed declaration that is re-used.
     */

    private static final String WHICH_IS_NOT_ALLOWED = "', which is not allowed.";

    /**
     * Create pools for single-valued data from a prescribed {@link Project} and {@link FeatureGroup}.
     * 
     * @param evaluation the evaluation description
     * @param project the project for which pools are required
     * @param featureGroup the feature group for which pools are required
     * @param unitMapper the mapper to convert measurement units
     * @param retrieverFactory the retriever factory
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public static List<Supplier<Pool<Pair<Double, Double>>>> getSingleValuedPools( Evaluation evaluation,
                                                                                   Project project,
                                                                                   FeatureGroup featureGroup,
                                                                                   UnitMapper unitMapper,
                                                                                   RetrieverFactory<Double, Double> retrieverFactory )
    {
        Objects.requireNonNull( evaluation, "Cannot create pools from a null evaluation." );
        Objects.requireNonNull( project, "Cannot create pools from a null project." );
        Objects.requireNonNull( unitMapper, "Cannot create pools without a measurement unit mapper." );
        Objects.requireNonNull( featureGroup, "Cannot create pools without a feature group description." );
        Objects.requireNonNull( retrieverFactory, "Cannot create pools without a retriever factory." );

        // Validate the project declaration for the required data type
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        DataSourceConfig baselineConfig = inputsConfig.getBaseline();
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, false );

        long projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature group '{}'.",
                      projectId,
                      featureGroup );

        // Get the times scale
        TimeScaleOuter desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

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
        if ( ConfigHelper.hasGeneratedBaseline( baselineConfig ) )
        {
            LOGGER.debug( "While creating pools for project '{}' and feature group '{}', discovered a baseline "
                          + "to generate from a data source.",
                          projectId,
                          featureGroup );

            PoolMetadata baselineMetadata = PoolFactory.createMetadata( evaluation,
                                                                        featureGroup,
                                                                        TimeWindowOuter.of(),
                                                                        desiredTimeScale,
                                                                        LeftOrRightOrBaseline.BASELINE );

            baselineGenerator = PoolFactory.getGeneratedBaseline( baselineConfig,
                                                                  retrieverFactory,
                                                                  upscaler,
                                                                  baselineMetadata,
                                                                  Double::isFinite );
        }

        // Create any required transformers for value constraints
        // Left transformer is a straightforward value transformer
        DoubleUnaryOperator leftTransformer = PoolFactory.getSingleValuedTransformer( pairConfig.getValues() );
        // Right transformer may consider the encapsulating event
        UnaryOperator<Event<Double>> rightTransformer =
                next -> Event.of( next.getTime(), leftTransformer.applyAsDouble( next.getValue() ) );

        // Create the pool requests
        List<PoolRequest> poolRequests = PoolFactory.getPoolRequests( evaluation,
                                                                      projectConfig,
                                                                      Set.of( featureGroup ) );

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
     * Create pools for ensemble data from a prescribed {@link Project} and {@link Feature}.
     * 
     * @param evaluation the evaluation description
     * @param project the project for which pools are required
     * @param featureGroup the feature group for which pools are required
     * @param unitMapper the mapper to convert measurement units
     * @param retrieverFactory the retriever factory
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public static List<Supplier<Pool<Pair<Double, Ensemble>>>> getEnsemblePools( Evaluation evaluation,
                                                                                 Project project,
                                                                                 FeatureGroup featureGroup,
                                                                                 UnitMapper unitMapper,
                                                                                 RetrieverFactory<Double, Ensemble> retrieverFactory )
    {
        Objects.requireNonNull( evaluation, "Cannot create pools from a null evaluation." );
        Objects.requireNonNull( project, "Cannot create pools from a null project." );
        Objects.requireNonNull( unitMapper, "Cannot create pools without a measurement unit mapper." );
        Objects.requireNonNull( featureGroup, "Cannot create pools without a feature group description." );
        Objects.requireNonNull( retrieverFactory, "Cannot create pools without a retriever factory." );

        // Validate the project declaration for the required data type
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, true );

        long projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature group '{}'.",
                      projectId,
                      featureGroup );

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

        // Create the pool requests
        List<PoolRequest> poolRequests = PoolFactory.getPoolRequests( evaluation,
                                                                      projectConfig,
                                                                      Set.of( featureGroup ) );

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
     * Generates the {@link PoolRequest} associated with a project in order to drive pool creation.
     * 
     * @param evaluation the evaluation description
     * @param projectConfig the project declaration
     * @param featureGroups the feature groups
     * @return the pool requests
     * @throws NullPointerException if the input is null
     * @throws PoolCreationException if the pool could not be created for any other reason
     */

    public static List<PoolRequest> getPoolRequests( Evaluation evaluation,
                                                     ProjectConfig projectConfig,
                                                     Set<FeatureGroup> featureGroups )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( featureGroups );

        PairConfig pairConfig = projectConfig.getPair();

        // Get the desired times scale
        TimeScaleOuter desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

        // Get the time windows and sort them
        Set<TimeWindowOuter> timeWindows =
                new TreeSet<>( TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig ) );

        List<PoolRequest> poolRequests = new ArrayList<>();

        // Iterate the features and time windows, creating metadata for each
        for ( FeatureGroup nextFeatures : featureGroups )
        {
            for ( TimeWindowOuter timeWindow : timeWindows )
            {
                PoolMetadata mainMetadata = PoolFactory.createMetadata( evaluation,
                                                                        nextFeatures,
                                                                        timeWindow,
                                                                        desiredTimeScale,
                                                                        LeftOrRightOrBaseline.RIGHT );

                // Create the basic metadata
                PoolMetadata baselineMetadata = null;
                if ( ConfigHelper.hasBaseline( projectConfig ) )
                {
                    baselineMetadata = PoolFactory.createMetadata( evaluation,
                                                                   nextFeatures,
                                                                   timeWindow,
                                                                   desiredTimeScale,
                                                                   LeftOrRightOrBaseline.BASELINE );
                }

                PoolRequest request = PoolRequest.of( mainMetadata, baselineMetadata );

                poolRequests.add( request );
            }
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
        wres.statistics.generated.Pool pool = MessageFactory.parse( featureGroup,
                                                                    timeWindow, // Default to start with
                                                                    desiredTimeScale,
                                                                    null,
                                                                    leftOrRightOrBaseline == LeftOrRightOrBaseline.BASELINE );

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
                                  PoolMetadata baselineMeta,
                                  Predicate<R> admissibleValue )
    {
        Objects.requireNonNull( baselineConfig );
        Objects.requireNonNull( retrieverFactory );
        Objects.requireNonNull( baselineMeta );

        // Persistence is supported
        if ( baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE )
        {
            LOGGER.trace( "Creating a persistence generator for pool {}.", baselineMeta );

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
     * Do not construct.
     */

    private PoolFactory()
    {
    }

}
