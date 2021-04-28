package wres.io.pooling;

import java.time.MonthDay;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
import wres.datamodel.FeatureTuple;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
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
import wres.events.Evaluation;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.EnsembleRetrieverFactory;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.SingleValuedRetrieverFactory;
import wres.io.retrieval.UnitMapper;

/**
 * A factory class for generating the pools of pairs associated with an evaluation.
 * 
 * @author james.brown@hydrosolved.com
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
     * Create pools for single-valued data from a prescribed {@link Project} and {@link Feature}.
     * 
     * @param evaluation the evaluation description
     * @param project the project for which pools are required
     * @param feature the feature for which pools are required
     * @param unitMapper the mapper to convert measurement units
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public static List<Supplier<Pool<Pair<Double, Double>>>> getSingleValuedPools( Evaluation evaluation,
                                                                                   Project project,
                                                                                   FeatureTuple feature,
                                                                                   UnitMapper unitMapper )
    {
        Objects.requireNonNull( evaluation, "Cannot create pools from a null evaluation." );
        Objects.requireNonNull( project, "Cannot create pools from a null project." );
        Objects.requireNonNull( unitMapper, "Cannot create pools without a measurement unit mapper." );
        Objects.requireNonNull( unitMapper, "Cannot create pools without a feature description." );

        // Validate the project declaration for the required data type
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        DataSourceConfig baselineConfig = inputsConfig.getBaseline();
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, false );

        int projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature '{}'.",
                      projectId,
                      feature );

        // Get the times scale
        TimeScaleOuter desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

        // Create a feature-shaped retriever factory to support retrieval for this project
        RetrieverFactory<Double, Double> retrieverFactory = SingleValuedRetrieverFactory.of( project,
                                                                                             feature,
                                                                                             unitMapper );

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

        // Create the basic metadata for the pools
        PoolMetadata mainMetadata = PoolFactory.createMetadata( evaluation,
                                                                feature,
                                                                desiredTimeScale,
                                                                LeftOrRightOrBaseline.RIGHT );

        // Create the basic metadata for the baseline pools, if any
        // Also, create a baseline generator function (e.g., persistence), if required
        PoolMetadata baselineMetadata = null;
        UnaryOperator<TimeSeries<Double>> baselineGenerator = null;
        if ( project.hasBaseline() )
        {
            LOGGER.debug( "While genenerating pools for project '{}' and feature '{}', discovered a baseline data "
                          + "source.",
                          projectId,
                          feature );

            baselineMetadata = PoolFactory.createMetadata( evaluation,
                                                           feature,
                                                           desiredTimeScale,
                                                           LeftOrRightOrBaseline.BASELINE );

            // Generated baseline declared?
            if ( ConfigHelper.hasGeneratedBaseline( baselineConfig ) )
            {
                baselineGenerator = PoolFactory.getGeneratedBaseline( baselineConfig,
                                                                      retrieverFactory.getBaselineRetriever(),
                                                                      Function.identity(),
                                                                      upscaler,
                                                                      baselineMetadata,
                                                                      Double::isFinite );
            }
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
                                                           .setBasicMetadata( mainMetadata )
                                                           .setBasicMetadataForBaseline( baselineMetadata )
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
     * @param feature the feature for which pools are required
     * @param unitMapper the mapper to convert measurement units
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public static List<Supplier<Pool<Pair<Double, Ensemble>>>> getEnsemblePools( Evaluation evaluation,
                                                                                 Project project,
                                                                                 FeatureTuple feature,
                                                                                 UnitMapper unitMapper )
    {
        Objects.requireNonNull( evaluation, "Cannot create pools from a null evaluation." );
        Objects.requireNonNull( project, "Cannot create pools from a null project." );
        Objects.requireNonNull( unitMapper, "Cannot create pools without a measurement unit mapper." );
        Objects.requireNonNull( unitMapper, "Cannot create pools without a feature description." );

        // Validate the project declaration for the required data type
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, true );

        int projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature '{}'.",
                      projectId,
                      feature );

        // Get the times scale
        TimeScaleOuter desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

        // Create a feature-shaped retriever factory to support retrieval for this project
        RetrieverFactory<Double, Ensemble> retrieverFactory = EnsembleRetrieverFactory.of( project,
                                                                                           feature,
                                                                                           unitMapper );

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

        // Create the basic metadata for the pools
        PoolMetadata mainMetadata = PoolFactory.createMetadata( evaluation,
                                                                feature,
                                                                desiredTimeScale,
                                                                LeftOrRightOrBaseline.RIGHT );

        // Create the basic metadata for the baseline pools, if any
        PoolMetadata baselineMetadata = null;
        if ( project.hasBaseline() )
        {
            LOGGER.debug( "While genenerating pools for project '{}' and feature '{}', discovered a baseline data "
                          + "source.",
                          projectId,
                          feature );

            baselineMetadata = PoolFactory.createMetadata( evaluation,
                                                           feature,
                                                           desiredTimeScale,
                                                           LeftOrRightOrBaseline.BASELINE );
        }

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
                                                             .setBasicMetadata( mainMetadata )
                                                             .setBasicMetadataForBaseline( baselineMetadata )
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
     * @param featureTuple the feature
     * @param desiredTimeScale the desired time scale
     * @param leftOrRightOrBaseline the context for the data as it relates to the declaration
     * @return the metadata
     */

    private static PoolMetadata createMetadata( Evaluation evaluation,
                                                FeatureTuple featureTuple,
                                                TimeScaleOuter desiredTimeScale,
                                                LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        wres.statistics.generated.Pool pool = MessageFactory.parse( featureTuple,
                                                                    TimeWindowOuter.of(), // Default to start with
                                                                    desiredTimeScale,
                                                                    null,
                                                                    leftOrRightOrBaseline == LeftOrRightOrBaseline.BASELINE );

        return PoolMetadata.of( evaluation.getEvaluationDescription(), pool );
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
     * Creates a baseline generator, if required. Supported baselines are built from left-ish data and consume and 
     * produce right-ish data.
     * 
     * @param baselineConfig the baseline declaration
     * @param source the data source for the generated baseline
     * @param mapper a mapper to map from left-ish data to right-ish data for baselines that consume and produce the
     *            same types of data (e.g., persistence). Not required otherwise.
     * @param upscaler an upscaler, which is optional unless the generated series requires upscaling
     * @param baselineMeta the baseline metadata to assist with logging
     * @param admissibleValue a guard for admissible values of the generated baseline
     */

    private static <L, R> UnaryOperator<TimeSeries<R>> getGeneratedBaseline( DataSourceConfig baselineConfig,
                                                                             Supplier<Stream<TimeSeries<L>>> source,
                                                                             Function<L, R> mapper,
                                                                             TimeSeriesUpscaler<R> upscaler,
                                                                             PoolMetadata baselineMeta,
                                                                             Predicate<R> admissibleValue )
    {
        Objects.requireNonNull( baselineConfig );
        Objects.requireNonNull( source );
        Objects.requireNonNull( baselineMeta );

        // Persistence is supported
        if ( baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE )
        {
            Objects.requireNonNull( mapper );

            LOGGER.trace( "Creating a persistence generator for pool {}.", baselineMeta );

            // Map from the input data type to the required type
            Function<TimeSeries<L>, TimeSeries<R>> map = next -> TimeSeriesSlicer.transform( next, mapper );
            Supplier<Stream<TimeSeries<R>>> persistenceSource = () -> source.get().map( map );

            // Order 1 by default. If others are supported later, add these                              
            return PersistenceGenerator.of( persistenceSource,
                                            upscaler,
                                            admissibleValue );


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
