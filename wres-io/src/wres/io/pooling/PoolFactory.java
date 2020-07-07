package wres.io.pooling;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

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
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.Builder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesCrossPairer.MatchMode;
import wres.datamodel.time.TimeSeriesOfDoubleBasicUpscaler;
import wres.datamodel.time.TimeSeriesOfEnsembleUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairer.TimePairingType;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.generators.PersistenceGenerator;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.EnsembleRetrieverFactory;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.SingleValuedRetrieverFactory;
import wres.io.retrieval.UnitMapper;
import wres.io.utilities.Database;

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
     * @param database The database to use.
     * @param project the project for which pools are required
     * @param feature the feature for which pools are required
     * @param unitMapper the mapper to convert measurement units
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public static List<Supplier<PoolOfPairs<Double, Double>>> getSingleValuedPools( Database database,
                                                                                    Project project,
                                                                                    Feature feature,
                                                                                    UnitMapper unitMapper )
    {
        Objects.requireNonNull( project, "Cannot create pools from a null project." );

        // Validate the project declaration for the required data type
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        DataSourceConfig baselineConfig = inputsConfig.getBaseline();
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, false );

        String featureString = ConfigHelper.getFeatureDescription( feature );

        int projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature '{}'.",
                      projectId,
                      featureString );

        // Get a unit mapper for the declared measurement units
        String desiredMeasurementUnit = pairConfig.getUnit();

        // Get the times scale
        TimeScaleOuter desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

        // Create a feature-shaped retriever factory to support retrieval for this project
        RetrieverFactory<Double, Double> retrieverFactory = SingleValuedRetrieverFactory.of( database,
                                                                                             project,
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
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleBasicUpscaler.of();

        // Create the basic metadata for the pools
        String variableId = ConfigHelper.getVariableIdFromProjectConfig( inputsConfig, false );
        String scenarioId = inputsConfig.getRight().getLabel();
        SampleMetadata mainMetadata = PoolFactory.createMetadata( projectConfig,
                                                                  feature,
                                                                  variableId,
                                                                  scenarioId,
                                                                  desiredMeasurementUnit,
                                                                  desiredTimeScale,
                                                                  LeftOrRightOrBaseline.RIGHT );

        // Create the basic metadata for the baseline pools, if any
        // Also, create a baseline generator function (e.g., persistence), if required
        SampleMetadata baselineMetadata = null;
        UnaryOperator<TimeSeries<Double>> baselineGenerator = null;
        if ( project.hasBaseline() )
        {
            LOGGER.debug( "While genenerating pools for project '{}' and feature '{}', discovered a baseline data "
                          + "source.",
                          projectId,
                          featureString );

            String baselineVariableId = ConfigHelper.getVariableIdFromProjectConfig( inputsConfig, true );
            String baselineScenarioId = inputsConfig.getBaseline().getLabel();
            baselineMetadata = PoolFactory.createMetadata( projectConfig,
                                                           feature,
                                                           baselineVariableId,
                                                           baselineScenarioId,
                                                           desiredMeasurementUnit,
                                                           desiredTimeScale,
                                                           LeftOrRightOrBaseline.BASELINE );

            // Generated baseline declared?
            if ( ConfigHelper.hasGeneratedBaseline( baselineConfig ) )
            {
                baselineGenerator = PoolFactory.getGeneratedBaseline( baselineConfig,
                                                                      retrieverFactory.getLeftRetriever(),
                                                                      Function.identity(),
                                                                      upscaler,
                                                                      baselineMetadata,
                                                                      Double::isFinite );
            }
        }

        // Create any required transformers for value constraints
        DoubleUnaryOperator transformer = PoolFactory.getSingleValuedTransformer( pairConfig.getValues() );

        // Build and return the pool suppliers
        return new PoolsGenerator.Builder<Double, Double>().setProject( project )
                                                           .setRetrieverFactory( retrieverFactory )
                                                           .setBasicMetadata( mainMetadata )
                                                           .setBasicMetadataForBaseline( baselineMetadata )
                                                           .setBaselineGenerator( baselineGenerator )
                                                           .setLeftTransformer( transformer::applyAsDouble )
                                                           .setRightTransformer( transformer::applyAsDouble )
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
     * @param database The database to use.
     * @param project the project for which pools are required
     * @param feature the feature for which pools are required
     * @param unitMapper the mapper to convert measurement units
     * @return a list of suppliers that supply pools of pairs
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the project is not consistent with the generation of pools for single-valued
     *            data
     */

    public static List<Supplier<PoolOfPairs<Double, Ensemble>>> getEnsemblePools( Database database,
                                                                                  Project project,
                                                                                  Feature feature,
                                                                                  UnitMapper unitMapper )
    {
        Objects.requireNonNull( project, "Cannot create pools from a null project." );

        // Validate the project declaration for the required data type
        // TODO: do not rely on the declared type. Detect the type instead
        // See #57301
        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        PoolFactory.validateRequestedPoolsAgainstDeclaration( inputsConfig, true );

        String featureString = ConfigHelper.getFeatureDescription( feature );

        int projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature '{}'.",
                      projectId,
                      featureString );

        // Get a unit mapper for the declared measurement units
        String desiredMeasurementUnit = pairConfig.getUnit();

        // Get the times scale
        TimeScaleOuter desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

        // Create a feature-shaped retriever factory to support retrieval for this project
        RetrieverFactory<Double, Ensemble> retrieverFactory = EnsembleRetrieverFactory.of( database,
                                                                                           project,
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
        TimeSeriesUpscaler<Double> leftUpscaler = TimeSeriesOfDoubleBasicUpscaler.of();
        TimeSeriesUpscaler<Ensemble> rightUpscaler = TimeSeriesOfEnsembleUpscaler.of( leftUpscaler );

        // Create the basic metadata for the pools
        String variableId = ConfigHelper.getVariableIdFromProjectConfig( inputsConfig, false );
        String scenarioId = inputsConfig.getRight().getLabel();
        SampleMetadata mainMetadata = PoolFactory.createMetadata( projectConfig,
                                                                  feature,
                                                                  variableId,
                                                                  scenarioId,
                                                                  desiredMeasurementUnit,
                                                                  desiredTimeScale,
                                                                  LeftOrRightOrBaseline.RIGHT );

        // Create the basic metadata for the baseline pools, if any
        SampleMetadata baselineMetadata = null;
        if ( project.hasBaseline() )
        {
            LOGGER.debug( "While genenerating pools for project '{}' and feature '{}', discovered a baseline data "
                          + "source.",
                          projectId,
                          featureString );

            String baselineVariableId = ConfigHelper.getVariableIdFromProjectConfig( inputsConfig, true );
            String baselineScenarioId = inputsConfig.getBaseline().getLabel();
            baselineMetadata = PoolFactory.createMetadata( projectConfig,
                                                           feature,
                                                           baselineVariableId,
                                                           baselineScenarioId,
                                                           desiredMeasurementUnit,
                                                           desiredTimeScale,
                                                           LeftOrRightOrBaseline.BASELINE );
        }

        // Create any required transformers for value constraints
        DoubleUnaryOperator leftTransformer = PoolFactory.getSingleValuedTransformer( pairConfig.getValues() );
        UnaryOperator<Ensemble> rightTransformer = PoolFactory.getEnsembleTransformer( leftTransformer );

        // Build and return the pool suppliers
        return new PoolsGenerator.Builder<Double, Ensemble>().setProject( project )
                                                             .setRetrieverFactory( retrieverFactory )
                                                             .setBasicMetadata( mainMetadata )
                                                             .setBasicMetadataForBaseline( baselineMetadata )
                                                             .setLeftTransformer( leftTransformer::applyAsDouble )
                                                             .setRightTransformer( rightTransformer )
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
     * @param projectConfig the project declaration
     * @param feature the feature
     * @param variableId the variable identifier
     * @param scenarioId the scenario identifier
     * @param measurementUnitString the measurement units string
     * @param desiredTimeScale the desired time scale
     * @param leftOrRightOrBaseline the context for the data as it relates to the declaration
     * @return the metadata
     */

    private static SampleMetadata createMetadata( ProjectConfig projectConfig,
                                                  Feature feature,
                                                  String variableId,
                                                  String scenarioId,
                                                  String measurementUnitString,
                                                  TimeScaleOuter desiredTimeScale,
                                                  LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        Float longitude = null;
        Float latitude = null;

        if ( Objects.nonNull( feature.getCoordinate() ) )
        {
            longitude = feature.getCoordinate().getLongitude();
            latitude = feature.getCoordinate().getLatitude();
        }

        Location location = Location.of( feature.getComid(),
                                         feature.getLocationId(),
                                         longitude,
                                         latitude,
                                         feature.getGageId() );

        DatasetIdentifier identifier = DatasetIdentifier.of( location,
                                                             variableId,
                                                             scenarioId,
                                                             null,
                                                             leftOrRightOrBaseline );

        MeasurementUnit measurementUnit = MeasurementUnit.of( measurementUnitString );

        return new Builder().setIdentifier( identifier )
                                          .setProjectConfig( projectConfig )
                                          .setMeasurementUnit( measurementUnit )
                                          .setTimeScale( desiredTimeScale )
                                          .build();
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
            return DoubleUnaryOperator.identity();
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
     * Returns a transformer for ensemble data if required.
     * 
     * @param leftTransformer the left transformer to compose
     * @return a transformer or null
     */

    private static UnaryOperator<Ensemble> getEnsembleTransformer( DoubleUnaryOperator leftTransformer )
    {
        if ( Objects.isNull( leftTransformer ) )
        {
            return null;
        }

        return toTransform -> {

            double[] transformed = Arrays.stream( toTransform.getMembers() )
                                         .map( leftTransformer )
                                         .toArray();

            String[] labels = null;
            if ( toTransform.getLabels().isPresent() )
            {
                labels = toTransform.getLabels().get();
            }

            return Ensemble.of( transformed, labels );
        };
    }

    /**
     * Creates a baseline generator, if required. Supported baselines are built from left-ish data and consume and 
     * produce right-ish data.
     * 
     * @param baselineConfig the baseline declaration
     * @param builder the pool builder
     * @param source the data source for the generated baseline
     * @param a mapper to map from left-ish data to right-ish data for baselines that consume and produce the 
     *            same types of data (e.g., persistence). Not required otherwise.
     * @param upscaler an upscaler, which is optional unless the generated series requires upscaling
     * @param baselineMeta the baseline metadata to assist with logging
     * @param admissibleValue a guard for admissible values of the generated baseline
     */

    private static <L, R> UnaryOperator<TimeSeries<R>> getGeneratedBaseline( DataSourceConfig baselineConfig,
                                                                             Supplier<Stream<TimeSeries<L>>> source,
                                                                             Function<L, R> mapper,
                                                                             TimeSeriesUpscaler<R> upscaler,
                                                                             SampleMetadata baselineMeta,
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
