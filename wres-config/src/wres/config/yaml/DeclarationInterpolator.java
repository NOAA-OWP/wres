package wres.config.yaml;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.CovariateDatasetBuilder;
import wres.config.yaml.components.CovariatePurpose;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.VariableBuilder;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

/**
 * <p>Interpolates missing declaration from the other declaration present. The interpolation of missing declaration may
 * be performed in stages, depending on when it is required and how it is informed. For example, the
 * {@link #interpolate(EvaluationDeclaration, boolean)} performs minimal interpolation that is informed by the
 * declaration alone, such as the interpolation of geographic features when the missing information can be obtained
 * without a service call.
 *
 * <p>Currently, {@link #interpolate(EvaluationDeclaration, boolean)} does not perform any external service calls. For
 * example, features and thresholds may be declared implicitly using a feature service or a threshold service,
 * respectively. The resulting features and thresholds are not resolved into explicit descriptions of the same options.
 * It is assumed that another module (wres-io) resolves these attributes.
 *
 * @author James Brown
 */
public class DeclarationInterpolator
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DeclarationInterpolator.class );
    /** Re-used string. */
    private static final String THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA =
            "The data type inferred from the time-series data ";
    /** Re-used string. */
    private static final String WHICH_IS_INCONSISTENT_PLEASE_FIX_THE = "', which is inconsistent. Please fix the ";
    /** Re-used string. */
    private static final String DECLARATION_HINT_LOOK_FOR_NEARBY_WARNING =
            "declaration. Hint: look for nearby warning ";
    /** Re-used string. */
    private static final String BUT_THE_DATA_TYPE_INFERRED_FROM_THE = "', but the data type inferred from the ";
    /** Re-used string. */
    private static final String DECLARATION_WAS = "declaration was '";
    /** Re-used string. */
    private static final String MESSAGES_THAT_INDICATE_WHY_THE_DATA_TYPE = "messages that indicate why the data type ";
    /** Re-used string. */
    private static final String INFERRED_FROM_THE_DECLARATION_WAS = "inferred from the declaration was '";
    /** Re-used string. */
    private static final String TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA_WHICH =
            "type inferred from the time-series data, which ";
    /** Re-used string. */
    private static final String THE_EVALUATION_WILL_PROCEED_WITH_THE_DATA =
            "'. The evaluation will proceed with the data ";
    /** Re-used string. */
    private static final String IF_THIS_IS_INCORRECT_PLEASE_DECLARE_THE =
            "'. If this is incorrect, please declare the ";
    /** Re-used string. */
    private static final String INTENDED_TYPE_EXPLICITLY = "intended 'type' explicitly.";
    /** Re-used string. */
    private static final String DATASET_WAS = "' dataset was ";
    /** Re-used string. */
    private static final String FOR_THE_PREDICTED_DATASET_WAS = "for the 'predicted' dataset was '";
    /** Re-used string. */
    private static final String BUT_THE_DATA_TYPE_WAS_EXPLICITLY_DECLARED_AS =
            "', but the data 'type' was explicitly declared as '";
    /** Re-used string. */
    private static final String THE_EVALUATION_WILL_PROCEED_WITH_THE_EXPLICITLY =
            "'. The evaluation will proceed with the explicitly";
    /** Re-used string. */
    private static final String DECLARED_TYPE_OF = " declared 'type' of '";
    /** Re-used string. */
    private static final String IF_THIS_IS_INCORRECT_PLEASE_FIX_THE_DECLARED =
            "'. If this is incorrect, please fix the declared ";
    /** Re-used string. */
    private static final String TYPE = "'type'.";
    /** Re-used string. */
    public static final String INTERPOLATED_THE_COVARIATE_DATASET_TO_HAVE_A_PURPOSE_OF =
            "Interpolated the covariate dataset {} to have a purpose of {}.";

    /**
     * Performs pre-ingest interpolation of "missing" declaration from the available declaration. Currently, this
     * method does not interpolate any declaration that requires service calls, such as features that are resolved by a
     * feature service or thresholds that are resolved by a threshold service. This method can also be viewed as
     * completing or "densifying" the declaration, based on hints provided by a user. The pre-ingest interpolation
     * includes all missing declaration that is invariant to the ingested data.
     *
     * @see #interpolate(EvaluationDeclaration, DataTypes, VariableNames, String, TimeScale, boolean)
     * @param declaration the raw declaration to interpolate
     * @param notify whether to notify any warnings encountered or assumptions made during interpolation
     * @return the interpolated declaration
     * @throws DeclarationException when the interpolation fails for any reason
     */
    public static EvaluationDeclaration interpolate( EvaluationDeclaration declaration, boolean notify )
    {
        EvaluationDeclarationBuilder adjustedBuilder = EvaluationDeclarationBuilder.builder( declaration );

        // Interpolate any absolute paths from paths relative to the default data directory. The UriDeserializer
        // performs basic disambiguation only and does not interpolate absolute paths
        DeclarationInterpolator.interpolateUris( adjustedBuilder );
        // Interpolate the time zone offsets for individual sources when supplied for the overall dataset
        DeclarationInterpolator.interpolateTimeZoneOffsets( adjustedBuilder );
        // Interpolate the feature authorities
        DeclarationInterpolator.interpolateFeatureAuthorities( adjustedBuilder );
        // Interpolate geospatial features when required, but without using a feature service
        DeclarationInterpolator.interpolateFeaturesWithoutFeatureService( adjustedBuilder );
        // Interpolate the covariate feature name orientations
        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationInterpolator.interpolateCovariateFeatureOrientations( adjustedBuilder ) );
        // Interpolate any data types that are required to support reading/ingest, rather than informed by ingest.
        // Note that data type interpolation will also be attempted post-ingest, informed by the types detected during
        // ingest, but some reading/ingest (e.g., from web services that support several data types) requires the type
        // to be declared or interpolated upfront
        events.addAll( DeclarationInterpolator.interpolateDataTypesPreIngest( adjustedBuilder ) );
        DeclarationInterpolator.interpolateCovariatePurpose( adjustedBuilder );
        // Interpolate evaluation metrics when required
        DeclarationInterpolator.interpolateMetrics( adjustedBuilder );
        // Interpolate summary statistics when required
        DeclarationInterpolator.interpolateSummaryStatistics( adjustedBuilder );
        // Interpolate the evaluation-wide decimal format string for each numeric format type
        DeclarationInterpolator.interpolateDecimalFormatforNumericFormats( adjustedBuilder );
        // Interpolate the metrics to ignore for each graphics format
        DeclarationInterpolator.interpolateMetricsToOmitFromGraphicsFormats( adjustedBuilder );
        // Interpolate the graphics formats from the other declaration present
        DeclarationInterpolator.interpolateGraphicsFormats( adjustedBuilder );
        // Interpolate the measurement units for value thresholds when they have not been declared explicitly
        DeclarationInterpolator.interpolateMeasurementUnitForValueThresholds( adjustedBuilder );
        // Interpolate thresholds for individual metrics, adding an "all data" threshold as needed
        DeclarationInterpolator.interpolateThresholdsForIndividualMetrics( adjustedBuilder );
        // Interpolate metric parameters
        DeclarationInterpolator.interpolateMetricParameters( adjustedBuilder );
        // Interpolate output formats where none exist
        DeclarationInterpolator.interpolateOutputs( adjustedBuilder );
        // Interpolate time windows
        DeclarationInterpolator.interpolateTimeWindows( adjustedBuilder );

        // Handle any events encountered
        DeclarationInterpolator.handleEvents( events, notify, true );

        return adjustedBuilder.build();
    }

    /**
     * Performs post-ingest interpolation of declaration, which is aided by information read from the time-series data.
     * This includes interpolation of the data types and all declaration that depends on the data types, as well as the
     * variable names, measurement unit and evaluation timescale.
     *
     * @see #interpolate(EvaluationDeclaration, boolean)
     * @param declaration the raw declaration to interpolate
     * @param dataTypes types the analyzed data types
     * @param variableNames the analyzed variable names
     * @param measurementUnit the analyzed measurement unit
     * @param timeScale the analyzed timescale, possibly null
     * @param notify whether to notify any warnings encountered or assumptions made during interpolation
     * @return the interpolated declaration
     * @throws NullPointerException if any required input is null
     * @throws DeclarationException if the existing declaration is inconsistent with the post-ingest information
     */
    public static EvaluationDeclaration interpolate( EvaluationDeclaration declaration,
                                                     DataTypes dataTypes,
                                                     VariableNames variableNames,
                                                     String measurementUnit,
                                                     TimeScale timeScale,
                                                     boolean notify )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( dataTypes );
        Objects.requireNonNull( variableNames );
        Objects.requireNonNull( measurementUnit );

        EvaluationDeclarationBuilder adjustedBuilder = EvaluationDeclarationBuilder.builder( declaration );

        // Disambiguate the "type" of data when it is not declared
        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationInterpolator.interpolateDataTypes( adjustedBuilder,
                                                                               dataTypes ) );
        // Interpolate the variable names
        List<EvaluationStatusEvent> variableEvents =
                DeclarationInterpolator.interpolateVariableNames( adjustedBuilder,
                                                                  variableNames );
        events.addAll( variableEvents );

        // Interpolate the measurement unit
        List<EvaluationStatusEvent> unitEvents =
                DeclarationInterpolator.interpolateMeasurementUnit( adjustedBuilder,
                                                                    measurementUnit );
        events.addAll( unitEvents );

        // Interpolate the measurement units for value thresholds
        DeclarationInterpolator.interpolateMeasurementUnitForValueThresholds( adjustedBuilder );

        // Interpolate the timescale
        List<EvaluationStatusEvent> scaleEvents =
                DeclarationInterpolator.interpolateTimeScale( adjustedBuilder,
                                                              timeScale );
        events.addAll( scaleEvents );

        // Interpolate evaluation metrics when required
        DeclarationInterpolator.interpolateMetrics( adjustedBuilder );
        // Interpolate thresholds for individual metrics, adding an "all data" threshold as needed
        DeclarationInterpolator.interpolateThresholdsForIndividualMetrics( adjustedBuilder );
        // Interpolate metric parameters
        DeclarationInterpolator.interpolateMetricParameters( adjustedBuilder );

        // Handle any events encountered
        DeclarationInterpolator.handleEvents( events, notify, false );

        return adjustedBuilder.build();
    }

    /**
     * Returns any feature tuples with one or more missing features that require interpolation.
     * @param evaluation the evaluation declaration, not null
     * @return the sparse features, including singletons and grouped features
     */

    public static Set<GeometryTuple> getSparseFeaturesToInterpolate( EvaluationDeclaration evaluation )
    {
        if ( Objects.isNull( evaluation.features() ) )
        {
            LOGGER.debug( "No sparse features were detected because no features were declared." );
            return Set.of();
        }

        Set<GeometryTuple> features = evaluation.features()
                                                .geometries();
        Predicate<GeometryTuple> filter;

        // Determine the correct type of filter for the feature
        if ( DeclarationUtilities.hasBaseline( evaluation ) )
        {
            filter = feature -> !feature.hasLeft()
                                || !feature.hasRight()
                                || !feature.hasBaseline();
        }
        else
        {
            filter = feature -> !feature.hasLeft()
                                || !feature.hasRight();
        }

        // Find sparse singletons
        Set<GeometryTuple> sparseFeatures = new HashSet<>();
        if ( Objects.nonNull( evaluation.features() ) )
        {
            Set<GeometryTuple> sparseSingletons = features.stream()
                                                          .filter( filter )
                                                          .collect( Collectors.toSet() );
            sparseFeatures.addAll( sparseSingletons );
        }

        // Find sparse grouped features
        if ( Objects.nonNull( evaluation.featureGroups() ) )
        {
            Set<GeometryTuple> sparseGroupedFeatures = evaluation.featureGroups()
                                                                 .geometryGroups()
                                                                 .stream()
                                                                 .flatMap( nextGroup -> nextGroup.getGeometryTuplesList()
                                                                                                 .stream() )
                                                                 .filter( filter )
                                                                 .collect( Collectors.toSet() );
            sparseFeatures.addAll( sparseGroupedFeatures );
        }

        return Collections.unmodifiableSet( sparseFeatures );
    }

    /**
     * Associates the specified thresholds with the appropriate metrics and adds an "all data" threshold to each
     * continuous metric.
     *
     * @param globalThresholds the mapped thresholds
     * @param addAllData is true to add an all data threshold where appropriate, false otherwise
     * @param builder the builder to mutate
     * @param addThresholds is true to add the global and local thresholds together, otherwise favor the local thresholds
     */

    static void addThresholdsToMetrics( Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> globalThresholds,
                                        EvaluationDeclarationBuilder builder,
                                        boolean addAllData,
                                        boolean addThresholds )
    {
        Set<Metric> metrics = builder.metrics();
        Set<Metric> adjustedMetrics = new LinkedHashSet<>( metrics.size() );

        LOGGER.debug( "Discovered these metrics whose thresholds will be adjusted: {}.", metrics );

        // Adjust the metrics
        for ( Metric next : metrics )
        {
            Metric adjustedMetric = DeclarationInterpolator.addThresholdsToMetric( globalThresholds,
                                                                                   next,
                                                                                   builder,
                                                                                   addAllData,
                                                                                   addThresholds );
            adjustedMetrics.add( adjustedMetric );
        }

        builder.metrics( adjustedMetrics );
    }

    /**
     * <p>Interpolates the type of time-series data to evaluate when required. There are three distinct sources of
     * information about the data type of each dataset:
     * <ol>
     *     <li>1. The explicitly declared 'type', which may be missing;</li>
     *     <li>2. The type inferred from the other declaration present; and</li>
     *     <li>3. The type inferred by inspecting each time-series dataset, which is supplied for each dataset.</li>
     * </ol>
     * <p>When attempting to interpolate the data type, errors or warnings are emitted depending on any conflicts
     * between these different sources of information. However, errors and warnings are only uncovered for conflicts
     * between (1) and (3) and, separately, between (2) and (3) and not between (1) and (2) because the purpose of this
     * class is not to validate the declaration for internal consistency (see {@link DeclarationValidator}), rather to
     * check that a consistent interpolation is possible. This method is intended to for post-ingest interpolation only.
     *
     * @see #interpolateDataTypesPreIngest(EvaluationDeclarationBuilder)
     * @param builder the declaration builder to adjust
     * @param dataTypes the data types
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateDataTypes( EvaluationDeclarationBuilder builder,
                                                                     DataTypes dataTypes )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Resolve the left or observed data type, if required
        if ( Objects.nonNull( builder.left() ) )
        {
            List<EvaluationStatusEvent> leftTypeEvents =
                    DeclarationInterpolator.interpolateObservedDataType( builder, dataTypes.leftType() );
            events.addAll( leftTypeEvents );
        }

        // Resolve the predicted data type, if required
        if ( Objects.nonNull( builder.right() ) )
        {
            List<EvaluationStatusEvent> rightTypeEvents =
                    DeclarationInterpolator.interpolatePredictedDataType( builder, dataTypes.rightType() );
            events.addAll( rightTypeEvents );
        }

        // Baseline data type has the same as the predicted data type, by default
        List<EvaluationStatusEvent> baseTypeEvents =
                DeclarationInterpolator.interpolateBaselineDataType( builder, dataTypes.baselineType() );
        events.addAll( baseTypeEvents );

        // Data type for covariates
        List<EvaluationStatusEvent> covariateTypeEvents =
                DeclarationInterpolator.interpolateCovariatesDataType( builder, dataTypes.covariatesType() );
        events.addAll( covariateTypeEvents );

        return Collections.unmodifiableList( events );
    }

    /**
     * Performs a minimal interpolation of data types pre-ingest for those cases where the data type drives
     * reading/ingest. For example, some web services may serve multiple data types from the same endpoint, requiring
     * the type to be inferred from the context when not explicitly declared.
     *
     * @param builder the builder
     * @return any interpolation events encountered
     */

    private static List<EvaluationStatusEvent> interpolateDataTypesPreIngest( EvaluationDeclarationBuilder builder )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Resolve the left or observed data type, if required
        if ( Objects.nonNull( builder.left() )
             && DeclarationInterpolator.isSpecialCaseForDataTypeInterpolation( builder.left() ) )
        {
            Dataset dataset = builder.left();
            DatasetBuilder datasetBuilder = DatasetBuilder.builder( dataset );
            List<EvaluationStatusEvent> interpolationEvents =
                    DeclarationInterpolator.interpolateDataTypeForSpecialCase( builder,
                                                                               datasetBuilder,
                                                                               DatasetOrientation.LEFT,
                                                                               datasetBuilder.sources()
                                                                                             .stream()
                                                                                             .findFirst()
                                                                                             .orElse( SourceBuilder.builder()
                                                                                                                   .build() )
                                                                                             .sourceInterface() );
            builder.left( datasetBuilder.build() );
            events.addAll( interpolationEvents );
        }

        // Resolve the predicted data type, if required
        if ( Objects.nonNull( builder.right() )
             && DeclarationInterpolator.isSpecialCaseForDataTypeInterpolation( builder.right() ) )
        {
            Dataset dataset = builder.right();
            DatasetBuilder datasetBuilder = DatasetBuilder.builder( dataset );
            List<EvaluationStatusEvent> interpolationEvents =
                    DeclarationInterpolator.interpolateDataTypeForSpecialCase( builder,
                                                                               datasetBuilder,
                                                                               DatasetOrientation.RIGHT,
                                                                               datasetBuilder.sources()
                                                                                             .stream()
                                                                                             .findFirst()
                                                                                             .orElse( SourceBuilder.builder()
                                                                                                                   .build() )
                                                                                             .sourceInterface() );
            builder.right( datasetBuilder.build() );
            events.addAll( interpolationEvents );
        }

        // Resolve the baseline data type, if required
        if ( DeclarationUtilities.hasBaseline( builder )
             && DeclarationInterpolator.isSpecialCaseForDataTypeInterpolation( builder.right() ) )
        {
            BaselineDataset baseline = builder.baseline();
            Dataset dataset = baseline.dataset();
            DatasetBuilder datasetBuilder = DatasetBuilder.builder( dataset );
            List<EvaluationStatusEvent> interpolationEvents =
                    DeclarationInterpolator.interpolateDataTypeForSpecialCase( builder,
                                                                               datasetBuilder,
                                                                               DatasetOrientation.BASELINE,
                                                                               datasetBuilder.sources()
                                                                                             .stream()
                                                                                             .findFirst()
                                                                                             .orElse( SourceBuilder.builder()
                                                                                                                   .build() )
                                                                                             .sourceInterface() );
            BaselineDataset adjusted = BaselineDatasetBuilder.builder( baseline )
                                                             .dataset( dataset )
                                                             .build();
            builder.baseline( adjusted );
            events.addAll( interpolationEvents );
        }

        // Resolve the covariate data types, if required
        if ( !builder.covariates()
                     .isEmpty() )
        {
            List<CovariateDataset> adjustedCovariates = new ArrayList<>();
            for ( CovariateDataset covariateDataset : builder.covariates() )
            {
                Dataset covariate = covariateDataset.dataset();
                DatasetBuilder datasetBuilder = DatasetBuilder.builder( covariate );

                if ( DeclarationInterpolator.isSpecialCaseForDataTypeInterpolation( covariate ) )
                {
                    List<EvaluationStatusEvent> interpolationEvents =
                            DeclarationInterpolator.interpolateDataTypeForSpecialCase( builder,
                                                                                       datasetBuilder,
                                                                                       DatasetOrientation.COVARIATE,
                                                                                       datasetBuilder.sources()
                                                                                                     .stream()
                                                                                                     .findFirst()
                                                                                                     .orElse(
                                                                                                             SourceBuilder.builder()
                                                                                                                          .build() )
                                                                                                     .sourceInterface() );
                    events.addAll( interpolationEvents );
                }

                CovariateDatasetBuilder covariateBuilder = CovariateDatasetBuilder.builder( covariateDataset )
                                                                                  .dataset( datasetBuilder.build() );
                adjustedCovariates.add( covariateBuilder.build() );
            }

            // Set the adjusted covariates
            builder.covariates( adjustedCovariates );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Handles evaluation status events, logging warnings when requested and throwing an {@link DeclarationException}
     * when errors are encountered.
     * @param events the evaluation status events
     * @param notify whether to notify warnings
     * @param beforeIngest whether the interpolation is happening before or after time-series ingest
     */
    private static void handleEvents( List<EvaluationStatusEvent> events, boolean notify, boolean beforeIngest )
    {
        // Notify any warnings? Push to log for now, but see #61930 (logging isn't for users)
        if ( notify
             && LOGGER.isWarnEnabled() )
        {
            List<EvaluationStatus.EvaluationStatusEvent> warnEvents =
                    events.stream()
                          .filter( a -> a.getStatusLevel()
                                        == EvaluationStatus.EvaluationStatusEvent.StatusLevel.WARN )
                          .toList();
            if ( !warnEvents.isEmpty() )
            {
                StringJoiner message = new StringJoiner( System.lineSeparator() );
                String spacer = "    - ";
                warnEvents.forEach( e -> message.add( spacer + e.getEventMessage() ) );

                String context = "after";

                if ( beforeIngest )
                {
                    context = "before";
                }

                LOGGER.warn( "Encountered {} warnings when interpolating missing declaration {} time-series ingest: "
                             + "{}{}",
                             warnEvents.size(),
                             context,
                             System.lineSeparator(),
                             message );
            }
        }

        // Errors?
        List<EvaluationStatus.EvaluationStatusEvent> errorEvents =
                events.stream()
                      .filter( a -> a.getStatusLevel()
                                    == EvaluationStatus.EvaluationStatusEvent.StatusLevel.ERROR )
                      .toList();
        if ( !errorEvents.isEmpty() )
        {
            StringJoiner message = new StringJoiner( System.lineSeparator() );
            String spacer = "    - ";
            errorEvents.forEach( e -> message.add( spacer + e.getEventMessage() ) );

            throw new DeclarationException( "While attempting to reconcile the declared evaluation with the "
                                            + "time-series data read from sources, encountered "
                                            + errorEvents.size()
                                            + " error(s) that must be fixed:"
                                            + System.lineSeparator() +
                                            message );
        }
    }

    /**
     * Performs any interpolation of features that can be conducted without a feature service.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateFeaturesWithoutFeatureService( EvaluationDeclarationBuilder builder )
    {
        // Interpolate features from featureful thresholds when not already declared explicitly. Must do this before
        // interpolating sparse features because the thresholds only declare a single feature, i.e., they are all sparse
        DeclarationInterpolator.interpolateFeaturesFromThresholds( builder );

        // Interpolate sparse features
        DeclarationInterpolator.interpolateSparseFeatures( builder );
    }

    /**
     * Interpolates sparsely declared features where possible (without using a feature service). Interpolation is only
     * possible when the feature authorities match for each dataset orientation that requires an interpolated feature.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateSparseFeatures( EvaluationDeclarationBuilder builder )
    {
        // Add autogenerated features
        if ( Objects.isNull( builder.featureService() )
             && DeclarationInterpolator.hasMatchingFeatureAuthorities( builder ) )
        {
            LOGGER.debug( "Interpolating geospatial features from the raw declaration." );

            boolean hasBaseline = DeclarationUtilities.hasBaseline( builder );

            // Apply to feature context
            if ( Objects.nonNull( builder.features() ) )
            {
                Set<GeometryTuple> features = builder.features()
                                                     .geometries();
                Set<GeometryTuple> denseFeatures = DeclarationInterpolator.interpolateSparseFeatures( features,
                                                                                                      hasBaseline );
                Features adjustedFeatures = new Features( denseFeatures, builder.features()
                                                                                .offsets() );
                builder.features( adjustedFeatures );
            }

            // Apply to feature group context
            if ( Objects.nonNull( builder.featureGroups() ) )
            {
                Set<GeometryGroup> geoGroups = builder.featureGroups()
                                                      .geometryGroups();

                // Preserve insertion order
                Set<GeometryGroup> adjustedGeoGroups = new LinkedHashSet<>();

                for ( GeometryGroup nextGroup : geoGroups )
                {
                    List<GeometryTuple> features = nextGroup.getGeometryTuplesList();
                    Set<GeometryTuple> denseFeatures = DeclarationInterpolator.interpolateSparseFeatures( features,
                                                                                                          hasBaseline );

                    GeometryGroup nextAdjustedGroup =
                            nextGroup.toBuilder()
                                     .clearGeometryTuples()
                                     .addAllGeometryTuples( denseFeatures )
                                     .build();
                    adjustedGeoGroups.add( nextAdjustedGroup );
                }
                FeatureGroups adjustedFeatureGroups = FeatureGroupsBuilder.builder()
                                                                          .geometryGroups( adjustedGeoGroups )
                                                                          .build();
                builder.featureGroups( adjustedFeatureGroups );
            }
        }
    }

    /**
     * Interpolates features from featureful thresholds when not already declared explicitly. Since the thresholds only
     * declare a single feature, not a complete tuple, this interpolation only proceeds when the feature authority is
     * present and equivalent for all datasets or a feature service has been declared to resolve the correlations.
     * Furthermore, it only proceeds when there are no explicit features declared in any context. See #116232.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateFeaturesFromThresholds( EvaluationDeclarationBuilder builder )
    {
        // No explicit features?
        Set<GeometryTuple> features = DeclarationUtilities.getFeatures( builder.build() );
        if ( !features.isEmpty() )
        {
            LOGGER.debug( "Could not interpolate any features from thresholds because explicit features were declared "
                          + "in other contexts. Features are only interpolated from thresholds when they are not "
                          + "declared in other contexts." );

            return;
        }

        // Feature authorities are known and matching?
        // If the feature authorities do not all match, a feature service must be available to resolve the sparse
        // threshold-features
        if ( !DeclarationInterpolator.hasMatchingFeatureAuthorities( builder )
             && Objects.isNull( builder.featureService() ) )
        {
            LOGGER.debug( "Could not interpolate any features from thresholds because the feature authorities were not "
                          + "all present and matching and no feature service was declared to help resolve any missing "
                          + "information." );

            return;
        }

        EvaluationDeclaration snapshot = builder.build();
        Set<Threshold> thresholds = DeclarationUtilities.getInbandThresholds( snapshot );

        // Are there featureful thresholds?
        Set<Threshold> featureful = thresholds.stream()
                                              .filter( n -> Objects.nonNull( n.feature() ) )
                                              .collect( Collectors.toSet() );

        if ( featureful.isEmpty() )
        {
            LOGGER.debug( "There were no featureful thresholds from which to interpolate geospatial features." );

            return;
        }

        Set<GeometryTuple> declared = DeclarationUtilities.getFeatures( snapshot );

        Set<GeometryTuple> leftNotDeclared =
                DeclarationInterpolator.getFeaturesNotAlreadyDeclared( declared,
                                                                       featureful,
                                                                       DatasetOrientation.LEFT );
        Set<GeometryTuple> notDeclared = new HashSet<>( leftNotDeclared );
        Set<GeometryTuple> rightNotDeclared =
                DeclarationInterpolator.getFeaturesNotAlreadyDeclared( declared,
                                                                       featureful,
                                                                       DatasetOrientation.RIGHT );
        notDeclared.addAll( rightNotDeclared );
        if ( DeclarationUtilities.hasBaseline( snapshot ) )
        {
            Set<GeometryTuple> baselineNotDeclared =
                    DeclarationInterpolator.getFeaturesNotAlreadyDeclared( declared,
                                                                           featureful,
                                                                           DatasetOrientation.BASELINE );
            notDeclared.addAll( baselineNotDeclared );
        }

        if ( !notDeclared.isEmpty() )
        {
            LOGGER.debug( "Discovered {} features associated with featureful thresholds that were not separately "
                          + "declared as features to evaluate. Adding the following features to evaluate: {}.",
                          notDeclared.size(),
                          notDeclared );

            // Add any existing features and reset
            if ( Objects.nonNull( builder.features() ) )
            {
                notDeclared.addAll( builder.features()
                                           .geometries() );
            }
            Features interpolatedFeatures = FeaturesBuilder.builder()
                                                           .geometries( notDeclared )
                                                           .build();
            builder.features( interpolatedFeatures );
        }
    }

    /**
     * Interpolates the purpose of a covariate dataset from the context in which it is declared.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateCovariatePurpose( EvaluationDeclarationBuilder builder )
    {
        if ( builder.covariates()
                    .stream()
                    .anyMatch( s -> s.purposes()
                                     .isEmpty() ) )
        {
            LOGGER.debug( "Interpolating the purpose of one or more covariate datasets." );

            // Find the covariates with no declared purpose
            List<CovariateDataset> covariates = builder.covariates()
                                                       .stream()
                                                       .filter( n -> n.purposes()
                                                                      .isEmpty() )
                                                       .toList();
            List<CovariateDataset> adjusted = new ArrayList<>();
            for ( CovariateDataset next : covariates )
            {
                CovariateDatasetBuilder adjustedBuilder = CovariateDatasetBuilder.builder( next );
                if ( Objects.nonNull( next.minimum() )
                     || Objects.nonNull( next.maximum() ) )
                {
                    adjustedBuilder.purposes( Set.of( CovariatePurpose.FILTER ) );
                    LOGGER.debug( INTERPOLATED_THE_COVARIATE_DATASET_TO_HAVE_A_PURPOSE_OF,
                                  next.dataset(),
                                  CovariatePurpose.FILTER );
                }
                else
                {
                    adjustedBuilder.purposes( Set.of( CovariatePurpose.DETECT ) );
                    LOGGER.debug( INTERPOLATED_THE_COVARIATE_DATASET_TO_HAVE_A_PURPOSE_OF,
                                  next.dataset(),
                                  CovariatePurpose.DETECT );
                }

                adjusted.add( adjustedBuilder.build() );
            }

            builder.covariates( adjusted );
        }
    }

    /**
     * Adds autogenerated metrics to the declaration. Does not add any metrics from the
     * {@link MetricConstants.SampleDataGroup#SINGLE_VALUED_TIME_SERIES} group because, while strictly valid for all
     * single-valued time-series, they are niche metrics that have a high computational burden and should be explicitly
     * added by a user. Likewise, does not add any metric differences in the presence of a baseline dataset, which must
     * be declared explicitly.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateMetrics( EvaluationDeclarationBuilder builder )
    {
        DataType rightType = null;

        if ( Objects.nonNull( builder.right() ) )
        {
            rightType = builder.right()
                               .type();
        }

        // No metrics defined and the type is known, so interpolate all valid metrics
        if ( builder.metrics()
                    .isEmpty()
             && Objects.nonNull( rightType ) )
        {
            LOGGER.debug( "Interpolating metrics from the raw declaration." );

            Set<MetricConstants> metrics = new LinkedHashSet<>();

            // Ensemble forecast time-series
            if ( rightType == DataType.ENSEMBLE_FORECASTS )
            {
                Set<MetricConstants> ensemble = MetricConstants.SampleDataGroup.ENSEMBLE.getMetrics();
                Set<MetricConstants> singleValued = MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics();
                metrics.addAll( singleValued );
                metrics.addAll( ensemble );

                // Probability or value thresholds? Then add discrete probability metrics and dichotomous metrics
                if ( !builder.probabilityThresholds()
                             .isEmpty()
                     || !builder.thresholds()
                                .isEmpty() )
                {
                    Set<MetricConstants> discreteProbability =
                            MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY.getMetrics();
                    Set<MetricConstants> dichotomous = MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();
                    metrics.addAll( discreteProbability );
                    metrics.addAll( dichotomous );
                }
            }
            // Single-valued time-series
            else
            {
                Set<MetricConstants> singleValued = MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics();
                metrics.addAll( singleValued );

                // Probability or value thresholds? Then add dichotomous metrics
                if ( !builder.probabilityThresholds()
                             .isEmpty() || !builder.thresholds()
                                                   .isEmpty() )
                {
                    Set<MetricConstants> dichotomous = MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();
                    metrics.addAll( dichotomous );
                }
            }

            // Remove any metric differences
            metrics = metrics.stream()
                             .filter( m -> !m.isDifferenceMetric() )
                             .collect( Collectors.toUnmodifiableSet() );

            // Remove any metrics that are incompatible with other declaration
            metrics = DeclarationInterpolator.getCompatibleMetrics( builder, metrics );

            // Wrap the metrics and set them
            Set<Metric> wrapped =
                    metrics.stream()
                           .map( next -> new Metric( next, null ) )
                           .collect( Collectors.toUnmodifiableSet() );
            builder.metrics( wrapped );
        }
    }

    /**
     * Interpolates the correct combinations of dimensions for summary statistics. All aggregation dimensions in time
     * are applied to each aggregation dimension in space. For example, if the declared dimensions are
     * {@link wres.statistics.generated.SummaryStatistic.StatisticDimension#FEATURES} and
     * {@link wres.statistics.generated.SummaryStatistic.StatisticDimension#VALID_DATE_POOLS}, then each summary
     * statistic will be calculated for all geographic features and valid date pools together. Upon reading the summary
     * statistics in the declaration, the statistics are merely deserialized individually, one for each dimension.
     * Here, the dimensions are combined or interpreted.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateSummaryStatistics( EvaluationDeclarationBuilder builder )
    {
        if ( builder.summaryStatistics()
                    .isEmpty() )
        {
            LOGGER.debug( "No summary statistics to interpolate." );
            return;
        }

        Set<SummaryStatistic.StatisticDimension> dimensions = builder.summaryStatistics()
                                                                     .stream()
                                                                     .flatMap( d -> d.getDimensionList()
                                                                                     .stream() )
                                                                     .collect( Collectors.toSet() );

        // Preserve iteration order of dimensions as they are listed - use a sorted set
        Set<SummaryStatistic.StatisticDimension> poolShaped =
                dimensions.stream()
                          .filter( s -> s.name()
                                         .contains( "POOLS" ) )
                          .collect( Collectors.toCollection( TreeSet::new ) );
        Set<SummaryStatistic.StatisticDimension> featureShaped =
                dimensions.stream()
                          .filter( s -> s == SummaryStatistic.StatisticDimension.FEATURES
                                        || s == SummaryStatistic.StatisticDimension.FEATURE_GROUP )
                          .collect( Collectors.toCollection( TreeSet::new ) );

        if ( !poolShaped.isEmpty() && !featureShaped.isEmpty() )
        {
            Set<SummaryStatistic> summaryStatistics = new HashSet<>();

            // Combine the temporal/pooling dimensions with each geographic dimension and then map per statistic name
            for ( SummaryStatistic.StatisticDimension nextFeature : featureShaped )
            {
                // Preserve order
                Set<SummaryStatistic.StatisticDimension> aggregated = new TreeSet<>( poolShaped );
                aggregated.add( nextFeature );

                // Add the aggregate dimensions to each statistic
                // Start by filtering for unique statistics by name
                Set<SummaryStatistic> filtered = builder.summaryStatistics()
                                                        .stream()
                                                        // Group by name
                                                        .collect( Collectors.groupingBy( SummaryStatistic::getStatistic ) )
                                                        // Iterate the newly grouped collection
                                                        .entrySet()
                                                        .stream()
                                                        // Flatten and unpack the filtered values
                                                        .flatMap( s -> s.getValue()
                                                                        .stream() )
                                                        .collect( Collectors.toSet() );

                filtered.forEach( s -> summaryStatistics.add( s.toBuilder()
                                                               .clearDimension()
                                                               .addAllDimension( aggregated )
                                                               .build() ) );
            }

            LOGGER.debug( "Interpolated the following summary statistics: {}.", summaryStatistics );

            builder.summaryStatistics( summaryStatistics );
        }
    }

    /**
     * Adds the decimal format string to each numeric format type.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateDecimalFormatforNumericFormats( EvaluationDeclarationBuilder builder )
    {
        // Something to adjust?
        if ( Objects.isNull( builder.formats() ) )
        {
            LOGGER.debug( "No numerical formats were discovered and, therefore, adjusted for decimal format." );
            return;
        }

        Outputs.Builder formatsBuilder = builder.formats()
                                                .outputs()
                                                .toBuilder();

        if ( formatsBuilder.hasCsv2() )
        {
            Outputs.Csv2Format.Builder csv2Builder = formatsBuilder.getCsv2Builder();
            Outputs.NumericFormat.Builder numericBuilder = csv2Builder.getOptionsBuilder();
            if ( Objects.nonNull( builder.decimalFormat() ) )
            {
                numericBuilder.setDecimalFormat( builder.decimalFormat()
                                                        .toPattern() );
                numericBuilder.setLeadUnit( Outputs.DurationUnit.valueOf( builder.durationFormat().name() ) );
            }
            csv2Builder.setOptions( numericBuilder );
            formatsBuilder.setCsv2( csv2Builder );
        }

        if ( formatsBuilder.hasCsv() )
        {
            Outputs.CsvFormat.Builder csvBuilder = formatsBuilder.getCsvBuilder();
            Outputs.NumericFormat.Builder numericBuilder = csvBuilder.getOptionsBuilder();
            if ( Objects.nonNull( builder.decimalFormat() ) )
            {
                numericBuilder.setDecimalFormat( builder.decimalFormat()
                                                        .toPattern() );
            }
            csvBuilder.setOptions( numericBuilder );
            formatsBuilder.setCsv( csvBuilder );
        }

        if ( formatsBuilder.hasPairs() )
        {
            Outputs.PairFormat.Builder pairsBuilder = formatsBuilder.getPairsBuilder();
            Outputs.NumericFormat.Builder numericBuilder = pairsBuilder.getOptionsBuilder();
            if ( Objects.nonNull( builder.decimalFormat() ) )
            {
                numericBuilder.setDecimalFormat( builder.decimalFormat()
                                                        .toPattern() );
            }
            pairsBuilder.setOptions( numericBuilder );
            formatsBuilder.setPairs( pairsBuilder );
        }

        // Set the new format info
        builder.formats( new Formats( formatsBuilder.build() ) );
    }

    /**
     * Adds the metrics for which graphics are not required to each geaphics format.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateMetricsToOmitFromGraphicsFormats( EvaluationDeclarationBuilder builder )
    {
        // Something to adjust?
        if ( Objects.isNull( builder.formats() ) )
        {
            LOGGER.debug( "No graphical formats were adjusted for metrics to ignore because no graphics outputs were "
                          + "declared." );
            return;
        }

        Outputs.Builder formatsBuilder = builder.formats()
                                                .outputs()
                                                .toBuilder();

        if ( formatsBuilder.hasPng() )
        {
            List<MetricName> pngAvoid = builder.metrics()
                                               .stream()
                                               .filter( next -> Objects.nonNull( next.parameters() )
                                                                && Boolean.FALSE.equals( next.parameters()
                                                                                             .png() ) )
                                               .map( Metric::name )
                                               .map( next -> MetricName.valueOf( next.name() ) )
                                               .toList();

            LOGGER.debug( "Discovered these metrics to avoid, which will be registered with all graphics outputs: {}.",
                          pngAvoid );
            Outputs.PngFormat.Builder pngBuilder = formatsBuilder.getPngBuilder();
            Outputs.GraphicFormat.Builder graphicBuilder = pngBuilder.getOptionsBuilder();
            graphicBuilder.clearIgnore()
                          .addAllIgnore( pngAvoid );
            pngBuilder.setOptions( graphicBuilder );
            formatsBuilder.setPng( pngBuilder );
        }

        if ( formatsBuilder.hasSvg() )
        {
            List<MetricName> svgAvoid = builder.metrics()
                                               .stream()
                                               .filter( next -> Objects.nonNull( next.parameters() )
                                                                && Boolean.FALSE.equals( next.parameters()
                                                                                             .svg() ) )
                                               .map( Metric::name )
                                               .map( next -> MetricName.valueOf( next.name() ) )
                                               .toList();
            Outputs.SvgFormat.Builder svgBuilder = formatsBuilder.getSvgBuilder();
            Outputs.GraphicFormat.Builder graphicBuilder = svgBuilder.getOptionsBuilder();
            graphicBuilder.clearIgnore()
                          .addAllIgnore( svgAvoid );
            svgBuilder.setOptions( graphicBuilder );
            formatsBuilder.setSvg( svgBuilder );
        }

        // Set the new format info
        builder.formats( new Formats( formatsBuilder.build() ) );
    }

    /**
     * Interpolates graphics format information from other declaration.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateGraphicsFormats( EvaluationDeclarationBuilder builder )
    {
        // Interpolate graphics formats from metric parameters
        DeclarationInterpolator.interpolateGraphicsFormatsFromMetricParameters( builder );

        // Interpolate graphics shapes from pooling window declaration
        DeclarationInterpolator.interpolateGraphicsOptions( builder );
    }

    /**
     * Adds the metrics for which graphics are not required to each graphics format.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateGraphicsFormatsFromMetricParameters( EvaluationDeclarationBuilder builder )
    {
        Outputs.Builder formatsBuilder = Outputs.newBuilder();
        if ( Objects.nonNull( builder.formats() ) )
        {
            formatsBuilder.mergeFrom( builder.formats()
                                             .outputs() );
        }

        // PNG format required but not declared?
        if ( !formatsBuilder.hasPng() )
        {
            boolean png = builder.metrics()
                                 .stream()
                                 .anyMatch( next -> Objects.nonNull( next.parameters() )
                                                    && Boolean.TRUE.equals( next.parameters()
                                                                                .png() ) );
            if ( png )
            {
                LOGGER.debug( "Discovered metrics that require PNG graphics, but the PNG format was not declared in "
                              + "the list of 'output_formats'. Adding the PNG format." );
                formatsBuilder.setPng( Formats.PNG_FORMAT );
            }
        }

        // SVG format required but not declared?
        if ( !formatsBuilder.hasSvg() )
        {
            boolean svg = builder.metrics()
                                 .stream()
                                 .anyMatch( next -> Objects.nonNull( next.parameters() )
                                                    && Boolean.TRUE.equals( next.parameters()
                                                                                .svg() ) );
            if ( svg )
            {
                LOGGER.debug( "Discovered metrics that require SVG graphics, but the SVG format was not declared in "
                              + "the list of 'output_formats'. Adding the SVG format." );
                formatsBuilder.setSvg( Formats.SVG_FORMAT );
            }
        }

        // Set the new format info
        builder.formats( new Formats( formatsBuilder.build() ) );
    }

    /**
     * Interpolates the graphics shapes from other declaration present
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateGraphicsOptions( EvaluationDeclarationBuilder builder )
    {
        Outputs.Builder formatsBuilder = Outputs.newBuilder();
        if ( Objects.nonNull( builder.formats() ) )
        {
            formatsBuilder.mergeFrom( builder.formats()
                                             .outputs() );
        }

        // PNG format
        if ( formatsBuilder.hasPng() )
        {
            Outputs.PngFormat.Builder pngFormat = formatsBuilder.getPng()
                                                                .toBuilder();
            Outputs.GraphicFormat options = DeclarationInterpolator.getGraphicsFormatOptions( pngFormat.getOptions(),
                                                                                              builder );
            pngFormat.setOptions( options );
            formatsBuilder.setPng( pngFormat );
        }

        // SVG format
        if ( formatsBuilder.hasSvg() )
        {
            Outputs.SvgFormat.Builder svgFormat = formatsBuilder.getSvg()
                                                                .toBuilder();
            Outputs.GraphicFormat options = DeclarationInterpolator.getGraphicsFormatOptions( svgFormat.getOptions(),
                                                                                              builder );
            svgFormat.setOptions( options );
            formatsBuilder.setSvg( svgFormat );
        }

        // Set the new format info
        builder.formats( new Formats( formatsBuilder.build() ) );
    }

    /**
     * Creates the graphics format options from the input.
     * @param options the existing options
     * @param builder the declaration builder
     * @return the graphics options
     */
    private static Outputs.GraphicFormat getGraphicsFormatOptions( Outputs.GraphicFormat options,
                                                                   EvaluationDeclarationBuilder builder )
    {
        Outputs.GraphicFormat.Builder newOptions = options.toBuilder();

        // Reference date pools?
        if ( ( !builder.referenceDatePools()
                       .isEmpty()
               || DeclarationInterpolator.hasExplicitReferenceDatePools( builder.timePools() ) )
             && options.getShape() == Outputs.GraphicFormat.GraphicShape.DEFAULT )
        {
            newOptions.setShape( Outputs.GraphicFormat.GraphicShape.ISSUED_DATE_POOLS );
        }
        // Valid date pools?
        else if ( ( !builder.validDatePools()
                            .isEmpty()
                    || Objects.nonNull( builder.eventDetection() )
                    || DeclarationInterpolator.hasExplicitValidDatePools( builder.timePools() ) )
                  && options.getShape() == Outputs.GraphicFormat.GraphicShape.DEFAULT )
        {
            newOptions.setShape( Outputs.GraphicFormat.GraphicShape.VALID_DATE_POOLS );
        }

        // Duration format?
        if ( Objects.nonNull( builder.durationFormat() ) )
        {
            newOptions.setLeadUnit( Outputs.DurationUnit.valueOf( builder.durationFormat().name() ) );
        }

        return newOptions.build();
    }

    /**
     * @param timeWindows the time windows
     * @return whether there is more than one time window with different reference dates
     */
    private static boolean hasExplicitReferenceDatePools( Set<TimeWindow> timeWindows )
    {
        Set<Pair<Timestamp, Timestamp>> referenceDates = new HashSet<>();
        for ( TimeWindow next : timeWindows )
        {
            Pair<Timestamp, Timestamp> pair = Pair.of( next.getEarliestReferenceTime(), next.getLatestReferenceTime() );
            referenceDates.add( pair );

            // Short-circuit
            if ( referenceDates.size() > 1 )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * @param timeWindows the time windows
     * @return whether there is more than one time window with different valid dates
     */
    private static boolean hasExplicitValidDatePools( Set<TimeWindow> timeWindows )
    {
        Set<Pair<Timestamp, Timestamp>> validDates = new HashSet<>();
        for ( TimeWindow next : timeWindows )
        {
            Pair<Timestamp, Timestamp> pair = Pair.of( next.getEarliestValidTime(), next.getLatestValidTime() );
            validDates.add( pair );

            // Short-circuit
            if ( validDates.size() > 1 )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Copies the evaluation units to the value threshold units when they are not declared explicitly.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateMeasurementUnitForValueThresholds( EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( builder.unit() ) )
        {
            LOGGER.debug( "Interpolating measurement units for value thresholds." );

            String unit = builder.unit();

            // Value thresholds
            Set<Threshold> valueThresholds = builder.thresholds();
            valueThresholds = DeclarationInterpolator.addUnitToValueThresholds( valueThresholds, unit );
            builder.thresholds( valueThresholds );

            // Threshold sets
            Set<Threshold> thresholdSets = builder.thresholdSets();
            thresholdSets = DeclarationInterpolator.addUnitToValueThresholds( thresholdSets, unit );
            builder.thresholdSets( thresholdSets );

            // Threshold sources
            DeclarationInterpolator.addUnitToValueThresholdSources( builder, unit );

            // Individual metrics
            Set<Metric> metrics = builder.metrics();
            Set<Metric> adjustedMetrics = new LinkedHashSet<>( metrics.size() );
            for ( Metric next : metrics )
            {
                // Adjust?
                if ( Objects.nonNull( next.parameters() ) && !next.parameters()
                                                                  .thresholds()
                                                                  .isEmpty() )
                {
                    Set<Threshold> adjusted = next.parameters()
                                                  .thresholds();
                    adjusted = DeclarationInterpolator.addUnitToValueThresholds( adjusted, unit );

                    MetricParameters adjustedParameters = MetricParametersBuilder.builder( next.parameters() )
                                                                                 .thresholds( adjusted )
                                                                                 .build();
                    Metric adjustedMetric = MetricBuilder.builder( next )
                                                         .parameters( adjustedParameters )
                                                         .build();
                    adjustedMetrics.add( adjustedMetric );
                }
                // Retain the existing metric parameters
                else
                {
                    adjustedMetrics.add( next );
                }
            }

            builder.metrics( adjustedMetrics );
        }
    }

    /**
     * Adds the prescribed measurement unit to all value threshold sources unless already defined.
     * @param builder the builder
     * @param unit the unit
     */

    private static void addUnitToValueThresholdSources( EvaluationDeclarationBuilder builder,
                                                        String unit )
    {
        Set<ThresholdSource> thresholdSources = builder.thresholdSources();
        if ( Objects.nonNull( thresholdSources ) )
        {
            Set<ThresholdSource> adjustedSources = new HashSet<>();

            for ( ThresholdSource nextSource : thresholdSources )
            {
                String nextUnit = nextSource.unit();
                if ( Objects.isNull( nextUnit ) || nextUnit.isBlank() )
                {
                    ThresholdSource adjusted = ThresholdSourceBuilder.builder( nextSource )
                                                                     .unit( unit )
                                                                     .build();
                    adjustedSources.add( adjusted );
                }
                else
                {
                    adjustedSources.add( nextSource );
                }
            }

            builder.thresholdSources( adjustedSources );
        }
    }

    /**
     * Interpolates the metric-specific thresholds for metrics without declared thresholds using the thresholds
     * declared at a higher level.
     *
     * @param builder the builder to mutate
     */
    private static void interpolateThresholdsForIndividualMetrics( EvaluationDeclarationBuilder builder )
    {
        LOGGER.debug( "Interpolating thresholds for metrics." );

        // Assemble the thresholds for each type
        Set<Threshold> allThresholds = new LinkedHashSet<>();
        allThresholds.addAll( builder.probabilityThresholds() );
        allThresholds.addAll( builder.thresholds() );
        allThresholds.addAll( builder.classifierThresholds() );
        allThresholds.addAll( builder.thresholdSets() );

        // Group by type
        Map<ThresholdType, Set<Threshold>> thresholdsByType
                = DeclarationUtilities.groupThresholdsByType( allThresholds );
        LOGGER.debug( "When interpolating thresholds for metrics, discovered the following thresholds to use: {}.",
                      thresholdsByType );

        DeclarationInterpolator.addThresholdsToMetrics( thresholdsByType, builder, true, false );
    }

    /**
     * Interpolates low-level metric parameters from corresponding parameter values that can be set for all metrics.
     * The general rule with overrides is to interpolate the parameter value when it is missing and to warn when a
     * different value is already set. In other words, the high-level parameter value is treated as a default that
     * cannot override an existing low-level setting. The warning should be user-facing and, therefore, handled during
     * declaration validation.
     *
     * @param builder the builder
     */
    private static void interpolateMetricParameters( EvaluationDeclarationBuilder builder )
    {
        wres.statistics.generated.Pool.EnsembleAverageType topType = builder.ensembleAverageType();

        Set<Metric> adjusted = new HashSet<>();
        for ( Metric next : builder.metrics() )
        {
            MetricBuilder metricBuilder = MetricBuilder.builder( next );
            MetricParametersBuilder parBuilder = MetricParametersBuilder.builder();

            if ( Objects.nonNull( next.parameters() ) )
            {
                parBuilder = MetricParametersBuilder.builder( next.parameters() );

                // Interpolate the summary statistic dimension for timing error summary statistics
                if ( Objects.nonNull( parBuilder.summaryStatistics() ) )
                {
                    Set<SummaryStatistic> summaryStatistics =
                            DeclarationInterpolator.interpolateTimingErrorSummaryStatistics( parBuilder.summaryStatistics() );
                    parBuilder.summaryStatistics( summaryStatistics );
                }
            }
            Pool.EnsembleAverageType parType = parBuilder.ensembleAverageType();
            if ( Objects.isNull( parType )
                 && Objects.nonNull( topType ) )
            {
                parBuilder.ensembleAverageType( topType );
            }
            metricBuilder.parameters( parBuilder.build() );
            adjusted.add( metricBuilder.build() );
        }

        // Set the adjusted metrics
        builder.metrics( adjusted );
    }

    /**
     * Adds the prescribed dimension to each summary statistic.
     *
     * @param statistics the existing summary statistics
     * @return the summary statistics with interpolated dimension
     */
    private static Set<SummaryStatistic> interpolateTimingErrorSummaryStatistics( Set<SummaryStatistic> statistics )
    {
        Objects.requireNonNull( statistics );

        if ( LOGGER.isDebugEnabled()
             && !statistics.isEmpty() )
        {
            LOGGER.debug( "Interpolated a dimension of {} for the summary statistics: {}.",
                          SummaryStatistic.StatisticDimension.TIMING_ERRORS,
                          statistics.stream()
                                    .map( SummaryStatistic::getStatistic )
                                    .toList() );
        }

        return statistics.stream()
                         .map( n -> n.toBuilder()
                                     .clearDimension()
                                     .addDimension( SummaryStatistic.StatisticDimension.TIMING_ERRORS )
                                     .build() )
                         .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Removes any metrics from the mutable set of auto-generated metrics that are inconsistent with other declaration.
     * @param builder the builder
     * @param metrics the metrics
     */
    private static Set<MetricConstants> getCompatibleMetrics( EvaluationDeclarationBuilder builder,
                                                              Set<MetricConstants> metrics )
    {
        Set<MetricConstants> compatible = new HashSet<>( metrics );

        // Remove any skill metrics that require an explicit baseline
        if ( Objects.isNull( builder.baseline() ) )
        {
            compatible = compatible.stream()
                                   .filter( m -> !m.isExplicitBaselineRequired() )
                                   .collect( Collectors.toSet() );
        }

        return Collections.unmodifiableSet( compatible );
    }

    /**
     * Adds a default output format of CSV2 where none exists.
     *
     * @param builder the builder to mutate
     */
    private static void interpolateOutputs( EvaluationDeclarationBuilder builder )
    {
        DeclarationInterpolator.interpolateOutputFormatsWhenNoneDeclared( builder );

        DeclarationInterpolator.interpolateCombinedGraphics( builder );
    }

    /**
     * Adds a default output format of CSV2 where none exists.
     *
     * @param builder the builder to mutate
     */
    private static void interpolateOutputFormatsWhenNoneDeclared( EvaluationDeclarationBuilder builder )
    {
        if ( Objects.isNull( builder.formats() )
             || Objects.equals( builder.formats()
                                       .outputs(), Outputs.getDefaultInstance() ) )
        {
            LOGGER.debug( "Adding a default output format of CSV2 because no output formats were declared." );
            Outputs.Builder formatsBuilder = Outputs.newBuilder()
                                                    .setCsv2( Formats.CSV2_FORMAT );
            builder.formats( new Formats( formatsBuilder.build() ) );
        }
    }

    /**
     * Interpolates the option for generating combined graphics.
     *
     * @param builder the builder to mutate
     */
    private static void interpolateCombinedGraphics( EvaluationDeclarationBuilder builder )
    {
        if ( DeclarationUtilities.hasGraphicsFormats( builder.build() )
             && Boolean.TRUE.equals( builder.combinedGraphics() ) )
        {
            LOGGER.debug( "Discovered a request for combined graphics. Transferring this to the formats description." );
            builder.formats( new Formats( Outputs.newBuilder( builder.formats()
                                                                     .outputs() )
                                                 .setCombineGraphics( true )
                                                 .build() ) );
        }
    }

    /**
     * Interpolates the missing components of any explicitly declared time windows.
     *
     * @param builder the builder to mutate
     */
    private static void interpolateTimeWindows( EvaluationDeclarationBuilder builder )
    {
        Set<TimeWindow> timeWindows = builder.timePools();
        Set<TimeWindow> adjustedTimeWindows = new HashSet<>();

        // Set the earliest and latest lead durations
        Pair<com.google.protobuf.Duration, com.google.protobuf.Duration> leadTimes =
                DeclarationInterpolator.getLeadDurationInterval( builder );
        com.google.protobuf.Duration earliestProtoDuration = leadTimes.getLeft();
        com.google.protobuf.Duration latestProtoDuration = leadTimes.getRight();

        // Set the earliest and latest valid times
        Pair<Timestamp, Timestamp> validTimes = DeclarationInterpolator.getValidTimeInterval( builder );
        Timestamp earliestValidProto = validTimes.getLeft();
        Timestamp latestValidProto = validTimes.getRight();

        // Set the earliest and latest reference times
        Pair<Timestamp, Timestamp> referenceTimes = DeclarationInterpolator.getReferenceTimeInterval( builder );
        Timestamp earliestReferenceProto = referenceTimes.getLeft();
        Timestamp latestReferenceProto = referenceTimes.getRight();

        for ( TimeWindow next : timeWindows )
        {
            TimeWindow.Builder nextBuilder = next.toBuilder();

            if ( !next.hasEarliestLeadDuration() )
            {
                nextBuilder.setEarliestLeadDuration( earliestProtoDuration );
            }

            if ( !next.hasLatestLeadDuration() )
            {
                nextBuilder.setLatestLeadDuration( latestProtoDuration );
            }

            if ( !next.hasEarliestValidTime() )
            {
                nextBuilder.setEarliestValidTime( earliestValidProto );
            }

            if ( !next.hasLatestValidTime() )
            {
                nextBuilder.setLatestValidTime( latestValidProto );
            }

            if ( !next.hasEarliestReferenceTime() )
            {
                nextBuilder.setEarliestReferenceTime( earliestReferenceProto );
            }

            if ( !next.hasLatestReferenceTime() )
            {
                nextBuilder.setLatestReferenceTime( latestReferenceProto );
            }

            TimeWindow nextAdjusted = nextBuilder.build();
            adjustedTimeWindows.add( nextAdjusted );
        }

        builder.timePools( adjustedTimeWindows );
    }

    /**
     * @param builder the declaration builder
     * @return the lead duration interval
     */

    private static Pair<com.google.protobuf.Duration, com.google.protobuf.Duration>
    getLeadDurationInterval( EvaluationDeclarationBuilder builder )
    {
        Duration earliestDuration = MessageUtilities.DURATION_MIN;
        Duration latestDuration = MessageUtilities.DURATION_MAX;

        if ( Objects.nonNull( builder.leadTimes() ) )
        {
            if ( Objects.nonNull( builder.leadTimes()
                                         .minimum() ) )
            {
                earliestDuration = builder.leadTimes()
                                          .minimum();
            }
            if ( Objects.nonNull( builder.leadTimes()
                                         .maximum() ) )
            {
                latestDuration = builder.leadTimes()
                                        .maximum();
            }
        }

        com.google.protobuf.Duration earliestProtoDuration = MessageUtilities.getDuration( earliestDuration );
        com.google.protobuf.Duration latestProtoDuration = MessageUtilities.getDuration( latestDuration );

        return Pair.of( earliestProtoDuration, latestProtoDuration );
    }

    /**
     * @param builder the declaration builder
     * @return the valid time interval
     */

    private static Pair<Timestamp, Timestamp> getValidTimeInterval( EvaluationDeclarationBuilder builder )
    {
        Instant earliestValid = Instant.MIN;
        Instant latestValid = Instant.MAX;
        if ( Objects.nonNull( builder.validDates() ) )
        {
            if ( Objects.nonNull( builder.validDates()
                                         .minimum() ) )
            {
                earliestValid = builder.validDates()
                                       .minimum();
            }
            if ( Objects.nonNull( builder.validDates()
                                         .maximum() ) )
            {
                latestValid = builder.validDates()
                                     .maximum();
            }
        }

        Timestamp earliestValidProto = MessageUtilities.getTimestamp( earliestValid );
        Timestamp latestValidProto = MessageUtilities.getTimestamp( latestValid );

        return Pair.of( earliestValidProto, latestValidProto );
    }


    /**
     * @param builder the declaration builder
     * @return the reference time interval
     */

    private static Pair<Timestamp, Timestamp> getReferenceTimeInterval( EvaluationDeclarationBuilder builder )
    {
        Instant earliestReference = Instant.MIN;
        Instant latestReference = Instant.MAX;
        if ( Objects.nonNull( builder.referenceDates() ) )
        {
            if ( Objects.nonNull( builder.referenceDates()
                                         .minimum() ) )
            {
                earliestReference = builder.referenceDates()
                                           .minimum();
            }
            if ( Objects.nonNull( builder.referenceDates()
                                         .maximum() ) )
            {
                latestReference = builder.referenceDates()
                                         .maximum();
            }
        }

        Timestamp earliestReferenceProto = MessageUtilities.getTimestamp( earliestReference );
        Timestamp latestReferenceProto = MessageUtilities.getTimestamp( latestReference );

        return Pair.of( earliestReferenceProto, latestReferenceProto );
    }

    /**
     * Adds the unit to each value threshold in the set that does not have the unit defined.
     *
     * @param thresholds the thresholds
     * @return the adjusted thresholds
     */
    private static Set<Threshold> addUnitToValueThresholds( Set<Threshold> thresholds,
                                                            String unit )
    {
        Set<Threshold> adjustedThresholds = new LinkedHashSet<>( thresholds.size() );
        for ( Threshold next : thresholds )
        {
            // Value threshold without a unit string...
            if ( next.type() == wres.config.yaml.components.ThresholdType.VALUE
                 && next.threshold()
                        .getThresholdValueUnits()
                        .isBlank()
                 // ... that is not an "all data" threshold
                 && !DeclarationUtilities.ALL_DATA_THRESHOLD.threshold()
                                                            .equals( next.threshold() ) )
            {
                wres.statistics.generated.Threshold adjusted = next.threshold()
                                                                   .toBuilder()
                                                                   .setThresholdValueUnits( unit )
                                                                   .build();
                Threshold threshold = ThresholdBuilder.builder( next )
                                                      .threshold( adjusted )
                                                      .build();
                adjustedThresholds.add( threshold );
                LOGGER.debug( "Adjusted a value threshold to use the evaluation unit of {} because the threshold unit "
                              + "was not declared explicitly. The adjusted threshold is: {}", unit, threshold );
            }
            else
            {
                adjustedThresholds.add( next );
            }
        }

        return Collections.unmodifiableSet( adjustedThresholds );
    }

    /**
     * Interpolates all URIs, resolving relative paths where needed.
     *
     * @param builder the declaration builder
     */

    private static void interpolateUris( EvaluationDeclarationBuilder builder )
    {
        Dataset adjustedLeft = DeclarationInterpolator.interpolateUris( builder.left() );
        Dataset adjustedRight = DeclarationInterpolator.interpolateUris( builder.right() );

        // Adjust the left and right
        builder.left( adjustedLeft )
               .right( adjustedRight );

        // Baseline?
        if ( Objects.nonNull( builder.baseline() ) )
        {
            Dataset adjustedBaselineDataset = DeclarationInterpolator.interpolateUris( builder.baseline()
                                                                                              .dataset() );
            BaselineDataset adjustedBaseline = BaselineDatasetBuilder.builder( builder.baseline() )
                                                                     .dataset( adjustedBaselineDataset )
                                                                     .build();
            // Adjust the baseline
            builder.baseline( adjustedBaseline );
        }

        // Covariates?
        List<CovariateDataset> adjustedCovariates = new ArrayList<>();
        for ( CovariateDataset covariate : builder.covariates() )
        {
            CovariateDatasetBuilder covariateBuilder = CovariateDatasetBuilder.builder( covariate );
            Dataset adjustedDataset = DeclarationInterpolator.interpolateUris( covariate.dataset() );
            covariateBuilder.dataset( adjustedDataset );
            adjustedCovariates.add( covariateBuilder.build() );
        }
        builder.covariates( adjustedCovariates );

        // Adjust any threshold sources
        Set<ThresholdSource> thresholdSources = builder.thresholdSources();
        Set<ThresholdSource> adjustedThresholdSources = new HashSet<>();
        for ( ThresholdSource nextSource : thresholdSources )
        {
            URI adjustedUri = DeclarationInterpolator.interpolateUri( nextSource.uri() );
            ThresholdSource adjustedSource = ThresholdSourceBuilder.builder( nextSource )
                                                                   .uri( adjustedUri )
                                                                   .build();
            adjustedThresholdSources.add( adjustedSource );
        }
        builder.thresholdSources( adjustedThresholdSources );
    }

    /**
     * Interpolates all URIs, resolving relative paths where needed.
     *
     * @param dataset the dataset whose source URIs should be interpolated
     * @return the dataset with interpolated URIs
     */

    private static Dataset interpolateUris( Dataset dataset )
    {
        if ( Objects.nonNull( dataset )
             && Objects.nonNull( dataset.sources() ) )
        {
            List<Source> sources = dataset.sources();
            List<Source> adjusted = new ArrayList<>();
            for ( Source next : sources )
            {
                URI adjustedUri = DeclarationInterpolator.interpolateUri( next.uri() );
                Source nextAdjusted = SourceBuilder.builder( next )
                                                   .uri( adjustedUri )
                                                   .build();
                adjusted.add( nextAdjusted );
            }

            return DatasetBuilder.builder( dataset )
                                 .sources( adjusted )
                                 .build();
        }

        LOGGER.debug( "No data sources to interpolate." );

        return dataset;
    }

    /**
     * Interpolates a URI, adding a file scheme where needed, resolving relative paths and performing any
     * platform-dependent disambiguation.
     * @param uri the URI
     * @return the adjusted URI
     */

    private static URI interpolateUri( URI uri )
    {
        // Web-like URI? If so, return as-is
        if ( Objects.isNull( uri ) )
        {
            LOGGER.debug( "Not adjusting null URI." );
            return null;
        }

        if ( !uri.isAbsolute() )
        {
            // Look for a data directory override first
            String dataDirectory = System.getProperty( "wres.dataDirectory" );
            if ( Objects.isNull( dataDirectory ) )
            {
                dataDirectory = System.getProperty( "user.dir" );
                LOGGER.debug( "Failed to discover a system property 'wres.dataDirectory': using the 'user.dir' as the "
                              + "default data directory, which is: {}", dataDirectory );
            }

            Path dataDirectoryPath = Paths.get( dataDirectory );
            URI absolute = dataDirectoryPath.resolve( uri.getPath() )
                                            .toUri();

            LOGGER.debug( "Adjusted a relative path of {} to an absolute path of {}.", uri, absolute );

            uri = absolute;
        }

        return uri;
    }

    /**
     * Looks for the time zone offset assigned to a dataset and copies this offset to all corresponding missing entries
     * for each data source.
     * @param builder the declaration builder to adjust
     */

    private static void interpolateTimeZoneOffsets( EvaluationDeclarationBuilder builder )
    {
        // Left dataset
        if ( Objects.nonNull( builder.left() ) )
        {
            ZoneOffset leftOffset = builder.left()
                                           .timeZoneOffset();
            if ( Objects.nonNull( leftOffset ) )
            {
                Dataset left = DeclarationInterpolator.copyTimeZoneToAllSources( leftOffset, builder.left() );
                builder.left( left );
            }
        }

        // Right dataset
        if ( Objects.nonNull( builder.right() ) )
        {
            ZoneOffset rightOffset = builder.right()
                                            .timeZoneOffset();
            if ( Objects.nonNull( rightOffset ) )
            {
                Dataset right = DeclarationInterpolator.copyTimeZoneToAllSources( rightOffset, builder.right() );
                builder.right( right );
            }
        }

        // Baseline dataset
        if ( DeclarationUtilities.hasBaseline( builder ) )
        {
            ZoneOffset baselineOffset = builder.baseline()
                                               .dataset()
                                               .timeZoneOffset();
            if ( Objects.nonNull( baselineOffset ) )
            {
                Dataset baselineDataset =
                        DeclarationInterpolator.copyTimeZoneToAllSources( baselineOffset, builder.baseline()
                                                                                                 .dataset() );
                BaselineDataset baseline = BaselineDatasetBuilder.builder( builder.baseline() )
                                                                 .dataset( baselineDataset )
                                                                 .build();
                builder.baseline( baseline );
            }
        }
    }

    /**
     * Copies the supplied time zone offset to all sources that are missing one.
     * @param offset the offset to copy
     * @param dataset the dataset whose sources without a time zone offset should be updated
     * @return the possible adjusted dataset
     */

    private static Dataset copyTimeZoneToAllSources( ZoneOffset offset, Dataset dataset )
    {
        DatasetBuilder builder = DatasetBuilder.builder( dataset );
        List<Source> sources = new ArrayList<>();
        for ( Source source : dataset.sources() )
        {
            if ( Objects.isNull( source.timeZoneOffset() ) )
            {
                Source adjusted = SourceBuilder.builder( source )
                                               .timeZoneOffset( offset )
                                               .build();
                sources.add( adjusted );
            }
            else
            {
                sources.add( source );
            }
        }

        // Set the adjusted sources
        builder.sources( sources );

        return builder.build();
    }

    /**
     * Attempts to interpolate the feature authority information from the data sources present, if not explicitly
     * declared.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateFeatureAuthorities( EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( builder.left() ) )
        {
            Set<FeatureAuthority> leftAuthorities = DeclarationUtilities.getFeatureAuthorities( builder.left() );
            Dataset left = DeclarationInterpolator.setFeatureAuthorityIfConsistent( builder.left(),
                                                                                    leftAuthorities,
                                                                                    DatasetOrientation.LEFT );
            builder.left( left );
        }

        if ( Objects.nonNull( builder.right() ) )
        {
            Set<FeatureAuthority> rightAuthorities = DeclarationUtilities.getFeatureAuthorities( builder.right() );
            Dataset right = DeclarationInterpolator.setFeatureAuthorityIfConsistent( builder.right(),
                                                                                     rightAuthorities,
                                                                                     DatasetOrientation.RIGHT );
            builder.right( right );
        }

        if ( DeclarationUtilities.hasBaseline( builder ) )
        {
            Set<FeatureAuthority> baselineAuthorities = DeclarationUtilities.getFeatureAuthorities( builder.baseline()
                                                                                                           .dataset() );
            Dataset baseline = DeclarationInterpolator.setFeatureAuthorityIfConsistent( builder.baseline()
                                                                                               .dataset(),
                                                                                        baselineAuthorities,
                                                                                        DatasetOrientation.BASELINE );

            BaselineDataset adjustedBaseline = BaselineDatasetBuilder.builder( builder.baseline() )
                                                                     .dataset( baseline )
                                                                     .build();

            builder.baseline( adjustedBaseline );
        }

        // Covariates
        List<CovariateDataset> adjustedCovariates = new ArrayList<>();
        for ( CovariateDataset covariateDataset : builder.covariates() )
        {
            Dataset covariate = covariateDataset.dataset();
            Set<FeatureAuthority> covariateAuthorities = DeclarationUtilities.getFeatureAuthorities( covariate );
            Dataset adjustedCovariate = DeclarationInterpolator.setFeatureAuthorityIfConsistent( covariate,
                                                                                                 covariateAuthorities,
                                                                                                 DatasetOrientation.COVARIATE );
            CovariateDataset adjustedCovariateDataset = CovariateDatasetBuilder.builder( covariateDataset )
                                                                               .dataset( adjustedCovariate )
                                                                               .build();
            adjustedCovariates.add( adjustedCovariateDataset );
        }
        builder.covariates( adjustedCovariates );
    }

    /**
     * Sets the feature authority for the dataset if there is a single authority.
     * @param dataset the dataset
     * @param authorities the authorities
     * @param orientation the dataset orientation to help with logging
     * @return the possibly adjusted dataset
     */
    private static Dataset setFeatureAuthorityIfConsistent( Dataset dataset,
                                                            Set<FeatureAuthority> authorities,
                                                            DatasetOrientation orientation )
    {
        if ( authorities.size() == 1 )
        {
            FeatureAuthority authority = authorities.iterator()
                                                    .next();

            LOGGER.debug( "Interpolated a common feature authority for all {} datasets, which was {}.",
                          orientation,
                          authority );

            return DatasetBuilder.builder( dataset )
                                 .featureAuthority( authority )
                                 .build();
        }
        else
        {
            LOGGER.debug( "Failed to interpolate a common feature authority for all {} datasets. The "
                          + "discovered authorities were: {}", orientation, authorities );

            return dataset;
        }
    }

    /**
     * Interpolates the orientation of the feature names associated with each covariate dataset.
     * @param builder the builder
     * @return any warnings or errors encountered
     */

    private static List<EvaluationStatusEvent> interpolateCovariateFeatureOrientations( EvaluationDeclarationBuilder builder )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        List<CovariateDataset> adjustedCovariates = new ArrayList<>();
        for ( CovariateDataset covariateDataset : builder.covariates() )
        {
            FeatureAuthority covariateAuthority = covariateDataset.dataset()
                                                                  .featureAuthority();
            DatasetOrientation orientation;

            // Default to LEFT and warn
            if ( Objects.isNull( covariateAuthority ) )
            {
                orientation = DatasetOrientation.LEFT;

                String extra = "";

                if ( Objects.nonNull( covariateDataset.dataset()
                                                      .variable() )
                     && Objects.nonNull( covariateDataset.dataset()
                                                         .variable()
                                                         .name() ) )
                {
                    extra = "for variable '" + covariateDataset.dataset()
                                                               .variable()
                                                               .name() + "' ";
                }

                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                               .setEventMessage( "Discovered a covariate dataset "
                                                                 + extra
                                                                 + "without an explicit 'feature_authority' declared."
                                                                 + " It is assumed that this covariate dataset uses "
                                                                 + "the same feature identities as the 'observed' "
                                                                 + "dataset. If this is incorrect, please "
                                                                 + "declare an explicit 'feature_authority' for both "
                                                                 + "the covariate dataset and the 'predicted' or "
                                                                 + "'baseline' dataset whose features are identified "
                                                                 + "with the same authority as the covariate. Failure "
                                                                 + "to correctly identify the feature names of the "
                                                                 + "covariate dataset could lead to the discovery of "
                                                                 + "no covariate data, which could in turn lead to "
                                                                 + "all pairs being filtered/ignored." )
                                               .build();
                events.add( event );
            }
            // Correlate with the declared authority of other datasets
            else
            {
                orientation = DeclarationInterpolator.getCovariateFeatureOrientation( builder.build(),
                                                                                      covariateDataset );
            }

            LOGGER.debug( "Interpolated a dataset orientation of {} for the covariate dataset {}.",
                          orientation,
                          covariateDataset );

            CovariateDataset adjusted = CovariateDatasetBuilder.builder( covariateDataset )
                                                               .featureNameOrientation( orientation )
                                                               .build();
            adjustedCovariates.add( adjusted );
        }
        builder.covariates( adjustedCovariates );

        return Collections.unmodifiableList( events );
    }

    /**
     * Inspects the covariate dataset to determine the {@link DatasetOrientation} of the dataset whose feature
     * authority matches that of the covariate. If the covariate dataset has no declared feature authority, defaults
     * to {@link DatasetOrientation#LEFT}.
     *
     * @param declaration the declaration
     * @param covariate the covariate dataset
     * @return the orientation of the dataset whose feature authority matches the covariate
     */

    private static DatasetOrientation getCovariateFeatureOrientation( EvaluationDeclaration declaration,
                                                                      CovariateDataset covariate )
    {
        FeatureAuthority covariateAuthority = covariate.dataset()
                                                       .featureAuthority();

        FeatureAuthority rightAuthority = DeclarationUtilities.getFeatureAuthorityFor( declaration,
                                                                                       DatasetOrientation.RIGHT );

        // In principle, the same authority could be declared/identified for more than one side of data and yet the
        // post-ingest attributes of the feature (e.g. presence/absence of coordinates) could lead to different
        // feature identities (e.g., in a database) for different dataset orientations, given the same feature name.
        // In that case, the retrieval of time-series data by feature name would need to be more sophisticated when the
        // feature authorities match.
        if ( covariateAuthority == rightAuthority )
        {
            return DatasetOrientation.RIGHT;
        }
        else if ( DeclarationUtilities.hasBaseline( declaration )
                  && DeclarationUtilities.getFeatureAuthorityFor( declaration,
                                                                  DatasetOrientation.BASELINE )
                     == covariateAuthority )
        {
            return DatasetOrientation.BASELINE;
        }
        else
        {
            return DatasetOrientation.LEFT;
        }
    }

    /**
     * Associates the specified thresholds with the specified metric and adds an "all data" threshold as needed. Also
     * adds featureful thresholds where possible.
     *
     * @param globalThresholds the mapped thresholds
     * @param metric the metric
     * @param builder the builder
     * @param addAllData is true to add an all data threshold where appropriate, false otherwise
     * @param addThresholds is true to add the global and local thresholds together, otherwise favor the local thresholds
     * @return the adjusted metric
     */

    private static Metric addThresholdsToMetric( Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> globalThresholds,
                                                 Metric metric,
                                                 EvaluationDeclarationBuilder builder,
                                                 boolean addAllData,
                                                 boolean addThresholds )
    {
        MetricConstants name = metric.name();

        LOGGER.debug( "Adjusting metric {} to include thresholds, as needed.", name );

        MetricParameters nextParameters = metric.parameters();
        MetricParametersBuilder parametersBuilder = MetricParametersBuilder.builder();

        boolean overrideAllData = false;

        // Add existing parameters where available
        if ( Objects.nonNull( nextParameters ) )
        {
            parametersBuilder = MetricParametersBuilder.builder( nextParameters );

            // Special case: an 'all data' threshold is declared explicitly for a metric. This indicates that the metric
            // should be computed for 'all data' only, as the 'all data' threshold is otherwise added by default, which
            // would render the request meaningless
            Set<Threshold> test = parametersBuilder.thresholds()
                                                   .stream()
                                                   .filter( t -> !t.generated() )
                                                   .collect( Collectors.toSet() );

            if ( test.size() == 1
                 && test.iterator()
                        .next()
                        .threshold()
                        .equals( DeclarationUtilities.ALL_DATA_THRESHOLD.threshold() ) )
            {
                LOGGER.debug( "The metric {} contained a threshold override for 'all data' only.", name );
                globalThresholds = Map.of();
                overrideAllData = true;
            }
        }

        // Value thresholds
        Set<Threshold> valByType =
                globalThresholds.get( ThresholdType.VALUE );
        Set<Threshold> valueThresholds =
                DeclarationInterpolator.getCombinedThresholds( valByType,
                                                               parametersBuilder.thresholds(),
                                                               addThresholds );

        // Add "all data" thresholds?
        if ( name.isContinuous()
             && addAllData
             && !overrideAllData )
        {
            valueThresholds.add( DeclarationUtilities.GENERATED_ALL_DATA_THRESHOLD );
        }

        // Render the value thresholds featureful, if possible
        valueThresholds = DeclarationInterpolator.getFeatureFulThresholds( valueThresholds, builder );

        // Set them
        parametersBuilder.thresholds( valueThresholds );

        // Probability thresholds
        Set<Threshold> probByType =
                globalThresholds.get( ThresholdType.PROBABILITY );
        Set<Threshold> probabilityThresholds =
                DeclarationInterpolator.getCombinedThresholds( probByType,
                                                               parametersBuilder.probabilityThresholds(),
                                                               addThresholds );

        // Render the probability thresholds featureful, if possible
        probabilityThresholds = DeclarationInterpolator.getFeatureFulThresholds( probabilityThresholds, builder );
        parametersBuilder.probabilityThresholds( probabilityThresholds );

        // Classifier thresholds, which only apply to categorical measures
        if ( ( Objects.isNull( parametersBuilder.classifierThresholds() )
               || parametersBuilder.classifierThresholds()
                                   .isEmpty() )
             && ( name.isInGroup( MetricConstants.SampleDataGroup.DICHOTOMOUS )
                  || name.isInGroup( MetricConstants.SampleDataGroup.MULTICATEGORY ) ) )
        {
            Set<Threshold> classByType =
                    globalThresholds.get( ThresholdType.PROBABILITY_CLASSIFIER );
            Set<Threshold> classifierThresholds =
                    DeclarationInterpolator.getCombinedThresholds( classByType,
                                                                   parametersBuilder.classifierThresholds(),
                                                                   addThresholds );
            // Render the classifier thresholds featureful, if possible
            classifierThresholds = DeclarationInterpolator.getFeatureFulThresholds( classifierThresholds, builder );
            parametersBuilder.classifierThresholds( classifierThresholds );
        }

        Metric adjustedMetric = metric;

        // Create the adjusted metric
        MetricParameters newParameters = parametersBuilder.build();

        // Something changed, which resulted in non-default parameters?
        if ( !newParameters.equals( nextParameters ) &&
             !newParameters.equals( MetricParametersBuilder.builder()
                                                           .build() ) )
        {
            adjustedMetric = MetricBuilder.builder( metric )
                                          .parameters( newParameters )
                                          .build();

            LOGGER.debug( "Adjusted a metric to include thresholds. The original metric was: {}. The adjusted metric "
                          + "is {}.", metric, adjustedMetric );
        }

        return adjustedMetric;
    }

    /**
     * Combines the input thresholds. If {@code adds} is {@code true} then the thresholds are added together.
     * Otherwise, the local thresholds are returned where they exist and the global thresholds if not.
     * @param globalThresholds the top-level thresholds, independent of metrics
     * @param localThresholds the metric-specific thresholds
     * @param add is true to add the global and local thresholds together, otherwise favor the local thresholds
     * @return the combined thresholds
     */
    private static Set<Threshold> getCombinedThresholds( Set<Threshold> globalThresholds,
                                                         Set<Threshold> localThresholds,
                                                         boolean add )
    {
        Set<Threshold> combined = new HashSet<>();

        if ( add )
        {
            if ( Objects.nonNull( globalThresholds ) )
            {
                combined.addAll( globalThresholds );
            }

            if ( Objects.nonNull( localThresholds ) )
            {
                combined.addAll( localThresholds );
            }
        }
        else
        {
            if ( Objects.nonNull( localThresholds )
                 && !localThresholds.isEmpty() )
            {
                combined.addAll( localThresholds );
            }
            else if ( Objects.nonNull( globalThresholds ) )
            {
                combined.addAll( globalThresholds );
            }
        }

        // Mutable
        return combined;
    }

    /**
     * Interpolates featureful thresholds from the supplied thresholds. Each threshold that is not currently associated
     * with a feature is repeated as many times as features and assigned the feature as an index.
     *
     * @param thresholds the thresholds
     * @return the featureful thresholds
     */

    private static Set<Threshold> getFeatureFulThresholds( Set<Threshold> thresholds,
                                                           EvaluationDeclarationBuilder declaration )
    {
        // Get all features associated with the embryonic declaration
        Set<GeometryTuple> features = DeclarationUtilities.getFeatures( declaration.build() );

        // No features, return the thresholds as-is
        if ( features.isEmpty() )
        {
            return thresholds;
        }

        Set<Threshold> featurefulThresholds = new HashSet<>();
        for ( Threshold next : thresholds )
        {
            // Already featureful
            if ( Objects.nonNull( next.feature() ) )
            {
                featurefulThresholds.add( next );
            }
            // Add the threshold for each feature
            else
            {
                Set<Threshold> featureful = DeclarationInterpolator.getThresholdForEachFeature( next, features );
                featurefulThresholds.addAll( featureful );
            }
        }

        return Collections.unmodifiableSet( featurefulThresholds );
    }

    /**
     * Creates a threshold from the base threshold for each feature in the supplied set of features.
     * @param baseThreshold the base threshold
     * @param features the features
     * @return the thresholds
     */
    private static Set<Threshold> getThresholdForEachFeature( Threshold baseThreshold, Set<GeometryTuple> features )
    {
        Set<Threshold> thresholds = new HashSet<>();
        for ( GeometryTuple next : features )
        {
            Geometry feature = null;
            DatasetOrientation orientation = null;

            // Find a feature and associated orientation, beginning with the left, then right, then baseline
            if ( next.hasLeft() )
            {
                feature = next.getLeft();
                orientation = DatasetOrientation.LEFT;
            }
            else if ( next.hasRight() )
            {
                feature = next.getRight();
                orientation = DatasetOrientation.RIGHT;
            }
            else if ( next.hasBaseline() )
            {
                feature = next.getBaseline();
                orientation = DatasetOrientation.BASELINE;
            }
            Threshold newThreshold = ThresholdBuilder.builder( baseThreshold )
                                                     .feature( feature )
                                                     .featureNameFrom( orientation )
                                                     .build();
            thresholds.add( newThreshold );
        }

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Interpolates the observed data type.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateObservedDataType( EvaluationDeclarationBuilder builder,
                                                                            DataType ingestedDataType )
    {
        Dataset observed = builder.left();
        DatasetBuilder datasetBuilder = DatasetBuilder.builder( observed );
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Interpolate the left or observed data type when undeclared
        if ( Objects.isNull( observed.type() ) )
        {
            List<EvaluationStatusEvent> undeclared =
                    DeclarationInterpolator.interpolateObservedDataTypeWhenUndeclared( builder,
                                                                                       datasetBuilder,
                                                                                       ingestedDataType,
                                                                                       DatasetOrientation.LEFT );
            events.addAll( undeclared );
        }
        // Interpolate the left or observed data type when explicitly declared
        else
        {
            List<EvaluationStatusEvent> declared =
                    DeclarationInterpolator.interpolateObservedDataTypeWhenDeclared( datasetBuilder,
                                                                                     ingestedDataType,
                                                                                     DatasetOrientation.LEFT );
            events.addAll( declared );
        }

        // Set the adjusted dataset
        builder.left( datasetBuilder.build() );

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the data type for each covariate dataset.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateCovariatesDataType( EvaluationDeclarationBuilder builder,
                                                                              DataType ingestedDataType )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        List<CovariateDataset> adjustedCovariates = new ArrayList<>();
        for ( CovariateDataset covariateDataset : builder.covariates() )
        {
            Dataset covariate = covariateDataset.dataset();
            DatasetBuilder datasetBuilder = DatasetBuilder.builder( covariate );

            // Interpolate the type when undeclared
            if ( Objects.isNull( covariate.type() ) )
            {
                List<EvaluationStatusEvent> undeclared =
                        DeclarationInterpolator.interpolateObservedDataTypeWhenUndeclared( builder,
                                                                                           datasetBuilder,
                                                                                           ingestedDataType,
                                                                                           DatasetOrientation.COVARIATE );
                events.addAll( undeclared );
            }
            // Interpolate the type when explicitly declared
            else
            {
                List<EvaluationStatusEvent> declared =
                        DeclarationInterpolator.interpolateObservedDataTypeWhenDeclared( datasetBuilder,
                                                                                         ingestedDataType,
                                                                                         DatasetOrientation.COVARIATE );
                events.addAll( declared );
            }

            CovariateDatasetBuilder covariateBuilder = CovariateDatasetBuilder.builder( covariateDataset )
                                                                              .dataset( datasetBuilder.build() );
            adjustedCovariates.add( covariateBuilder.build() );
        }

        // Set the adjusted covariates
        builder.covariates( adjustedCovariates );

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the data type for observation-like datasets when there is explicit declaration of the data type. By
     * default, uses the declared type, but also checks against the data type inferred from ingest.
     * @param datasetBuilder the dataset builder
     * @param ingestedDataType the data type inferred from ingest
     * @param orientation the dataset orientation
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateObservedDataTypeWhenDeclared( DatasetBuilder datasetBuilder,
                                                                                        DataType ingestedDataType,
                                                                                        DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( ingestedDataType )
             && ingestedDataType != datasetBuilder.type() )
        {
            String article = "the";
            if ( orientation == DatasetOrientation.COVARIATE )
            {
                article = "a";
            }

            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                           .setEventMessage( THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                             + "for "
                                                             + article
                                                             + "'"
                                                             + orientation
                                                             + DATASET_WAS
                                                             + ingestedDataType
                                                             + BUT_THE_DATA_TYPE_WAS_EXPLICITLY_DECLARED_AS
                                                             + datasetBuilder.type()
                                                             + THE_EVALUATION_WILL_PROCEED_WITH_THE_EXPLICITLY
                                                             + DECLARED_TYPE_OF
                                                             + datasetBuilder.type()
                                                             + IF_THIS_IS_INCORRECT_PLEASE_FIX_THE_DECLARED
                                                             + TYPE )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the data type for observation-like datasets when there is no explicit declaration of the data type.
     * @param builder the evaluation builder
     * @param datasetBuilder the dataset builder
     * @param ingestedDataType the data type inferred from ingest
     * @param orientation the dataset orientation
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateObservedDataTypeWhenUndeclared( EvaluationDeclarationBuilder builder,
                                                                                          DatasetBuilder datasetBuilder,
                                                                                          DataType ingestedDataType,
                                                                                          DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        String article = "the";
        String extra = "";
        if ( orientation == DatasetOrientation.COVARIATE )
        {
            article = "a";
            if ( Objects.nonNull( datasetBuilder.variable() ) )
            {
                extra = "with a variable name of '" + datasetBuilder.variable()
                                                                    .name()
                        + "' ";
            }
        }

        String defaultStartMessage = "Discovered that "
                                     + article
                                     + " '"
                                     + orientation
                                     + "' dataset "
                                     + extra
                                     + "has no declared data 'type'.";
        String defaultEndMessage = "If this is incorrect, please declare the 'type' explicitly.";

        DataType calculatedDataType;

        // Analysis durations present? If so, assume analyses, unless there is an analysis interface declared for
        // another side of data
        if ( DeclarationUtilities.hasAnalysisTimes( builder )
             && DeclarationInterpolator.noAnalysisInterface( builder.right() )
             && ( !DeclarationUtilities.hasBaseline( builder )
                  || DeclarationInterpolator.noAnalysisInterface( builder.baseline()
                                                                         .dataset() ) ) )
        {
            calculatedDataType = DataType.ANALYSES;

            // Warn
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                           .setEventMessage( defaultStartMessage
                                                             + " When inspecting the declaration alone, inferred that "
                                                             + "the 'type' is 'analyses' because analysis durations "
                                                             + "were declared and analyses are typically used to "
                                                             + "verify other datasets. "
                                                             + defaultEndMessage )
                                           .build();
            events.add( event );
        }
        else
        {
            calculatedDataType = DataType.OBSERVATIONS;

            // Warn
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                           .setEventMessage( defaultStartMessage
                                                             + " Provisionally assuming that the 'type' is "
                                                             + "'observations'. This assumption may be adjusted if the "
                                                             + "'type' can be inferred from the time-series data. "
                                                             + defaultEndMessage )
                                           .build();
            events.add( event );
        }

        // The data type to use
        DataType typeToUse = calculatedDataType;

        // Is the ingested data type known?
        if ( Objects.nonNull( ingestedDataType ) )
        {
            // Is it consistent with the type inferred from the declaration? If not, we only emit an error if the
            // type inferred from the declaration is ANALYSES because this requires definitive/unique declaration
            // options. Otherwise, we emit a warning.
            if ( ingestedDataType != calculatedDataType
                 && calculatedDataType == DataType.ANALYSES )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                               .setEventMessage( THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                                 + "for "
                                                                 + article
                                                                 + " '"
                                                                 + orientation
                                                                 + DATASET_WAS
                                                                 + ingestedDataType
                                                                 + BUT_THE_DATA_TYPE_INFERRED_FROM_THE
                                                                 + DECLARATION_WAS
                                                                 + calculatedDataType
                                                                 + WHICH_IS_INCONSISTENT_PLEASE_FIX_THE
                                                                 + DECLARATION_HINT_LOOK_FOR_NEARBY_WARNING
                                                                 + MESSAGES_THAT_INDICATE_WHY_THE_DATA_TYPE
                                                                 + INFERRED_FROM_THE_DECLARATION_WAS
                                                                 + calculatedDataType
                                                                 + "'." )
                                               .build();
                events.add( event );
            }
            else if ( ingestedDataType != calculatedDataType )
            {
                typeToUse = ingestedDataType;

                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                               .setEventMessage( THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                                 + "for "
                                                                 + article
                                                                 + "'"
                                                                 + orientation
                                                                 + DATASET_WAS
                                                                 + ingestedDataType
                                                                 + BUT_THE_DATA_TYPE_INFERRED_FROM_THE
                                                                 + DECLARATION_WAS
                                                                 + calculatedDataType
                                                                 + THE_EVALUATION_WILL_PROCEED_WITH_THE_DATA
                                                                 + TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA_WHICH
                                                                 + "is '"
                                                                 + ingestedDataType
                                                                 + IF_THIS_IS_INCORRECT_PLEASE_DECLARE_THE
                                                                 + INTENDED_TYPE_EXPLICITLY )
                                               .build();
                events.add( event );
            }
        }

        // Adjust the type
        datasetBuilder.type( typeToUse );

        return Collections.unmodifiableList( events );
    }

    /**
     * @param dataset the dataset
     * @return whether the dataset has an analysis interface
     */

    private static boolean noAnalysisInterface( Dataset dataset )
    {
        if ( Objects.isNull( dataset ) )
        {
            return true;
        }

        return dataset.sources()
                      .stream()
                      .noneMatch( s -> Objects.nonNull( s.sourceInterface() )
                                       && s.sourceInterface()
                                           .isAnalysisInterface() );
    }

    /**
     * Interpolates the predicted data type.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolatePredictedDataType( EvaluationDeclarationBuilder builder,
                                                                             DataType ingestedDataType )
    {
        Dataset predicted = builder.right();

        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Interpolate the right or predicted data type when undeclared
        if ( Objects.isNull( predicted.type() ) )
        {
            List<EvaluationStatusEvent> undeclared =
                    DeclarationInterpolator.interpolatePredictedDataTypeWhenUndeclared( builder,
                                                                                        ingestedDataType );
            events.addAll( undeclared );
        }
        // Interpolate the right or predicted data type when explicitly declared
        else
        {
            List<EvaluationStatusEvent> declared =
                    DeclarationInterpolator.interpolatePredictedDataTypeWhenDeclared( builder,
                                                                                      ingestedDataType );
            events.addAll( declared );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the predicted data type when the type is undeclared.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolatePredictedDataTypeWhenUndeclared( EvaluationDeclarationBuilder builder,
                                                                                           DataType ingestedDataType )
    {
        Dataset predicted = builder.right();
        List<EvaluationStatusEvent> events = new ArrayList<>();

        String defaultStartMessage = "Discovered that the 'predicted' dataset has no declared data 'type'. ";
        String defaultEndMessage = " If this is incorrect, please declare the 'type' explicitly.";

        String reasonMessage;

        DataType calculatedDataType;

        // Discover hints from the declaration
        Set<String> ensembleDeclaration = DeclarationUtilities.getEnsembleDeclaration( builder );
        Set<String> forecastDeclaration = DeclarationUtilities.getForecastDeclaration( builder );

        // Ensemble declaration?
        if ( !ensembleDeclaration.isEmpty() )
        {
            reasonMessage = "Inferred a 'type' of 'ensemble forecasts' because the following ensemble "
                            + "declaration was discovered: " + ensembleDeclaration + ".";
            calculatedDataType = DataType.ENSEMBLE_FORECASTS;
        }
        // Forecast declaration?
        else if ( !forecastDeclaration.isEmpty() )
        {
            reasonMessage = "Inferred a 'type' of 'single valued forecasts' because the following forecast "
                            + "declaration was discovered and no ensemble declaration was discovered to suggest "
                            + "that the forecasts are 'ensemble forecasts': " + forecastDeclaration + ".";
            calculatedDataType = DataType.SINGLE_VALUED_FORECASTS;
        }
        // Source declaration that refers to a service that delivers multiple data types? If so, cannot infer type
        else if ( predicted.sources()
                           .stream()
                           .anyMatch( next -> Objects.nonNull( next.sourceInterface() )
                                              && next.sourceInterface()
                                                     .getDataTypes()
                                                     .size() > 1 ) )
        {
            reasonMessage = "Could not infer the 'predicted' data type because sources were declared with "
                            + "interfaces that support multiple data types.";
            calculatedDataType = null;
        }
        else
        {
            reasonMessage = "Inferred a 'type' of 'simulations' because no declaration was discovered to "
                            + "suggest that any dataset contains 'single valued forecasts' or 'ensemble "
                            + "forecast'.";

            calculatedDataType = DataType.SIMULATIONS;
        }

        // Warn
        EvaluationStatusEvent declarationEvent
                = EvaluationStatusEvent.newBuilder()
                                       .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                       .setEventMessage( defaultStartMessage
                                                         + reasonMessage
                                                         + defaultEndMessage )
                                       .build();
        events.add( declarationEvent );

        // The data type to use
        DataType typeToUse = calculatedDataType;

        // Is the ingested data type known?
        if ( Objects.nonNull( ingestedDataType ) )
        {
            // Is it consistent with the type inferred from the declaration? If not, we only emit an error if the
            // type inferred from the declaration is ENSEMBLE_FORECASTS because this requires definitive/unique
            // declaration options. Otherwise, we emit a warning.
            if ( ingestedDataType != calculatedDataType
                 && calculatedDataType == DataType.ENSEMBLE_FORECASTS )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                               .setEventMessage( THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                                 + FOR_THE_PREDICTED_DATASET_WAS
                                                                 + ingestedDataType
                                                                 + BUT_THE_DATA_TYPE_INFERRED_FROM_THE
                                                                 + DECLARATION_WAS
                                                                 + calculatedDataType
                                                                 + WHICH_IS_INCONSISTENT_PLEASE_FIX_THE
                                                                 + DECLARATION_HINT_LOOK_FOR_NEARBY_WARNING
                                                                 + MESSAGES_THAT_INDICATE_WHY_THE_DATA_TYPE
                                                                 + INFERRED_FROM_THE_DECLARATION_WAS
                                                                 + calculatedDataType
                                                                 + "'." )
                                               .build();
                events.add( event );
            }
            else if ( ingestedDataType != calculatedDataType )
            {
                typeToUse = ingestedDataType;

                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                               .setEventMessage( THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                                 + FOR_THE_PREDICTED_DATASET_WAS
                                                                 + ingestedDataType
                                                                 + BUT_THE_DATA_TYPE_INFERRED_FROM_THE
                                                                 + DECLARATION_WAS
                                                                 + calculatedDataType
                                                                 + THE_EVALUATION_WILL_PROCEED_WITH_THE_DATA
                                                                 + TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA_WHICH
                                                                 + "is '"
                                                                 + ingestedDataType
                                                                 + IF_THIS_IS_INCORRECT_PLEASE_DECLARE_THE
                                                                 + INTENDED_TYPE_EXPLICITLY )
                                               .build();
                events.add( event );
            }
        }

        // Set the type
        Dataset newPredicted = DatasetBuilder.builder( predicted )
                                             .type( typeToUse )
                                             .build();
        builder.right( newPredicted );

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the predicted data type when there is explicit declaration for a predicted data type. By default,
     * uses the declared type, but also checks against the data type inferred from ingest.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolatePredictedDataTypeWhenDeclared( EvaluationDeclarationBuilder builder,
                                                                                         DataType ingestedDataType )
    {
        Dataset predicted = builder.right();
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( ingestedDataType )
             && ingestedDataType != predicted.type() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                           .setEventMessage( THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                             + FOR_THE_PREDICTED_DATASET_WAS
                                                             + ingestedDataType
                                                             + BUT_THE_DATA_TYPE_WAS_EXPLICITLY_DECLARED_AS
                                                             + predicted.type()
                                                             + THE_EVALUATION_WILL_PROCEED_WITH_THE_EXPLICITLY
                                                             + DECLARED_TYPE_OF
                                                             + predicted.type()
                                                             + IF_THIS_IS_INCORRECT_PLEASE_FIX_THE_DECLARED
                                                             + TYPE )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the baseline data type.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateBaselineDataType( EvaluationDeclarationBuilder builder,
                                                                            DataType ingestedDataType )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( DeclarationUtilities.hasBaseline( builder ) )
        {
            BaselineDataset baseline = builder.baseline();
            Dataset baselineDataset = baseline.dataset();

            // Interpolate the baseline data type when undeclared
            if ( Objects.isNull( baselineDataset.type() ) )
            {
                List<EvaluationStatusEvent> undeclared =
                        DeclarationInterpolator.interpolateBaselineDataTypeWhenUndeclared( builder,
                                                                                           ingestedDataType );
                events.addAll( undeclared );
            }
            // Interpolate the baseline data type when explicitly declared
            else
            {
                List<EvaluationStatusEvent> declared =
                        DeclarationInterpolator.interpolateBaselineDataTypeWhenDeclared( builder,
                                                                                         ingestedDataType );
                events.addAll( declared );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the baseline data type when there is no declaration of the baseline data type.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateBaselineDataTypeWhenUndeclared( EvaluationDeclarationBuilder builder,
                                                                                          DataType ingestedDataType )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        BaselineDataset baseline = builder.baseline();
        Dataset baselineDataset = baseline.dataset();

        Dataset predicted = builder.right();

        // Same as the predicted data type, by default
        DataType calculatedDataType = predicted.type();

        String reason = "Inferred a 'type' of '"
                        + calculatedDataType
                        + "' to match the 'type' of the 'predicted' dataset.";

        // Generated baseline defined? If so, observations
        if ( Objects.nonNull( baseline.generatedBaseline() ) )
        {
            calculatedDataType = DataType.OBSERVATIONS;
            reason = "Inferred a 'type' of 'observations' because a generated baseline was defined and all "
                     + "supported baseline types require observation-like data.";
        }

        // Warn
        EvaluationStatusEvent declaredEvent
                = EvaluationStatusEvent.newBuilder()
                                       .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                       .setEventMessage( "Discovered that the 'baseline' dataset has no "
                                                         + "declared data 'type'. "
                                                         + reason
                                                         + " If this is incorrect, please declare the 'type' "
                                                         + "explicitly." )
                                       .build();
        events.add( declaredEvent );

        // The data type to use
        DataType typeToUse = calculatedDataType;

        // Is the ingested data type known?
        if ( Objects.nonNull( ingestedDataType ) )
        {
            // Is it consistent with the type inferred from the declaration? If not, we only emit an error if
            // the type inferred from the declaration is ENSEMBLE_FORECASTS because this requires
            // definitive/unique declaration options. Otherwise, we emit a warning.
            if ( ingestedDataType != calculatedDataType && calculatedDataType == DataType.ENSEMBLE_FORECASTS )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                               .setEventMessage(
                                                       THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                       + "for the 'baseline' dataset was '"
                                                       + ingestedDataType
                                                       + BUT_THE_DATA_TYPE_INFERRED_FROM_THE
                                                       + DECLARATION_WAS
                                                       + calculatedDataType
                                                       + WHICH_IS_INCONSISTENT_PLEASE_FIX_THE
                                                       + DECLARATION_HINT_LOOK_FOR_NEARBY_WARNING
                                                       + MESSAGES_THAT_INDICATE_WHY_THE_DATA_TYPE
                                                       + INFERRED_FROM_THE_DECLARATION_WAS
                                                       + calculatedDataType
                                                       + "'." )
                                               .build();
                events.add( event );
            }
            else if ( ingestedDataType != calculatedDataType )
            {
                typeToUse = ingestedDataType;

                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                               .setEventMessage(
                                                       THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                       + "for the 'baseline' dataset was '"
                                                       + ingestedDataType
                                                       + BUT_THE_DATA_TYPE_INFERRED_FROM_THE
                                                       + DECLARATION_WAS
                                                       + calculatedDataType
                                                       + THE_EVALUATION_WILL_PROCEED_WITH_THE_DATA
                                                       + TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA_WHICH
                                                       + "is '"
                                                       + ingestedDataType
                                                       + IF_THIS_IS_INCORRECT_PLEASE_DECLARE_THE
                                                       + INTENDED_TYPE_EXPLICITLY )
                                               .build();
                events.add( event );
            }
        }

        Dataset newBaselineDataset = DatasetBuilder.builder( baselineDataset )
                                                   .type( typeToUse )
                                                   .build();
        BaselineDataset newBaseline =
                BaselineDatasetBuilder.builder( baseline )
                                      .dataset( newBaselineDataset )
                                      .build();
        builder.baseline( newBaseline );

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the baseline data type when there is explicit declaration for a baseline data type. By default,
     * uses the declared type, but also checks against the data type inferred from ingest.
     * @param builder the builder
     * @param ingestedDataType the data type inferred from ingest
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateBaselineDataTypeWhenDeclared( EvaluationDeclarationBuilder builder,
                                                                                        DataType ingestedDataType )
    {
        BaselineDataset baseline = builder.baseline();
        Dataset baselineDataset = baseline.dataset();
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( ingestedDataType )
             && ingestedDataType != baselineDataset.type() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                           .setEventMessage( THE_DATA_TYPE_INFERRED_FROM_THE_TIME_SERIES_DATA
                                                             + "for the baseline dataset was '"
                                                             + ingestedDataType
                                                             + BUT_THE_DATA_TYPE_WAS_EXPLICITLY_DECLARED_AS
                                                             + baselineDataset.type()
                                                             + THE_EVALUATION_WILL_PROCEED_WITH_THE_EXPLICITLY
                                                             + DECLARED_TYPE_OF
                                                             + baselineDataset.type()
                                                             + IF_THIS_IS_INCORRECT_PLEASE_FIX_THE_DECLARED
                                                             + TYPE )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Determines whether the specified {@link Dataset} is a special case for data type interpolation, notably whether
     * the data source involves reading from a web service that serves multiple data types for which the type must be
     * declared as part of the service call. See GitHub #384.
     *
     * @see #interpolateDataTypeForSpecialCase(EvaluationDeclarationBuilder, DatasetBuilder, DatasetOrientation, SourceInterface)
     * @param dataset the dataset
     * @return whether the data type interpolation is a special case
     */

    private static boolean isSpecialCaseForDataTypeInterpolation( Dataset dataset )
    {
        Objects.requireNonNull( dataset );
        return Objects.isNull( dataset.type() )
               && dataset.sources()
                         .stream()
                         .anyMatch( s -> s.sourceInterface() == SourceInterface.WRDS_AHPS
                                         || s.sourceInterface() == SourceInterface.WRDS_NWM );
    }

    /**
     * Interpolates the data type for special cases. See GitHub #384.
     *
     * @see #isSpecialCaseForDataTypeInterpolation(Dataset)
     * @param builder the evaluation builder
     * @param datasetBuilder the dataset builder to adjust
     * @param orientation the dataset orientation to help qualify any warnings
     * @param sourceInterface the data source interface
     * @return any interpolation events encountered
     */

    private static List<EvaluationStatusEvent> interpolateDataTypeForSpecialCase( EvaluationDeclarationBuilder builder,
                                                                                  DatasetBuilder datasetBuilder,
                                                                                  DatasetOrientation orientation,
                                                                                  SourceInterface sourceInterface )
    {
        // Double-check that this is a special case
        if ( !DeclarationInterpolator.isSpecialCaseForDataTypeInterpolation( datasetBuilder.build() ) )
        {
            throw new IllegalArgumentException( "Could not interpolate the data type for a special case because the "
                                                + "dataset does not involve a special case." );
        }
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Always assume single-valued forecasts, but adjust the warning depending on whether forecast declaration is
        // present
        Set<String> forecast = DeclarationUtilities.getForecastDeclaration( builder.build() );

        String qualify = "When interpolating the data 'type' for the '"
                         + orientation
                         + "' dataset, encountered a data 'source' with an 'interface' of '"
                         + sourceInterface
                         + "'. Assuming that 'single valued forecasts' are required. If this is wrong, please declare "
                         + "the data 'type' explicitly.";

        if ( !forecast.isEmpty() )
        {
            qualify = "When interpolating the data 'type' for the '"
                      + orientation
                      + "' dataset, encountered a data 'source' with an 'interface' of '"
                      + sourceInterface
                      + "'. As forecast declaration was detected, the assumed data 'type' is "
                      + "'single valued forecasts'. If this is wrong, please declare the data 'type' explicitly. The "
                      + "forecast declaration was: "
                      + forecast;
        }

        datasetBuilder.type( DataType.SINGLE_VALUED_FORECASTS );

        EvaluationStatusEvent event =
                EvaluationStatusEvent.newBuilder()
                                     .setStatusLevel( EvaluationStatusEvent.StatusLevel.WARN )
                                     .setEventMessage( qualify )
                                     .build();
        events.add( event );

        return Collections.unmodifiableList( events );
    }

    /**
     * Fills any sparse geometries. Only use this method when the feature authorities match on all sides and only
     * supply this method with sparse geometries.
     *
     * @param geometries the geometries to fill
     * @param hasBaseline whether a baseline has been declared
     * @return the geometries with any partially declared geometries filled, where applicable
     */

    private static Set<GeometryTuple> interpolateSparseFeatures( Collection<GeometryTuple> geometries,
                                                                 boolean hasBaseline )
    {
        // Preserve insertion order
        Set<GeometryTuple> adjustedGeometries = new LinkedHashSet<>();
        for ( GeometryTuple next : geometries )
        {
            GeometryTuple.Builder adjusted = next.toBuilder();
            Set<Geometry> tuple = new HashSet<>();

            // Find the dense geometry
            if ( next.hasLeft() )
            {
                tuple.add( next.getLeft() );
            }
            if ( next.hasRight() )
            {
                tuple.add( next.getRight() );
            }
            if ( next.hasBaseline() )
            {
                tuple.add( next.getBaseline() );
            }

            // Check that there is one true geometry
            if ( tuple.size() == 1 )
            {
                Geometry singleton = tuple.iterator()
                                          .next();

                DeclarationInterpolator.interpolateSparseFeatures( adjusted, singleton, hasBaseline );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Failed to densify geometry tuple {} because there was not exactly one common geometry "
                              + "to copy.", DeclarationFactory.PROTBUF_STRINGIFIER.apply( next ) );
            }

            adjustedGeometries.add( adjusted.build() );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            Features features = FeaturesBuilder.builder()
                                               .geometries( adjustedGeometries )
                                               .build();

            LOGGER.debug( "Interpolated the following geometries: {}.", features );
        }

        return Collections.unmodifiableSet( adjustedGeometries );
    }

    /**
     * Checks that each side of data has a matching, non-null feature authority if the datasets are non-null.
     * @param builder the builder
     * @return whether there is a common feature authority
     */
    private static boolean hasMatchingFeatureAuthorities( EvaluationDeclarationBuilder builder )
    {
        return Objects.nonNull( builder.left() )
               && Objects.nonNull( builder.right() )
               && Objects.nonNull( builder.left()
                                          .featureAuthority() )
               && Objects.equals( builder.left()
                                         .featureAuthority(),
                                  builder.right()
                                         .featureAuthority() )
               && ( !DeclarationUtilities.hasBaseline( builder )
                    || Objects.equals( builder.left()
                                              .featureAuthority(),
                                       builder.baseline()
                                              .dataset()
                                              .featureAuthority() ) );
    }

    /**
     * Interpolates any missing features from a dense feature.
     * @param sparse the geometry tuple with potentially sparse geometries
     * @param dense the dense feature
     * @param hasBaseline whether the feature requires a baseline
     */

    private static void interpolateSparseFeatures( GeometryTuple.Builder sparse,
                                                   Geometry dense,
                                                   boolean hasBaseline )
    {
        // Only set the geometries that are actually sparse
        if ( !sparse.hasLeft() )
        {
            sparse.setLeft( dense );
        }

        if ( !sparse.hasRight() )
        {
            sparse.setRight( dense );
        }

        if ( !sparse.hasBaseline() && hasBaseline )
        {
            sparse.setBaseline( dense );
        }
    }

    /**
     * Returns the features from the thresholds if they are not already contained in the set of declared features.
     * @param declared the declared features
     * @param featureful the featureful thresholds
     * @param orientation the orientation from which to obtain feature names
     * @return the features not already declared
     */

    private static Set<GeometryTuple> getFeaturesNotAlreadyDeclared( Set<GeometryTuple> declared,
                                                                     Set<Threshold> featureful,
                                                                     DatasetOrientation orientation )
    {
        // Get the sided features declared for thresholds
        Set<Geometry> thresholdFeatures = featureful.stream()
                                                    .filter( n -> n.featureNameFrom() == orientation )
                                                    .map( Threshold::feature )
                                                    .collect( Collectors.toSet() );

        Set<GeometryTuple> notDeclared = new HashSet<>();

        Set<String> names = DeclarationUtilities.getFeatureNamesFor( declared, orientation );

        for ( Geometry next : thresholdFeatures )
        {
            if ( !names.contains( next.getName() ) )
            {
                GeometryTuple tuple = DeclarationInterpolator.getFeatureTupleFrom( next, orientation );
                notDeclared.add( tuple );
            }
        }

        return Collections.unmodifiableSet( notDeclared );
    }

    /**
     * Creates a feature tuple from the geometry and orientation.
     * @param feature the feature
     * @param orientation the orientation
     * @return the feature tuple
     */
    private static GeometryTuple getFeatureTupleFrom( Geometry feature, DatasetOrientation orientation )
    {
        return switch ( orientation )
        {
            case LEFT -> GeometryTuple.newBuilder()
                                      .setLeft( feature )
                                      .build();
            case RIGHT -> GeometryTuple.newBuilder()
                                       .setRight( feature )
                                       .build();
            case BASELINE -> GeometryTuple.newBuilder()
                                          .setBaseline( feature )
                                          .build();
            default -> throw new IllegalArgumentException( "Unrecognized dataset orientation in this context: "
                                                           + orientation );
        };
    }

    /**
     * Interpolates the variable names using the supplied input.
     *
     * @param builder the declaration builder to adjust
     * @param variableNames the variable names
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateVariableNames( EvaluationDeclarationBuilder builder,
                                                                         VariableNames variableNames )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Interpolate the name for the left dataset
        if ( Objects.nonNull( builder.left() ) )
        {
            String oldLeftName = null;
            if ( DeclarationInterpolator.hasVariableName( builder.left() ) )
            {
                oldLeftName = builder.left()
                                     .variable()
                                     .name();
            }

            DatasetBuilder leftBuilder = DatasetBuilder.builder( builder.left() );
            VariableBuilder leftVariable = VariableBuilder.builder();
            if ( Objects.nonNull( builder.left()
                                         .variable() ) )
            {
                leftVariable = VariableBuilder.builder( builder.left()
                                                               .variable() );
            }

            Set<String> newName = DeclarationInterpolator.getSingletonOrEmptySet( variableNames.leftVariableName() );
            List<EvaluationStatusEvent> leftEvents =
                    DeclarationInterpolator.interpolateVariableNames( oldLeftName,
                                                                      newName,
                                                                      leftVariable::name,
                                                                      DatasetOrientation.LEFT );
            leftBuilder.variable( leftVariable.build() );
            builder.left( leftBuilder.build() );
            events.addAll( leftEvents );
        }

        // Interpolate the name for the right dataset
        if ( Objects.nonNull( builder.right() ) )
        {
            String oldRightName = null;
            if ( DeclarationInterpolator.hasVariableName( builder.right() ) )
            {
                oldRightName = builder.right()
                                      .variable()
                                      .name();
            }

            DatasetBuilder rightBuilder = DatasetBuilder.builder( builder.right() );
            VariableBuilder rightVariable = VariableBuilder.builder();
            if ( Objects.nonNull( builder.right()
                                         .variable() ) )
            {
                rightVariable = VariableBuilder.builder( builder.right()
                                                                .variable() );
            }

            Set<String> newName = DeclarationInterpolator.getSingletonOrEmptySet( variableNames.rightVariableName() );
            List<EvaluationStatusEvent> rightEvents =
                    DeclarationInterpolator.interpolateVariableNames( oldRightName,
                                                                      newName,
                                                                      rightVariable::name,
                                                                      DatasetOrientation.RIGHT );
            rightBuilder.variable( rightVariable.build() );
            builder.right( rightBuilder.build() );

            // Add the right events
            events.addAll( rightEvents );
        }

        // Interpolate the name for the baseline dataset, where present
        if ( Objects.nonNull( builder.baseline() ) )
        {
            String oldBaselineName = null;
            if ( DeclarationInterpolator.hasVariableName( builder.baseline()
                                                                 .dataset() ) )
            {
                oldBaselineName = builder.baseline()
                                         .dataset()
                                         .variable()
                                         .name();
            }

            DatasetBuilder baselineDatasetBuilder = DatasetBuilder.builder( builder.baseline()
                                                                                   .dataset() );
            VariableBuilder baselineVariable = VariableBuilder.builder();
            if ( Objects.nonNull( builder.baseline()
                                         .dataset()
                                         .variable() ) )
            {
                baselineVariable = VariableBuilder.builder( builder.baseline()
                                                                   .dataset()
                                                                   .variable() );
            }

            BaselineDatasetBuilder baselineBuilder = BaselineDatasetBuilder.builder( builder.baseline() );

            Set<String> newName =
                    DeclarationInterpolator.getSingletonOrEmptySet( variableNames.baselineVariableName() );
            List<EvaluationStatusEvent> baselineEvents =
                    DeclarationInterpolator.interpolateVariableNames( oldBaselineName,
                                                                      newName,
                                                                      baselineVariable::name,
                                                                      DatasetOrientation.BASELINE );
            baselineDatasetBuilder.variable( baselineVariable.build() );
            baselineBuilder.dataset( baselineDatasetBuilder.build() );
            builder.baseline( baselineBuilder.build() );

            // Add the baseline events
            events.addAll( baselineEvents );
        }

        // Covariate dataset(s)?
        List<EvaluationStatusEvent> covariateEvents =
                DeclarationInterpolator.interpolateCovariateVariableName( builder,
                                                                          variableNames );
        events.addAll( covariateEvents );

        return Collections.unmodifiableList( events );
    }

    /**
     * Helper that returns a singleton if the input is available, otherwise an empty set.
     * @param variableName the variable name
     * @return a singleton or empty set
     */
    private static Set<String> getSingletonOrEmptySet( String variableName )
    {
        if ( Objects.isNull( variableName ) )
        {
            return Set.of();
        }

        return Set.of( variableName );
    }

    /**
     * Interpolates the covariate variable names using the supplied input.
     *
     * @param builder the declaration builder to adjust
     * @param variableNames the variable names
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateCovariateVariableName( EvaluationDeclarationBuilder builder,
                                                                                 VariableNames variableNames )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // No covariate dataset(s)?
        if ( builder.covariates()
                    .isEmpty() )
        {
            LOGGER.debug( "No covariates were discovered with variable names to interpolate." );
        }
        // If there is more than one covariate, all names must be declared
        else if ( builder.covariates()
                         .size() > 1 )
        {
            if ( builder.covariates()
                        .stream()
                        .anyMatch( c -> Objects.isNull( c.dataset()
                                                         .variable() )
                                        || Objects.isNull( c.dataset()
                                                            .variable()
                                                            .name() ) ) )
            {
                EvaluationStatusEvent error =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                             .setEventMessage(
                                                     "When declaring two or more 'covariates', the 'name' "
                                                     + "of each 'variable' must be declared explicitly, but "
                                                     + "one or more of the 'covariates' had no declared "
                                                     + "'variable' and 'name'. Please clarify the 'name' of "
                                                     + "the 'variable' for each covariate and try again." )
                                             .build();
                events.add( error );
            }
            else
            {
                LOGGER.debug( "Discovered multiple covariates, each with a declared variable named, so the variable "
                              + "names were not interpolated." );
            }
        }
        // If there is one covariate declared and multiple ingested names, the covariate name must be declared
        else if ( builder.covariates()
                         .size() == 1
                  && variableNames.covariateVariableNames()
                                  .size() > 1
                  && builder.covariates()
                            .stream()
                            .anyMatch( c -> Objects.isNull( c.dataset()
                                                             .variable() )
                                            || Objects.isNull( c.dataset()
                                                                .variable()
                                                                .name() ) ) )
        {
            EvaluationStatusEvent error =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                         .setEventMessage( "Discovered a 'covariate' dataset that contains more "
                                                           + "than one variable: "
                                                           + variableNames.covariateVariableNames()
                                                           + ". However, the declaration does not identify which "
                                                           + "of these variables should be used. Please declare "
                                                           + "the 'name' of the 'variable' to use and try again." )
                                         .build();
            events.add( error );
        }
        // Interpolate the name for the covariate dataset
        else
        {
            String oldCovariateName = null;
            if ( DeclarationInterpolator.hasVariableName( builder.covariates()
                                                                 .get( 0 )
                                                                 .dataset() ) )
            {
                oldCovariateName = builder.covariates()
                                          .get( 0 )
                                          .dataset()
                                          .variable()
                                          .name();
            }

            DatasetBuilder covariateDatasetBuilder = DatasetBuilder.builder( builder.covariates()
                                                                                    .get( 0 )
                                                                                    .dataset() );
            CovariateDatasetBuilder covariateBuilder = CovariateDatasetBuilder.builder( builder.covariates()
                                                                                               .get( 0 ) );
            VariableBuilder covariateVariable = VariableBuilder.builder();
            if ( Objects.nonNull( covariateDatasetBuilder
                                          .variable() ) )
            {
                covariateVariable = VariableBuilder.builder( covariateDatasetBuilder.variable() );
            }

            List<EvaluationStatusEvent> covariateEvents =
                    DeclarationInterpolator.interpolateVariableNames( oldCovariateName,
                                                                      variableNames.covariateVariableNames(),
                                                                      covariateVariable::name,
                                                                      DatasetOrientation.COVARIATE );
            covariateDatasetBuilder.variable( covariateVariable.build() );
            covariateBuilder.dataset( covariateDatasetBuilder.build() );
            builder.covariates( List.of( covariateBuilder.build() ) );

            // Add the covariate events
            events.addAll( covariateEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param dataset the dataset
     * @return whether the dataset has a variable name
     */
    private static boolean hasVariableName( Dataset dataset )
    {
        return Objects.nonNull( dataset )
               && Objects.nonNull( dataset.variable() )
               && Objects.nonNull( dataset.variable()
                                          .name() );
    }

    /**
     * Interpolates the variable names using the supplied input.
     *
     * @param oldName the existing variable name, possibly null
     * @param newNames the new variable name options
     * @param nameSetter the setter for the new name
     * @param orientation the dataset orientation
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateVariableNames( String oldName,
                                                                         Set<String> newNames,
                                                                         Consumer<String> nameSetter,
                                                                         DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( newNames.isEmpty() )
        {
            LOGGER.debug( "New new variable name to interpolate for the {} dataset.", orientation );
            return List.of();
        }
        else if ( Objects.isNull( oldName )
                  && newNames.size() == 1 )
        {
            nameSetter.accept( newNames.iterator()
                                       .next() );
        }
        else if ( Objects.nonNull( oldName )
                  && newNames.stream()
                             .noneMatch( oldName::equalsIgnoreCase ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                           .setEventMessage( "When attempting to set the variable name for the '"
                                                             + orientation
                                                             + "' dataset, found a declared variable name of '"
                                                             + oldName
                                                             + "', which is inconsistent with the variable names of '"
                                                             + newNames
                                                             + "' discovered in the time-series data source. Please "
                                                             + "omit the 'name' of the 'variable' for the '"
                                                             + orientation
                                                             + "' dataset from the declaration or use one of '"
                                                             + newNames
                                                             + "' for consistency with the data source." )
                                           .build();
            events.add( event );
        }
        // Set the name, which is the same as the old name, ignoring case. The reason to set here is to render the case
        // equivalent to the ingested name
        else
        {
            LOGGER.debug( "Found matching variable names in the declaration and data sources for the {} dataset.",
                          orientation );

            nameSetter.accept( newNames.iterator()
                                       .next() );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the measurement unit.
     * @param builder the evaluation builder
     * @param measurementUnit the measurement unit
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateMeasurementUnit( EvaluationDeclarationBuilder builder,
                                                                           String measurementUnit )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( Objects.isNull( builder.unit() ) )
        {
            LOGGER.debug( "Set the measurement unit to {}.", measurementUnit );
            builder.unit( measurementUnit );
        }
        else if ( !builder.unit()
                          .equalsIgnoreCase( measurementUnit ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                           .setEventMessage( "The declared measurement unit of '"
                                                             + builder.unit()
                                                             + "' is inconsistent with the measurement unit of '"
                                                             + measurementUnit
                                                             + "', which was analyzed from the source data. Please "
                                                             + "remove the 'unit' from the declaration or use the "
                                                             + "analyzed unit of '"
                                                             + measurementUnit
                                                             + "'." )
                                           .build();
            events.add( event );
        }
        // Set the unit, which is the same as the declared unit, ignoring case. The reason to set here is to render the
        // case equivalent to the analyzed unit
        else
        {
            LOGGER.debug( "Found declared and analyzed measurement units of '{}' are consistent.",
                          measurementUnit );

            builder.unit( measurementUnit );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Interpolates the evaluation timescale.
     * @param builder the evaluation builder
     * @param timeScale the evaluation timescale
     * @return any interpolation events encountered
     */
    private static List<EvaluationStatusEvent> interpolateTimeScale( EvaluationDeclarationBuilder builder,
                                                                     TimeScale timeScale )
    {
        if ( Objects.isNull( timeScale ) )
        {
            LOGGER.debug( "The analyzed timescale is unknown." );
            return List.of();
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( Objects.isNull( builder.timeScale() ) )
        {
            LOGGER.debug( "Set the measurement unit to {}.", timeScale );
            builder.timeScale( new wres.config.yaml.components.TimeScale( timeScale ) );
        }
        else if ( !builder.timeScale()
                          .timeScale()
                          .equals( timeScale ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                           .setEventMessage( "The declared evaluation timescale of '"
                                                             + builder.timeScale()
                                                             + "' is inconsistent with the timescale of '"
                                                             + timeScale
                                                             + "', which was analyzed from the source data. Please "
                                                             + "remove the 'time_scale' from the declaration or use "
                                                             + "the analyzed timescale of '"
                                                             + timeScale
                                                             + "'." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Hidden constructor.
     */
    private DeclarationInterpolator()
    {
    }
}
