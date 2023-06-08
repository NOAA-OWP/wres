package wres.config.yaml;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.DoubleValue;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.util.GeometricShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.config.yaml.DeclarationFactory.PROTBUF_STRINGIFIER;

import wres.config.MetricConstants;
import wres.config.xml.CsvThresholdReader;
import wres.config.xml.MetricConstantsFactory;
import wres.config.xml.ProjectConfigs;
import wres.config.xml.generated.Circle;
import wres.config.xml.generated.DataSourceBaselineConfig;
import wres.config.xml.generated.DataSourceConfig;
import wres.config.xml.generated.DatasourceType;
import wres.config.xml.generated.DateCondition;
import wres.config.xml.generated.DesiredTimeScaleConfig;
import wres.config.xml.generated.DestinationConfig;
import wres.config.xml.generated.DoubleBoundsType;
import wres.config.xml.generated.DurationBoundsType;
import wres.config.xml.generated.DurationUnit;
import wres.config.xml.generated.EnsembleCondition;
import wres.config.xml.generated.FeatureDimension;
import wres.config.xml.generated.FeatureGroup;
import wres.config.xml.generated.FeaturePool;
import wres.config.xml.generated.GraphicalType;
import wres.config.xml.generated.IntBoundsType;
import wres.config.xml.generated.LenienceType;
import wres.config.xml.generated.MetricConfigName;
import wres.config.xml.generated.MetricsConfig;
import wres.config.xml.generated.NamedFeature;
import wres.config.xml.generated.OutputTypeSelection;
import wres.config.xml.generated.PairConfig;
import wres.config.xml.generated.Polygon;
import wres.config.xml.generated.PoolingWindowConfig;
import wres.config.xml.generated.ProjectConfig;
import wres.config.xml.generated.ThresholdDataType;
import wres.config.xml.generated.ThresholdFormat;
import wres.config.xml.generated.ThresholdsConfig;
import wres.config.xml.generated.TimeScaleConfig;
import wres.config.xml.generated.TimeSeriesMetricConfigName;
import wres.config.xml.generated.UnnamedFeature;
import wres.config.xml.generated.UrlParameter;
import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.DecimalFormatPretty;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EnsembleFilterBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceBuilder;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Format;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.SpatialMaskBuilder;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.TimeScaleBuilder;
import wres.config.yaml.components.TimeScaleLenience;
import wres.config.yaml.components.UnitAlias;
import wres.config.yaml.components.Values;
import wres.config.yaml.components.Variable;
import wres.config.yaml.components.VariableBuilder;
import wres.config.yaml.deserializers.ZoneOffsetDeserializer;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;

/**
 * Class that helps migrate from the old-style XML declaration language to the new YAML language.
 *
 * @author James Brown
 */
public class DeclarationMigrator
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DeclarationMigrator.class );

    /** String representation of the default missing value in the old declaration language. */
    private static final String DEFAULT_MISSING_STRING_OLD = "-999.0";

    /** Default metric parameters. */
    private static final MetricParameters DEFAULT_METRIC_PARAMETERS = MetricParametersBuilder.builder()
                                                                                             .build();

    /**
     * Migrates from an old-style declaration POJO (unmarshalled from am XML string) to a new-style declaration POJO
     * (unmarshalled from a YAML string).
     *
     * @param projectConfig the old style declaration POJO
     * @param inlineThresholds is true to migrate any external source of CSV thresholds inline to the declaration,
     *                         false to migrate into a {@link ThresholdSource}
     * @return the new style declaration POJO
     * @throws NullPointerException if expected declaration is missing
     */

    public static EvaluationDeclaration from( ProjectConfig projectConfig, boolean inlineThresholds )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getInputs() );
        Objects.requireNonNull( projectConfig.getPair() );
        Objects.requireNonNull( projectConfig.getMetrics() );
        Objects.requireNonNull( projectConfig.getOutputs() );

        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder();

        // Migrate the project name
        DeclarationMigrator.migrateName( projectConfig, builder );
        // Migrate the datasets
        DeclarationMigrator.migrateDatasets( projectConfig.getInputs(), builder );
        // Migrate the features
        DeclarationMigrator.migrateFeatures( projectConfig.getPair()
                                                          .getFeature(), builder );
        // Migrate the feature groups
        DeclarationMigrator.migrateFeatureGroups( projectConfig.getPair()
                                                               .getFeatureGroup(), builder );
        // Migrate the feature service
        DeclarationMigrator.migrateFeatureService( projectConfig.getPair()
                                                                .getFeatureService(), builder );
        // Migrate the spatial mask
        DeclarationMigrator.migrateSpatialMask( projectConfig.getPair()
                                                             .getGridSelection(), builder );
        // Migrate all the time filters
        DeclarationMigrator.migrateTimeFilters( projectConfig.getPair(), builder );
        // Migrate the evaluation timescale
        DeclarationMigrator.migrateEvaluationTimeScale( projectConfig.getPair()
                                                                     .getDesiredTimeScale(), builder );
        // Migrate the units
        builder.unit( projectConfig.getPair()
                                   .getUnit() );
        // Migrate the unit aliases
        DeclarationMigrator.migrateUnitAliases( projectConfig.getPair()
                                                             .getUnitAlias(), builder );
        // Migrate cross-pairing
        DeclarationMigrator.migrateCrossPairing( projectConfig.getPair()
                                                              .getCrossPair(), builder );
        // Migrate the value filter
        DeclarationMigrator.migrateValueFilter( projectConfig.getPair()
                                                             .getValues(), builder );
        // Migrate the metrics and any associated thresholds
        DeclarationMigrator.migrateMetrics( projectConfig, builder, inlineThresholds );
        // Migrate the output formats
        DeclarationMigrator.migrateOutputFormats( projectConfig.getOutputs(), builder );

        return builder.build();
    }

    /**
     * Migrate the project name.
     *
     * @param projectConfig the project declaration
     * @param builder the new declaration builder
     */

    private static void migrateName( ProjectConfig projectConfig, EvaluationDeclarationBuilder builder )
    {
        String name = projectConfig.getName();
        // Set a non-null name that does not match the default in the old language
        if ( Objects.nonNull( name ) && !name.equals( "unnamed project" ) )
        {
            builder.label( projectConfig.getName() );
        }
    }

    /**
     * Migrates the datasets from the old declaration to the new declaration builder.
     * @param datasets the datasets to migrate
     * @param builder the new declaration builder
     */

    private static void migrateDatasets( ProjectConfig.Inputs datasets, EvaluationDeclarationBuilder builder )
    {
        Objects.requireNonNull( datasets );

        LOGGER.debug( "Migrating datasets from an old-style project declaration to a new-style declaration." );

        Dataset left = DeclarationMigrator.migrateDataset( datasets.getLeft() );
        Dataset right = DeclarationMigrator.migrateDataset( datasets.getRight() );
        BaselineDataset baseline = null;
        DataSourceBaselineConfig baselineConfig = datasets.getBaseline();

        // Baseline dataset? If so, this has extras
        if ( Objects.nonNull( baselineConfig ) )
        {
            Dataset baselineDataset = DeclarationMigrator.migrateDataset( baselineConfig );
            BaselineDatasetBuilder baselineBuilder = BaselineDatasetBuilder.builder()
                                                                           .dataset( baselineDataset );

            if ( Objects.nonNull( baselineConfig.getPersistence() ) )
            {
                LOGGER.debug( "Adding persistence of order {} to the baseline dataset.",
                              baselineConfig.getPersistence() );
                baselineBuilder.persistence( baselineConfig.getPersistence() );
            }

            // Old style declaration
            if ( Objects.nonNull( baselineConfig.getTransformation() )
                 && "persistence".equals( baselineConfig.getTransformation()
                                                        .value() ) )

            {
                LOGGER.debug( "Discovered an old-style persistence declaration. Adding persistence of order 1 to the "
                              + "baseline dataset." );
                baselineBuilder.persistence( 1 );
            }

            // Separate metrics?
            baselineBuilder.separateMetrics( baselineConfig.isSeparateMetrics() );

            baseline = baselineBuilder.build();
        }

        // Add the datasets
        builder.left( left ).right( right ).baseline( baseline );
    }

    /**
     * Migrates the features from the old declaration to the new declaration builder.
     * @param features the features to migrate
     * @param builder the new declaration builder
     */

    private static void migrateFeatures( List<NamedFeature> features, EvaluationDeclarationBuilder builder )
    {
        if ( !features.isEmpty() )
        {
            Set<GeometryTuple> geometries = DeclarationMigrator.migrateFeatures( features );
            Features wrappedFeatures = new Features( geometries );
            builder.features( wrappedFeatures );
        }
    }

    /**
     * Migrates the feature groups from the old declaration to the new declaration builder.
     * @param featureGroups the feature groups to migrate
     * @param builder the new declaration builder
     */

    private static void migrateFeatureGroups( List<FeaturePool> featureGroups, EvaluationDeclarationBuilder builder )
    {
        if ( !featureGroups.isEmpty() )
        {
            Set<GeometryGroup> geometryGroups =
                    featureGroups.stream()
                                 .map( DeclarationMigrator::migrateFeatureGroup )
                                 .collect( Collectors.toSet() );
            FeatureGroups wrappedGroups = new FeatureGroups( geometryGroups );
            builder.featureGroups( wrappedGroups );
        }
    }

    /**
     * Migrates the feature service from the old declaration to the new declaration builder.
     * @param featureService the feature service to migrate
     * @param builder the new declaration builder
     */

    private static void migrateFeatureService( wres.config.xml.generated.FeatureService featureService,
                                               EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( featureService ) )
        {
            Set<FeatureServiceGroup> featureGroups =
                    featureService.getGroup()
                                  .stream()
                                  .map( DeclarationMigrator::migrateFeatureServiceGroup )
                                  .collect( Collectors.toSet() );
            FeatureService service = FeatureServiceBuilder.builder()
                                                          .featureGroups( featureGroups )
                                                          .uri( featureService.getBaseUrl() )
                                                          .build();
            builder.featureService( service );
        }
    }

    /**
     * Migrates a spatial mask from the old declaration to the new declaration builder.
     * @param mask the spatial mask to migrate
     * @param builder the new declaration builder
     */
    private static void migrateSpatialMask( List<UnnamedFeature> mask, EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( mask ) && !mask.isEmpty() )
        {
            Pair<String, Long> maskComponents = DeclarationMigrator.migrateSpatialMask( mask );
            SpatialMask spatialMask = null;
            if ( Objects.nonNull( maskComponents )
                 && Objects.nonNull( maskComponents.getLeft() ) )
            {
                org.locationtech.jts.geom.Geometry maskGeometry =
                        DeclarationUtilities.getGeometry( maskComponents.getLeft(), maskComponents.getRight() );
                spatialMask = SpatialMaskBuilder.builder()
                                                .geometry( maskGeometry )
                                                .build();
            }

            builder.spatialMask( spatialMask );
        }
    }

    /**
     * Migrates the time filters from the old declaration to the new declaration builder.
     * @param pair the pairs declaration that contains the time filters
     * @param builder the new declaration builder
     */
    private static void migrateTimeFilters( PairConfig pair, EvaluationDeclarationBuilder builder )
    {
        // Reference time filter
        if ( Objects.nonNull( pair.getIssuedDates() ) )
        {
            TimeInterval referenceDates = DeclarationMigrator.migrateTimeInterval( pair.getIssuedDates() );
            builder.referenceDates( referenceDates );
            LOGGER.debug( "Migrated a reference time filter: {}.", referenceDates );
        }

        // Valid time filter
        if ( Objects.nonNull( pair.getDates() ) )
        {
            TimeInterval validDates = DeclarationMigrator.migrateTimeInterval( pair.getDates() );
            builder.validDates( validDates );
            LOGGER.debug( "Migrated a valid time filter: {}.", validDates );
        }

        // Reference time pools
        if ( Objects.nonNull( pair.getIssuedDatesPoolingWindow() ) )
        {
            TimePools referenceTimePools = DeclarationMigrator.migrateTimePools( pair.getIssuedDatesPoolingWindow() );
            builder.referenceDatePools( referenceTimePools );
            LOGGER.debug( "Migrated reference time pools: {}.", referenceTimePools );
        }

        // Valid time pools
        if ( Objects.nonNull( pair.getValidDatesPoolingWindow() ) )
        {
            TimePools validTimePools = DeclarationMigrator.migrateTimePools( pair.getValidDatesPoolingWindow() );
            builder.validDatePools( validTimePools );
            LOGGER.debug( "Migrated valid time pools: {}.", validTimePools );
        }

        // Lead duration filter
        if ( Objects.nonNull( pair.getLeadHours() ) )
        {
            IntBoundsType leadBounds = pair.getLeadHours();
            Duration minimum = DeclarationMigrator.getDurationOrNull( leadBounds.getMinimum(), null );
            Duration maximum = DeclarationMigrator.getDurationOrNull( leadBounds.getMaximum(), null );
            LeadTimeInterval leadTimes = new LeadTimeInterval( minimum, maximum );
            builder.leadTimes( leadTimes );
            LOGGER.debug( "Migrated a lead time filter: {}.", leadTimes );
        }

        // Lead time pools
        if ( Objects.nonNull( pair.getLeadTimesPoolingWindow() ) )
        {
            TimePools leadTimePools = DeclarationMigrator.migrateTimePools( pair.getLeadTimesPoolingWindow() );
            builder.leadTimePools( leadTimePools );
            LOGGER.debug( "Migrated lead time pools: {}.", leadTimePools );
        }

        // Analysis durations
        if ( Objects.nonNull( pair.getAnalysisDurations() ) )
        {
            DurationBoundsType durations = pair.getAnalysisDurations();
            ChronoUnit unit = ChronoUnit.valueOf( durations.getUnit()
                                                           .name() );
            Integer minimumDeclared = durations.getGreaterThan();
            if ( Objects.nonNull( minimumDeclared ) )
            {
                minimumDeclared = minimumDeclared + 1;
                Integer maximum = durations.getLessThanOrEqualTo();
                if ( Objects.nonNull( maximum ) && minimumDeclared > maximum )
                {
                    minimumDeclared = maximum;
                }
                else
                {
                    LOGGER.debug( "When migrating the analysis times, added 1 {} to the lower bound, as the old "
                                  + "declaration language uses an exclusive lower bound and the new declaration "
                                  + "language uses an inclusive lower bound. The migrated lower bound is: {}.",
                                  minimumDeclared,
                                  unit );
                }
            }

            Duration minimum = DeclarationMigrator.getDurationOrNull( minimumDeclared, unit );
            Duration maximum = DeclarationMigrator.getDurationOrNull( durations.getLessThanOrEqualTo(), unit );
            AnalysisTimes analysisTimes = new AnalysisTimes( minimum, maximum );
            builder.analysisTimes( analysisTimes );
            LOGGER.debug( "Migrated an analysis duration filter: {}.", analysisTimes );
        }

        // Season filter
        if ( Objects.nonNull( pair.getSeason() ) )
        {
            Season season = DeclarationMigrator.migrateSeason( pair.getSeason() );
            builder.season( season );
            LOGGER.debug( "Migrated a season filter: {}.", season );
        }
    }

    /**
     * Returns a duration unit from the input or null.
     * @param period the optional period
     * @param unit the optional unit
     * @return the duration or null
     */
    private static Duration getDurationOrNull( Integer period, ChronoUnit unit )
    {
        Duration duration = null;
        if ( Objects.nonNull( period ) )
        {
            if ( Objects.isNull( unit ) )
            {
                duration = Duration.ofHours( period );
            }
            else
            {
                duration = Duration.of( period, unit );
            }
        }

        return duration;
    }

    /**
     * Migrates the evaluation timescale from the old declaration to the new declaration builder.
     * @param timeScale the timescale to migrate
     * @param builder the new declaration builder
     */
    private static void migrateEvaluationTimeScale( DesiredTimeScaleConfig timeScale,
                                                    EvaluationDeclarationBuilder builder )
    {
        TimeScale timeScaleMigrated = DeclarationMigrator.migrateTimeScale( timeScale );

        if ( Objects.nonNull( timeScaleMigrated ) )
        {
            LOGGER.debug( "Encountered a desired time scale to migrate: {}.", timeScale );

            wres.statistics.generated.TimeScale.Builder scaleBuilder = timeScaleMigrated.timeScale()
                                                                                        .toBuilder();

            if ( Objects.nonNull( timeScale.getEarliestDay() ) )
            {
                scaleBuilder.setStartDay( timeScale.getEarliestDay() );
            }

            if ( Objects.nonNull( timeScale.getEarliestMonth() ) )
            {
                scaleBuilder.setStartMonth( timeScale.getEarliestMonth() );
            }

            if ( Objects.nonNull( timeScale.getLatestDay() ) )
            {
                scaleBuilder.setEndDay( timeScale.getLatestDay() );
            }

            if ( Objects.nonNull( timeScale.getLatestMonth() ) )
            {
                scaleBuilder.setEndMonth( timeScale.getLatestMonth() );
            }

            if ( Objects.nonNull( timeScale.getFrequency() ) && Objects.nonNull( timeScale.getUnit() ) )
            {
                ChronoUnit unit = ChronoUnit.valueOf( timeScale.getUnit()
                                                               .name() );
                Duration frequency = Duration.of( timeScale.getFrequency(), unit );
                builder.pairFrequency( frequency );
                LOGGER.debug( "Discovered the frequency associated with the desired time scale and migrated it to a "
                              + "paired frequency of: {}.", frequency );
            }

            TimeScale adjusted = new TimeScale( scaleBuilder.build() );
            builder.timeScale( adjusted );

            LOGGER.debug( "Migrated an evaluation timescale: {}.", adjusted );

            LenienceType lenience = timeScale.getLenient();
            TimeScaleLenience timeScaleLenience = DeclarationMigrator.migrateLenienceType( lenience );
            builder.rescaleLenience( timeScaleLenience );
        }
    }

    /**
     * Migrates the {@link LenienceType} to a {@link TimeScaleLenience}.
     * @param lenienceType the lenience type to migrate
     * @return the migrated lenience type
     */

    private static TimeScaleLenience migrateLenienceType( LenienceType lenienceType )
    {
        return switch ( lenienceType )
                {
                    case TRUE -> TimeScaleLenience.ALL;
                    case FALSE -> TimeScaleLenience.NONE;
                    default -> TimeScaleLenience.valueOf( lenienceType.name() );
                };
    }

    /**
     * Migrates the unit aliases from the old declaration to the new declaration builder.
     * @param unitAliases the unit aliases
     * @param builder the new declaration builder
     */
    private static void migrateUnitAliases( List<wres.config.xml.generated.UnitAlias> unitAliases,
                                            EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( unitAliases ) && !unitAliases.isEmpty() )
        {
            LOGGER.debug( "Encountered these unit aliases to migrate: {}.", unitAliases );

            Set<UnitAlias> aliases = unitAliases.stream()
                                                .map( DeclarationMigrator::migrateUnitAlias )
                                                .collect( Collectors.toSet() );

            LOGGER.debug( "Migrated these unit aliases: {}.", aliases );

            builder.unitAliases( aliases );
        }
    }

    /**
     * Migrates the cross pairing option to the new declaration builder.
     * @param crossPair the cross pairing
     * @param builder the new declaration builder
     */
    private static void migrateCrossPairing( wres.config.xml.generated.CrossPair crossPair,
                                             EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( crossPair ) )
        {
            LOGGER.debug( "Encountered cross pairing declaration to migrate: {}.", crossPair );
            CrossPair crossPairMigrated = CrossPair.FUZZY;
            if ( crossPair.isExact() )
            {
                crossPairMigrated = CrossPair.EXACT;
            }
            LOGGER.debug( "Migrated this cross-pairing option: {}.", crossPairMigrated );
            builder.crossPair( crossPairMigrated );
        }
    }

    /**
     * Migrates the value filter to the new declaration builder.
     * @param valueFilter the value filter
     * @param builder the new declaration builder
     */
    private static void migrateValueFilter( DoubleBoundsType valueFilter,
                                            EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( valueFilter ) )
        {
            LOGGER.debug( "Encountered a value filter to migrate: {}.", valueFilter );
            Values values = new Values( valueFilter.getMinimum(),
                                        valueFilter.getMaximum(),
                                        valueFilter.getDefaultMinimum(),
                                        valueFilter.getDefaultMaximum() );
            LOGGER.debug( "Migrated this value filter: {}.", valueFilter );
            builder.values( values );
        }
    }

    /**
     * Migrates the metrics to the new declaration builder.
     *
     * @param projectConfig the project configuration, including the metrics configuration
     * @param builder the new declaration builder
     * @param inline is true to migrate external CSV thresholds inline, false to use a {@link ThresholdSource}
     */
    private static void migrateMetrics( ProjectConfig projectConfig,
                                        EvaluationDeclarationBuilder builder,
                                        boolean inline )
    {
        List<MetricsConfig> metrics = projectConfig.getMetrics();

        if ( Objects.nonNull( metrics ) && !metrics.isEmpty() )
        {
            LOGGER.debug( "Encountered {} groups of metrics to migrate.", metrics.size() );

            // Is there a single set of thresholds? If so, migrate to top-level thresholds
            List<ThresholdsConfig> globalThresholds =
                    DeclarationMigrator.getGlobalThresholds( metrics );
            boolean addThresholdsPerMetric = true;
            if ( !globalThresholds.isEmpty() )
            {
                LOGGER.debug( "Discovered these global thresholds to migrate: {}.", globalThresholds );

                addThresholdsPerMetric = false;
                DeclarationMigrator.migrateGlobalThresholds( globalThresholds, builder, inline );
            }

            // Now migrate the metrics
            DeclarationMigrator.migrateMetrics( metrics, projectConfig, builder, addThresholdsPerMetric, inline );
        }
    }

    /**
     * Migrates the output formats to the new declaration builder.
     *
     * @param outputs the output formats to migrate
     * @param builder the new declaration builder to mutate
     */
    private static void migrateOutputFormats( ProjectConfig.Outputs outputs,
                                              EvaluationDeclarationBuilder builder )
    {
        // Iterate through the destinations and migrate each one
        for ( DestinationConfig next : outputs.getDestination() )
        {
            DeclarationMigrator.migrateOutputFormat( next, builder );
        }

        DurationUnit durationUnit = outputs.getDurationFormat();
        ChronoUnit timeFormat = ChronoUnit.valueOf( durationUnit.name() );
        LOGGER.debug( "Discovered a duration format to migrate: {}.", timeFormat );
        builder.durationFormat( timeFormat );
    }

    /**
     * Migrates the output format to the new declaration builder.
     *
     * @param output the output format to migrate
     * @param builder the new declaration builder to mutate
     */
    private static void migrateOutputFormat( DestinationConfig output,
                                             EvaluationDeclarationBuilder builder )
    {
        LOGGER.debug( "Migrating output format: {}.", output.getType() );

        // Migrate the decimal format
        String decimalFormat = output.getDecimalFormat();
        if ( Objects.nonNull( decimalFormat ) )
        {
            if ( Objects.isNull( builder.decimalFormat() ) )
            {
                DecimalFormatPretty format = new DecimalFormatPretty( decimalFormat );
                builder.decimalFormat( format );
                LOGGER.debug( "Discovered a decimal format to migrate: {}.", decimalFormat );
            }
            else if ( decimalFormat.replace( "#", "" )
                                   .length() > builder.decimalFormat()
                                                      .toString()
                                                      .replace( "#", "" )
                                                      .length() )
            {
                DecimalFormatPretty format = new DecimalFormatPretty( decimalFormat );
                LOGGER.warn( "Discovered more than one decimal format to migrate. The new declaration language "
                             + "supports only one format per evaluation. The existing format is {} and the new format "
                             + "is {}. Choosing {}.", builder.decimalFormat(), format, format );
                builder.decimalFormat( format );
            }
        }

        // Set the basic formats
        Outputs.Builder formatsBuilder = Outputs.newBuilder();
        if ( Objects.nonNull( builder.formats() ) )
        {
            formatsBuilder.mergeFrom( builder.formats()
                                             .outputs() );
        }

        // Set the decimal formatter for the individual output options
        Outputs.NumericFormat numericFormat = Formats.DEFAULT_NUMERIC_FORMAT;
        if ( Objects.nonNull( builder.decimalFormat() ) )
        {
            numericFormat = numericFormat.toBuilder()
                                         .setDecimalFormat( builder.decimalFormat()
                                                                   .toPattern() )
                                         .build();
        }

        switch ( output.getType() )
        {
            case CSV, NUMERIC -> formatsBuilder.setCsv( Formats.CSV_FORMAT.toBuilder()
                                                                          .setOptions( numericFormat )
                                                                          .build() );
            case CSV2 -> formatsBuilder.setCsv2( Formats.CSV2_FORMAT.toBuilder()
                                                                    .setOptions( numericFormat )
                                                                    .build() );
            case PNG, GRAPHIC -> DeclarationMigrator.migratePngFormat( output, formatsBuilder, builder );
            case SVG -> DeclarationMigrator.migrateSvgFormat( output, formatsBuilder, builder );
            case NETCDF -> formatsBuilder.setNetcdf( Formats.NETCDF_FORMAT );
            case NETCDF_2 -> formatsBuilder.setNetcdf2( Formats.NETCDF2_FORMAT );
            case PAIRS -> formatsBuilder.setPairs( Formats.PAIR_FORMAT.toBuilder()
                                                                      .setOptions( numericFormat )
                                                                      .build() );
            case PROTOBUF -> formatsBuilder.setProtobuf( Formats.PROTOBUF_FORMAT );
        }

        // Migrate the formats
        builder.formats( new Formats( formatsBuilder.build() ) );
    }

    /**
     * Migrates a PNG output format.
     * @param output the output
     * @param builder the builder to mutate
     * @param evaluationBuilder the evaluation builder whose metric parameters may need to be updated
     */

    private static void migratePngFormat( DestinationConfig output,
                                          Outputs.Builder builder,
                                          EvaluationDeclarationBuilder evaluationBuilder )
    {
        Outputs.PngFormat.Builder pngBuilder = Formats.PNG_FORMAT.toBuilder();
        Outputs.GraphicFormat.Builder graphicsFormatBuilder = pngBuilder.getOptions()
                                                                        .toBuilder();

        // Set any extra parameters
        GraphicalType graphics = output.getGraphical();
        if ( Objects.nonNull( graphics ) )
        {
            Set<Metric> metrics = DeclarationMigrator.migrateGraphicsOptions( graphics,
                                                                              graphicsFormatBuilder,
                                                                              evaluationBuilder.metrics(),
                                                                              output.getOutputType(),
                                                                              Format.PNG );

            // Set the possibly adjusted metrics
            evaluationBuilder.metrics( metrics );
        }

        pngBuilder.setOptions( graphicsFormatBuilder );
        builder.setPng( pngBuilder );
    }

    /**
     * Migrates a SVG output format.
     * @param output the output
     * @param builder the builder to mutate
     * @param evaluationBuilder the evaluation builder whose metric parameters may need to be updated
     */

    private static void migrateSvgFormat( DestinationConfig output,
                                          Outputs.Builder builder,
                                          EvaluationDeclarationBuilder evaluationBuilder )
    {
        Outputs.SvgFormat.Builder svgBuilder = Formats.SVG_FORMAT.toBuilder();
        Outputs.GraphicFormat.Builder graphicsFormatBuilder = svgBuilder.getOptions()
                                                                        .toBuilder();

        // Set any extra parameters
        GraphicalType graphics = output.getGraphical();
        if ( Objects.nonNull( graphics ) )
        {
            Set<Metric> metrics = DeclarationMigrator.migrateGraphicsOptions( graphics,
                                                                              graphicsFormatBuilder,
                                                                              evaluationBuilder.metrics(),
                                                                              output.getOutputType(),
                                                                              Format.SVG );

            // Set the possibly adjusted metrics
            evaluationBuilder.metrics( metrics );
        }

        svgBuilder.setOptions( graphicsFormatBuilder );
        builder.setSvg( svgBuilder );
    }

    /**
     * Migrates the graphical format options
     * @param graphics the graphics options.
     * @param builder the builder to mutate
     * @param metrics the metrics whose parameters may need to be updated
     * @param shape the shape of the graphic
     * @param format the format
     * @return the possibly updated metrics
     */

    private static Set<Metric> migrateGraphicsOptions( GraphicalType graphics,
                                                       Outputs.GraphicFormat.Builder builder,
                                                       Set<Metric> metrics,
                                                       OutputTypeSelection shape,
                                                       Format format )
    {
        if ( Objects.nonNull( graphics.getHeight() ) )
        {
            builder.setHeight( graphics.getHeight() );
        }

        if ( Objects.nonNull( graphics.getWidth() ) )
        {
            builder.setWidth( graphics.getWidth() );
        }

        if ( Objects.nonNull( shape ) )
        {
            Outputs.GraphicFormat.GraphicShape shapeNew = Outputs.GraphicFormat.GraphicShape.valueOf( shape.name() );
            builder.setShape( shapeNew );
            LOGGER.debug( "Migrated the graphic shape for the {} format: {}.", format, shapeNew );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            String protoString = PROTBUF_STRINGIFIER.apply( builder.build() );
            LOGGER.debug( "Migrated these graphics options for {}: {}.", format, protoString );
        }

        if ( Objects.nonNull( metrics ) && !graphics.getSuppressMetric()
                                                    .isEmpty() )
        {
            return DeclarationMigrator.migrateSuppressMetrics( graphics.getSuppressMetric(), metrics, format );
        }
        else
        {
            return metrics;
        }
    }

    /**
     * Adjusts the metric parameters to suppress writing of graphics for the
     * @param suppress the list of metrics whose format parameter should be suppressed
     * @param metrics the metrics
     * @param format the format to suppress
     * @return the adjusted metrics
     */

    private static Set<Metric> migrateSuppressMetrics( List<MetricConfigName> suppress,
                                                       Set<Metric> metrics,
                                                       Format format )
    {
        // Preserve insertion order
        Set<Metric> migrated = new LinkedHashSet<>();
        Set<MetricConstants> suppressMigrated = suppress.stream()
                                                        .map( MetricConstantsFactory::from )
                                                        .collect( Collectors.toSet() );

        for ( Metric metric : metrics )
        {
            if ( suppressMigrated.contains( metric.name() ) )
            {
                LOGGER.debug( "Migrating a metric parameter for {} to suppress format {}.", metric.name(), format );

                MetricBuilder migratedMetricBuilder = MetricBuilder.builder( metric );
                MetricParametersBuilder parBuilder = MetricParametersBuilder.builder();

                // Set the existing parameters if available
                if ( Objects.nonNull( metric.parameters() ) )
                {
                    parBuilder = MetricParametersBuilder.builder( metric.parameters() );
                }

                // Suppress the format
                if ( format == Format.PNG )
                {
                    parBuilder.png( false );
                }
                else if ( format == Format.SVG )
                {
                    parBuilder.svg( false );
                }

                Metric migratedMetric = migratedMetricBuilder.parameters( parBuilder.build() )
                                                             .build();
                migrated.add( migratedMetric );
            }
            else
            {
                migrated.add( metric );
            }
        }

        return Collections.unmodifiableSet( migrated );
    }

    /**
     * Migrates the unit aliases from the old declaration to the new declaration builder.
     * @param unitAlias the unit alias to migrate
     * @return the migrated unit alias
     */
    private static UnitAlias migrateUnitAlias( wres.config.xml.generated.UnitAlias unitAlias )
    {
        return new UnitAlias( unitAlias.getAlias(), unitAlias.getUnit() );
    }

    /**
     * Migrates a {@link PairConfig.Season} to a {@link Season}.
     * @param season the season to migrate
     * @return the migrated season
     */

    private static Season migrateSeason( PairConfig.Season season )
    {
        short startDay = 1;
        short startMonth = 1;
        short endDay = 31;
        short endMonth = 12;

        // Update defaults
        if ( season.getEarliestMonth() != 0 )
        {
            startMonth = season.getEarliestMonth();
        }
        if ( season.getEarliestDay() != 0 )
        {
            startDay = season.getEarliestDay();
        }
        if ( season.getLatestMonth() != 0 )
        {
            endMonth = season.getLatestMonth();
        }
        if ( season.getLatestDay() != 0 )
        {
            endDay = season.getLatestDay();
        }

        MonthDay earliest = MonthDay.of( startMonth, startDay );
        MonthDay latest = MonthDay.of( endMonth, endDay );

        return new Season( earliest, latest );
    }

    /**
     * Migrates from a {@link DataSourceConfig} to a {@link Dataset}.
     *
     * @param dataSource the data source
     * @return the dataset
     */

    private static Dataset migrateDataset( DataSourceConfig dataSource )
    {
        if ( Objects.isNull( dataSource ) )
        {
            LOGGER.debug( "Encountered a null data source, not migrating." );
            return null;
        }

        ZoneOffset universalZoneOffset = DeclarationMigrator.migrateTimeZoneOffset( dataSource.getSource() );
        List<Source> sources = DeclarationMigrator.migrateSources( dataSource.getSource(),
                                                                   dataSource.getUrlParameter(),
                                                                   Objects.nonNull( universalZoneOffset ) );
        EnsembleFilter filter = DeclarationMigrator.migrateEnsembleFilter( dataSource.getEnsemble() );
        FeatureAuthority featureAuthority =
                DeclarationMigrator.migrateFeatureAuthority( dataSource.getFeatureDimension() );
        Variable variable = DeclarationMigrator.migrateVariable( dataSource.getVariable() );
        DataType type = DeclarationMigrator.migrateDataType( dataSource.getType() );
        Duration timeShift = DeclarationMigrator.migrateTimeShift( dataSource.getTimeShift() );
        TimeScale timeScale = DeclarationMigrator.migrateTimeScale( dataSource.getExistingTimeScale() );

        return DatasetBuilder.builder()
                             .sources( sources )
                             .ensembleFilter( filter )
                             .featureAuthority( featureAuthority )
                             .label( dataSource.getLabel() )
                             .variable( variable )
                             .type( type )
                             .timeShift( timeShift )
                             .timeZoneOffset( universalZoneOffset )
                             .timeScale( timeScale )
                             .build();
    }

    /**
     * Attempts to discover and return a universal time zone offset that applies across all sources. If there are no
     * time zone offsets defined, or they vary per-source, returns null.
     * @param sources the sources to inspect
     * @return the universal time zone offset or null if no such offset applies
     */

    private static ZoneOffset migrateTimeZoneOffset( List<DataSourceConfig.Source> sources )
    {
        Set<ZoneOffset> offsets = sources.stream()
                                         .map( DataSourceConfig.Source::getZoneOffset )
                                         .filter( Objects::nonNull )
                                         .map( ZoneOffsetDeserializer::getZoneOffset )
                                         .collect( Collectors.toSet() );

        if ( offsets.size() == 1 )
        {
            ZoneOffset offset = offsets.iterator()
                                       .next();
            LOGGER.debug( "Identified a universal time zone offset of {} for these data sources: {}.",
                          offset,
                          sources );
            return offset;
        }

        LOGGER.debug( "Failed to identify a universal time zone offset for the supplied data sources. Discovered "
                      + "these time zone offsets for the individual sources:  {}. The data sources were: {}.",
                      offsets,
                      sources );

        return null;
    }

    /**
     * Migrates a collection of {@link DataSourceConfig.Source} to a collection of {@link Source}.
     *
     * @param sources the data sources
     * @param parameters the optional URL parameters
     * @param universalTimeZoneOffset whether there is a universal time zone offset. If so, do not migrate per source
     * @return the migrated sources
     */

    private static List<Source> migrateSources( List<DataSourceConfig.Source> sources,
                                                List<UrlParameter> parameters,
                                                boolean universalTimeZoneOffset )
    {
        return sources.stream()
                      .map( next -> DeclarationMigrator.migrateSource( next,
                                                                       parameters,
                                                                       universalTimeZoneOffset ) )
                      .toList();
    }

    /**
     * Migrates a {@link DataSourceConfig.Source} to a {@link Source}.
     *
     * @param source the data sources
     * @param parameters the optional URL parameters
     * @param universalTimeZoneOffset whether there is a universal time zone offset. If so, do not migrate per source
     * @return the migrated source
     */

    private static Source migrateSource( DataSourceConfig.Source source,
                                         List<UrlParameter> parameters,
                                         boolean universalTimeZoneOffset )
    {
        SourceBuilder builder = SourceBuilder.builder()
                                             .uri( source.getValue() )
                                             .pattern( source.getPattern() );

        // Do not propagate the default missing value from the old declaration
        if ( Objects.nonNull( source.getMissingValue() )
             && !DEFAULT_MISSING_STRING_OLD.equals( source.getMissingValue() ) )
        {
            String[] split = source.getMissingValue()
                                   .split( "," );
            List<Double> missing = Arrays.stream( split )
                                         .map( Double::parseDouble )
                                         .toList();
            builder.missingValue( missing );
        }

        if ( Objects.nonNull( source.getInterface() ) )
        {
            String interfaceName = source.getInterface().name();
            SourceInterface sourceInterface = SourceInterface.valueOf( interfaceName );
            builder.sourceInterface( sourceInterface );
        }

        if ( !universalTimeZoneOffset && Objects.nonNull( source.getZoneOffset() ) )
        {
            ZoneOffset offset = ZoneOffsetDeserializer.getZoneOffset( source.getZoneOffset() );
            builder.timeZoneOffset( offset );
        }

        if ( Objects.nonNull( parameters ) && !parameters.isEmpty() )
        {
            Map<String, String> sourceParameters
                    = parameters.stream()
                                .collect( Collectors.toUnmodifiableMap( UrlParameter::getName,
                                                                        UrlParameter::getValue ) );
            builder.parameters( sourceParameters );
        }

        return builder.build();
    }

    /**
     * Migrates a {@link NamedFeature} to a {@link GeometryTuple}.
     * @param feature the feature to migrate
     * @return the migrated feature
     */

    private static GeometryTuple migrateFeature( NamedFeature feature )
    {
        GeometryTuple.Builder builder = GeometryTuple.newBuilder();

        String leftName = feature.getLeft();
        if ( Objects.nonNull( leftName ) )
        {
            Geometry left = Geometry.newBuilder()
                                    .setName( leftName )
                                    .build();
            builder.setLeft( left );
        }

        String rightName = feature.getRight();
        if ( Objects.nonNull( rightName ) )
        {
            Geometry right = Geometry.newBuilder()
                                     .setName( rightName )
                                     .build();
            builder.setRight( right );
        }

        String baselineName = feature.getBaseline();
        if ( Objects.nonNull( baselineName ) )
        {
            Geometry baseline = Geometry.newBuilder()
                                        .setName( baselineName )
                                        .build();
            builder.setBaseline( baseline );
        }

        return builder.build();
    }

    /**
     * Migrates a collection of {@link NamedFeature} to {@link GeometryTuple}.
     * @param features the geospatial features
     * @return the migrated geospatial features
     */

    private static Set<GeometryTuple> migrateFeatures( List<NamedFeature> features )
    {
        return features.stream()
                       .map( DeclarationMigrator::migrateFeature )
                       .collect( Collectors.toSet() );
    }

    /**
     * Migrates a {@link FeaturePool} to a {@link GeometryGroup}.
     * @param featureGroup the feature group to migrate
     * @return the migrated feature group
     */

    private static GeometryGroup migrateFeatureGroup( FeaturePool featureGroup )
    {
        Set<GeometryTuple> members = DeclarationMigrator.migrateFeatures( featureGroup.getFeature() );

        return GeometryGroup.newBuilder()
                            .setRegionName( featureGroup.getName() )
                            .addAllGeometryTuples( members )
                            .build();
    }

    /**
     * Migrates a {@link FeatureGroup} to a {@link FeatureServiceGroup}.
     * @param featureGroup the feature group to migrate
     * @return the migrated feature group
     */

    private static FeatureServiceGroup migrateFeatureServiceGroup( FeatureGroup featureGroup )
    {
        return new FeatureServiceGroup( featureGroup.getType(), featureGroup.getValue(), featureGroup.isPool() );
    }

    /**
     * Migrates a collection of features to a WKT geometry string.
     * @param features the features
     * @return the WKT string and associated SRID, if any
     */

    private static Pair<String, Long> migrateSpatialMask( List<UnnamedFeature> features )
    {
        org.locationtech.jts.geom.Geometry unionGeometry = null;
        Long srid = null;

        if ( features.isEmpty() )
        {
            LOGGER.debug( "No spatial mask features to migrate." );
            return null;
        }

        if ( features.size() > 1 )
        {
            for ( UnnamedFeature next : features )
            {
                Pair<org.locationtech.jts.geom.Geometry, Long> nextG = DeclarationMigrator.migrateGeometry( next );
                org.locationtech.jts.geom.Geometry nextGeometry = nextG.getLeft();
                if ( Objects.nonNull( nextG.getRight() ) )
                {
                    srid = ( long ) nextGeometry.getSRID();
                }
                if ( Objects.isNull( unionGeometry ) && Objects.nonNull( nextGeometry ) )
                {
                    unionGeometry = nextGeometry;
                }
                else if ( Objects.nonNull( nextGeometry ) )
                {
                    unionGeometry = unionGeometry.union( nextGeometry );
                }
            }
        }
        else
        {
            Pair<org.locationtech.jts.geom.Geometry, Long> g = DeclarationMigrator.migrateGeometry( features.get( 0 ) );
            unionGeometry = g.getLeft();
            srid = g.getRight();
        }

        String wkt = null;
        if ( Objects.nonNull( unionGeometry ) )
        {
            WKTWriter writer = new WKTWriter();
            wkt = writer.write( unionGeometry );
        }

        LOGGER.debug( "Migrated a spatial mask with a WKT string of {} and a SRID of {}.", wkt, srid );

        return Pair.of( wkt, srid );
    }

    /**
     * Migrates a {@link UnnamedFeature} to a {@link org.locationtech.jts.geom.Geometry}.
     * @param feature the feature
     * @return the migrated geometry and SRID, if any
     */

    private static Pair<org.locationtech.jts.geom.Geometry, Long> migrateGeometry( UnnamedFeature feature )
    {
        org.locationtech.jts.geom.Geometry geometry = null;
        Long srid = null;

        if ( Objects.nonNull( feature.getCircle() ) )
        {
            GeometricShapeFactory shapeMaker = new GeometricShapeFactory();
            Circle circle = feature.getCircle();
            shapeMaker.setWidth( circle.getDiameter() );
            shapeMaker.setBase( new CoordinateXY( circle.getLongitude(), circle.getLatitude() ) );
            geometry = shapeMaker.createCircle();
            srid = circle.getSrid()
                         .longValue();
        }
        else if ( Objects.nonNull( feature.getPolygon() ) && feature.getPolygon()
                                                                    .getPoint()
                                                                    .size() > 1 )
        {
            Polygon polygon = feature.getPolygon();
            srid = polygon.getSrid()
                          .longValue();
            List<Polygon.Point> points = polygon.getPoint();
            GeometryFactory geometryFactory = new GeometryFactory();

            List<Coordinate> coordinates = new ArrayList<>();
            for ( Polygon.Point nextPoint : points )
            {
                double longitude = Double.parseDouble( Float.toString( nextPoint.getLongitude() ) );
                double latitude = Double.parseDouble( Float.toString( nextPoint.getLatitude() ) );
                Coordinate coordinate = new CoordinateXY( longitude, latitude );
                coordinates.add( coordinate );
            }

            // Close the polygon
            coordinates.add( coordinates.get( 0 ) );

            geometry = geometryFactory.createPolygon( coordinates.toArray( new Coordinate[1] ) );
        }
        else if ( Objects.nonNull( feature.getCoordinate() ) )
        {
            LOGGER.warn( "Discovered a spatial coordinate associated with a grid selection, which is not a valid area "
                         + "selection. This spatial coordinate declaration will not be migrated. If you want to "
                         + "include a grid selection, please remove the single coordinate and add a polygon or "
                         + "circle." );
        }

        return Pair.of( geometry, srid );
    }

    /**
     * Migrates a collection of {@link EnsembleCondition} to an {@link EnsembleFilter}. Note that the old-style
     * declaration schema is incoherent because it allows for inclusion/exclusion at the level of individual members,
     * whereas the new style declaration requires that all members have the same included/excluded state. If an old-
     * style declaration contains both included/excluded members in the same declaration it is redundant at best and
     * incoherent at worst. It is incoherent when the excluded members are part of the subset of included members, and
     * it is superfluous otherwise (since all members that are not included are, by definition, excluded). In this case,
     * a warning will be emitted and only the excluded members transferred.
     *
     * @param filters the ensemble filters
     * @return the migrated ensemble filter
     */

    private static EnsembleFilter migrateEnsembleFilter( List<EnsembleCondition> filters )
    {
        if ( Objects.isNull( filters ) || filters.isEmpty() )
        {
            LOGGER.debug( "Encountered a null or empty collection of ensemble filters, not migrating." );
            return null;
        }

        Set<String> included = filters.stream()
                                      .filter( next -> !next.isExclude() )
                                      .map( EnsembleCondition::getName )
                                      .collect( Collectors.toSet() );

        Set<String> excluded = filters.stream()
                                      .filter( EnsembleCondition::isExclude )
                                      .map( EnsembleCondition::getName )
                                      .collect( Collectors.toSet() );

        EnsembleFilterBuilder builder = EnsembleFilterBuilder.builder();

        // Both included and excluded filters, which is not coherent, but does not produce an error here because the
        // old-style declaration allows it
        if ( !included.isEmpty() && !excluded.isEmpty() )
        {
            LOGGER.warn( "The original declaration requests that some ensemble members are included and some are "
                         + "excluded. The members to include are: {}. The members to exclude are: {}. This is "
                         + "probably not coherent because all members outside of the exclude list are included, "
                         + "by definition. Thus, only the excluded members will be migrated.", included, excluded );
            builder.members( excluded ).exclude( true );
        }
        // One or other are present
        else
        {
            Set<String> members = new HashSet<>( excluded );
            members.addAll( included );
            builder.members( members ).exclude( included.isEmpty() );
        }

        return builder.build();
    }

    /**
     * Migrates a {@link TimeScaleConfig} to a {@link TimeScale}.
     *
     * @param timeScaleConfig the timescale
     * @return the migrated timescale
     */

    private static TimeScale migrateTimeScale( TimeScaleConfig timeScaleConfig )
    {
        if ( Objects.isNull( timeScaleConfig ) )
        {
            LOGGER.debug( "Encountered a missing time scale, not migrating." );
            return null;
        }

        wres.statistics.generated.TimeScale.Builder timeScaleInner = wres.statistics.generated.TimeScale.newBuilder();

        if ( Objects.nonNull( timeScaleConfig.getPeriod() ) )
        {
            java.time.Duration period = ProjectConfigs.getDurationFromTimeScale( timeScaleConfig );
            com.google.protobuf.Duration canonicalPeriod = com.google.protobuf.Duration.newBuilder()
                                                                                       .setSeconds( period.getSeconds() )
                                                                                       .setNanos( period.getNano() )
                                                                                       .build();
            timeScaleInner.setPeriod( canonicalPeriod );
        }

        if ( Objects.isNull( timeScaleConfig.getFunction() ) )
        {
            timeScaleInner.setFunction( wres.statistics.generated.TimeScale.TimeScaleFunction.UNKNOWN );
        }
        else
        {
            wres.statistics.generated.TimeScale.TimeScaleFunction innerFunction =
                    wres.statistics.generated.TimeScale.TimeScaleFunction.valueOf( timeScaleConfig.getFunction()
                                                                                                  .name() );
            timeScaleInner.setFunction( innerFunction );
        }

        return TimeScaleBuilder.builder()
                               .timeScale( timeScaleInner.build() )
                               .build();
    }

    /**
     * Migrates a {@link FeatureDimension} to a {@link FeatureAuthority}.
     *
     * @param featureDimension the feature dimension
     * @return the feature authority
     */

    private static FeatureAuthority migrateFeatureAuthority( FeatureDimension featureDimension )
    {
        FeatureAuthority featureAuthority = null;

        if ( Objects.nonNull( featureDimension ) )
        {
            featureAuthority = FeatureAuthority.valueOf( featureDimension.name() );
        }
        return featureAuthority;
    }

    /**
     * Migrates a {@link DataSourceConfig.Variable} to a {@link Variable}.
     *
     * @param variable the variable
     * @return the migrated variable
     */

    private static Variable migrateVariable( DataSourceConfig.Variable variable )
    {
        Variable migrated = null;

        if ( Objects.nonNull( variable ) )
        {
            migrated = VariableBuilder.builder()
                                      .name( variable.getValue() )
                                      .label( variable.getLabel() )
                                      .build();
        }
        return migrated;
    }

    /**
     * Migrates a {@link DatasourceType} to a {@link DataType}.
     *
     * @param type the data source type
     * @return the migrated type
     */

    private static DataType migrateDataType( DatasourceType type )
    {
        DataType dataType = null;

        if ( Objects.nonNull( type ) )
        {
            dataType = DataType.valueOf( type.name() );
        }
        return dataType;
    }

    /**
     * Migrates a {@link DataSourceConfig.TimeShift} to a {@link Duration}.
     *
     * @param timeShift the time shift
     * @return the migrated time shift
     */

    private static Duration migrateTimeShift( DataSourceConfig.TimeShift timeShift )
    {
        Duration duration = null;

        if ( Objects.nonNull( timeShift ) )
        {
            String unitName = timeShift.getUnit().name();
            ChronoUnit chronoUnit = ChronoUnit.valueOf( unitName );
            duration = Duration.of( timeShift.getWidth(), chronoUnit );
        }
        return duration;
    }

    /**
     * Migrates a {@link DateCondition} to a {@link TimeInterval}.
     * @param dateCondition the date condition
     * @return the time interval
     */
    private static TimeInterval migrateTimeInterval( DateCondition dateCondition )
    {
        Instant earliest = null;
        Instant latest = null;

        if ( Objects.nonNull( dateCondition.getEarliest() ) )
        {
            earliest = Instant.parse( dateCondition.getEarliest() );
        }

        if ( Objects.nonNull( dateCondition.getLatest() ) )
        {
            latest = Instant.parse( dateCondition.getLatest() );
        }

        return new TimeInterval( earliest, latest );
    }

    /**
     * Migrates a {@link TimePools} to a {@link PoolingWindowConfig}.
     * @param poolingWindow the pooling window
     * @return the time pool
     */
    private static TimePools migrateTimePools( PoolingWindowConfig poolingWindow )
    {
        ChronoUnit unit = ChronoUnit.valueOf( poolingWindow.getUnit()
                                                           .name() );
        Duration period = Duration.of( poolingWindow.getPeriod(), unit );
        Duration frequency = null;

        if ( Objects.nonNull( poolingWindow.getFrequency() ) )
        {
            frequency = Duration.of( poolingWindow.getFrequency(), unit );
        }

        return new TimePools( period, frequency );
    }

    /**
     * Migrates the thresholds to the declaration builder.
     * @param thresholds the thresholds to migrate
     * @param builder the declaration builder
     * @param inline is true to migrate external CSV thresholds inline, false to use a {@link ThresholdSource}
     */

    private static void migrateGlobalThresholds( List<ThresholdsConfig> thresholds,
                                                 EvaluationDeclarationBuilder builder,
                                                 boolean inline )
    {
        // Iterate the thresholds
        for ( ThresholdsConfig nextThresholds : thresholds )
        {
            wres.config.xml.generated.ThresholdType nextType = DeclarationMigrator.getThresholdType( nextThresholds );
            Set<Threshold> migrated = DeclarationMigrator.migrateThresholds( nextThresholds, builder, inline );
            Set<Threshold> combined = new LinkedHashSet<>( migrated );

            if ( nextType == wres.config.xml.generated.ThresholdType.PROBABILITY )
            {
                if ( Objects.nonNull( builder.probabilityThresholds() ) )
                {
                    combined.addAll( builder.probabilityThresholds() );
                }
                builder.probabilityThresholds( combined );

                LOGGER.debug( "Migrated these probability thresholds for all metrics: {}.", migrated );
            }
            else if ( nextType == wres.config.xml.generated.ThresholdType.VALUE )
            {
                if ( Objects.nonNull( builder.thresholds() ) )
                {
                    combined.addAll( builder.thresholds() );
                }
                builder.thresholds( combined );

                LOGGER.debug( "Migrated these value thresholds for all metrics: {}.", migrated );
            }
            else if ( nextType == wres.config.xml.generated.ThresholdType.PROBABILITY_CLASSIFIER )
            {
                if ( Objects.nonNull( builder.classifierThresholds() ) )
                {
                    combined.addAll( builder.classifierThresholds() );
                }
                builder.classifierThresholds( combined );

                LOGGER.debug( "Migrated these classifier thresholds for all metrics: {}.", migrated );
            }
        }
    }

    /**
     * Migrates the metrics. Thresholds are only migrated on request. For example, if global thresholds are present,
     * these can be migrated to the top-level threshold buckets, rather than registered with each metric.
     *
     * @param metrics the metrics to migrate
     * @param projectConfig the overall declaration used as context when migrating metrics
     * @param builder the builder to mutate
     * @param addThresholdsPerMetric whether the thresholds declared in each metric group should be added to each metric
     * @param inline is true to migrate external CSV thresholds inline, false to use a {@link ThresholdSource}
     */
    private static void migrateMetrics( List<MetricsConfig> metrics,
                                        ProjectConfig projectConfig,
                                        EvaluationDeclarationBuilder builder,
                                        boolean addThresholdsPerMetric,
                                        boolean inline )
    {
        // Iterate through each metric group
        for ( MetricsConfig next : metrics )
        {
            // In general "all valid" metrics means no explicit migration since no metrics means "all valid" in the new
            // language. However, there is one exception: when the old language suppresses graphics formats on a per-
            // metric basis, then all metrics must be explicitly migrated because the new format records this as a
            // metric parameter
            if ( DeclarationMigrator.hasExplicitMetrics( next )
                 || DeclarationMigrator.hasSuppressedGraphics( projectConfig ) )
            {
                LOGGER.debug( "Discovered metrics to migrate from the following declaration: {}.", next );

                Set<MetricConstants> metricNames = MetricConstantsFactory.getMetricsFromConfig( next, projectConfig );

                // Acquire the parameters that apply to all metrics in this group
                MetricParameters groupParameters = DeclarationMigrator.migrateMetricParameters( next,
                                                                                                builder,
                                                                                                addThresholdsPerMetric,
                                                                                                inline );

                // Increment the metrics, preserving insertion order
                Set<Metric> overallMetrics = new LinkedHashSet<>();
                if ( Objects.nonNull( builder.metrics() ) )
                {
                    overallMetrics.addAll( builder.metrics() );
                }

                // Acquire and set the parameters that apply to individual metrics, combining with the group parameters
                Set<Metric> innerMetrics = DeclarationMigrator.migrateMetricSpecificParameters( metricNames,
                                                                                                groupParameters );

                overallMetrics.addAll( innerMetrics );

                LOGGER.debug( "Adding these migrated metrics to the metric store, which now contains {} metrics: {}.",
                              overallMetrics.size(), innerMetrics );

                builder.metrics( overallMetrics );
            }
            else
            {
                LOGGER.debug( "The following metrics declaration had no explicit metrics to migrate: {}", next );
            }
        }
    }

    /**
     * Migrates the metric-specific parameters, appending them to the input parameters. The only metric-specific
     * parameters are timing error summary statistics, which can be gleaned from the metric names, since these include
     * both the overall metrics and the summary statistics for timing error metrics.
     *
     * @param metricNames the metric names
     * @param parameters the existing parameters that should be incremented
     * @return the adjusted metrics with parameters
     */

    private static Set<Metric> migrateMetricSpecificParameters( Set<MetricConstants> metricNames,
                                                                MetricParameters parameters )
    {
        Set<Metric> returnMe = new LinkedHashSet<>();

        // Iterate through the metrics, increment the parameters and set them
        for ( MetricConstants next : metricNames )
        {
            // Add the parameters for duration diagrams
            if ( next.isInGroup( MetricConstants.StatisticType.DURATION_DIAGRAM ) )
            {
                MetricParametersBuilder builder = MetricParametersBuilder.builder();
                if ( Objects.nonNull( parameters ) )
                {
                    builder = MetricParametersBuilder.builder( parameters );
                }
                Set<MetricConstants> durationScores
                        = metricNames.stream()
                                     // Find the duration scores whose parent is the diagram in question
                                     .filter( m -> m.getParent() == next )
                                     // Get the child, which is a univariate measure
                                     .map( MetricConstants::getChild )
                                     .collect( Collectors.toSet() );
                builder.summaryStatistics( durationScores );

                // Do not add parameters that match the defaults
                MetricParameters durationDiagramPars = builder.build();
                if ( DEFAULT_METRIC_PARAMETERS.equals( durationDiagramPars ) )
                {
                    durationDiagramPars = null;
                }

                returnMe.add( new Metric( next, durationDiagramPars ) );

                LOGGER.debug( "Migrated these summary statistics for the {}: {}.",
                              next,
                              durationScores );
            }
            // Ignore duration scores, which are parameters of duration diagrams and add the other metrics as-is
            else if ( !next.isInGroup( MetricConstants.StatisticType.DURATION_SCORE ) )
            {
                returnMe.add( new Metric( next, parameters ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * @param metrics the metrics to test
     * @return whether there are some explicitly declared metrics
     */

    private static boolean hasExplicitMetrics( MetricsConfig metrics )
    {
        return !( metrics.getMetric()
                         .stream()
                         .allMatch( next -> next.getName() == MetricConfigName.ALL_VALID )
                  && metrics.getTimeSeriesMetric()
                            .stream()
                            .allMatch( next -> next.getName() == TimeSeriesMetricConfigName.ALL_VALID ) );
    }

    /**
     * @param projectConfig the declaration to inspect
     * @return whether some graphics formats suppress individual metrics
     */
    private static boolean hasSuppressedGraphics( ProjectConfig projectConfig )
    {
        return projectConfig.getOutputs()
                            .getDestination()
                            .stream()
                            .anyMatch( next -> Objects.nonNull( next.getGraphical() )
                                               && !next.getGraphical()
                                                       .getSuppressMetric()
                                                       .isEmpty() );
    }

    /**
     * Migrates the parameters in the input to a {@link MetricParameters}.
     *
     * @param metric the metric whose parameters should be read
     * @param builder the builder, which may be updated with threshold service declaration
     * @param addThresholdsPerMetric whether the thresholds declared in each metric group should be added to each metric
     * @param inline is true to migrate external CSV thresholds inline, false to use a {@link ThresholdSource}
     * @return the migrated metric parameters
     */
    private static MetricParameters migrateMetricParameters( MetricsConfig metric,
                                                             EvaluationDeclarationBuilder builder,
                                                             boolean addThresholdsPerMetric,
                                                             boolean inline )
    {
        MetricParametersBuilder parametersBuilder = MetricParametersBuilder.builder();

        // Set the thresholds, if needed
        if ( addThresholdsPerMetric && Objects.nonNull( metric.getThresholds() ) )
        {
            List<ThresholdsConfig> thresholds = metric.getThresholds();
            Set<Threshold> migratedThresholds = thresholds.stream()
                                                          .map( next -> DeclarationMigrator.migrateThresholds( next,
                                                                                                               builder,
                                                                                                               inline ) )
                                                          .flatMap( Set::stream )

                                                          .collect( Collectors.toSet() );
            // Group by threshold type
            Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> grouped
                    = DeclarationUtilities.groupThresholdsByType( migratedThresholds );

            // Set the thresholds
            if ( grouped.containsKey( ThresholdType.PROBABILITY ) )
            {
                parametersBuilder.probabilityThresholds( grouped.get( ThresholdType.PROBABILITY ) );
            }
            if ( grouped.containsKey( ThresholdType.VALUE ) )
            {
                parametersBuilder.thresholds( grouped.get( ThresholdType.VALUE ) );
            }
            if ( grouped.containsKey( ThresholdType.PROBABILITY_CLASSIFIER ) )
            {
                parametersBuilder.classifierThresholds( grouped.get( ThresholdType.PROBABILITY_CLASSIFIER ) );
            }
        }

        // Set the minimum sample size
        if ( Objects.nonNull( builder.minimumSampleSize() )
             && Objects.nonNull( metric.getMinimumSampleSize() )
             && !Objects.equals( builder.minimumSampleSize(),
                                 metric.getMinimumSampleSize() ) )
        {
            int sampleSize = Math.max( builder.minimumSampleSize(), metric.getMinimumSampleSize() );

            // Warn if there is a different sample size for each of several metric groups: migrate the largest
            LOGGER.warn( "Discovered more than one minimum sample size to migrate. Choosing the larger of the "
                         + "minimum sample sizes, which is {}.", sampleSize );
            builder.minimumSampleSize( sampleSize );
        }
        else
        {
            builder.minimumSampleSize( metric.getMinimumSampleSize() );
        }

        // Set the ensemble average
        if ( Objects.nonNull( metric.getEnsembleAverage() ) )
        {
            Pool.EnsembleAverageType average = Pool.EnsembleAverageType.valueOf( metric.getEnsembleAverage()
                                                                                       .name() );
            parametersBuilder.ensembleAverageType( average );
        }

        MetricParameters migratedParameters = parametersBuilder.build();

        // Do not set default parameters
        if ( DEFAULT_METRIC_PARAMETERS.equals( migratedParameters ) )
        {
            LOGGER.debug( "While migrating parameters, discovered default parameters values, which will not be "
                          + "migrated explicitly. The original declaration to migrate was: {}.", metric );
            migratedParameters = null;
        }

        return migratedParameters;
    }

    /**
     * Migrates old-style declaration of thresholds to canonical thresholds.
     * @param thresholds the thresholds to migrate
     * @param builder the builder, which may be updated with threshold service declaration
     * @param inline is true to migrate external CSV thresholds inline, false to use a {@link ThresholdSource}
     * @return the migrated thresholds
     */

    private static Set<Threshold> migrateThresholds( ThresholdsConfig thresholds,
                                                     EvaluationDeclarationBuilder builder,
                                                     boolean inline )
    {
        // Either an internal list of thresholds in CSV format or an external point, such as a file or service call,
        // which are not migrated here
        Object internalOrExternal = thresholds.getCommaSeparatedValuesOrSource();

        // Concrete thresholds?
        if ( internalOrExternal instanceof String csv )
        {
            return DeclarationMigrator.migrateCsvThresholds( csv, thresholds );
        }
        // External source of thresholds call to obtain concrete thresholds
        else if ( internalOrExternal instanceof ThresholdsConfig.Source source )
        {
            return DeclarationMigrator.migrateExternalThresholds( source, thresholds, builder, inline );
        }

        LOGGER.warn( "Discovered an unrecognized threshold source, which will not be migrated: {}.", thresholds );
        return Collections.emptySet();
    }

    /**
     * Migrates a CSV-formatted threshold string.
     *
     * @param thresholds the threshold string
     * @param metadata the threshold metadata
     * @return the thresholds
     */
    private static Set<Threshold> migrateCsvThresholds( String thresholds, ThresholdsConfig metadata )
    {
        LOGGER.debug( "Discovered a source of thresholds in CSV format to migrate: {}.", metadata );
        Set<Threshold> migrated = new LinkedHashSet<>();

        // Default threshold
        wres.statistics.generated.Threshold.Builder builder =
                DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder();

        // Need to map enums
        wres.config.xml.generated.ThresholdType type = DeclarationMigrator.getThresholdType( metadata );
        ThresholdType newType = ThresholdType.valueOf( type.name() );

        if ( Objects.nonNull( metadata.getOperator() ) )
        {
            wres.config.xml.generated.ThresholdOperator operator = metadata.getOperator();
            wres.statistics.generated.Threshold.ThresholdOperator canonicalOperator =
                    ProjectConfigs.getThresholdOperator( operator );
            builder.setOperator( canonicalOperator );
        }

        if ( Objects.nonNull( metadata.getApplyTo() ) )
        {
            ThresholdDataType dataType = metadata.getApplyTo();
            ThresholdOrientation orientation = ThresholdOrientation.valueOf( dataType.name() );
            wres.statistics.generated.Threshold.ThresholdDataType canonicalDataType = orientation.canonical();
            builder.setDataType( canonicalDataType );
        }

        // Read the threshold values
        double[] values = Arrays.stream( thresholds.split( "," ) )
                                .mapToDouble( Double::parseDouble )
                                .toArray();

        // Create the thresholds
        for ( double next : values )
        {
            // Clear existing values
            builder.clearLeftThresholdValue()
                   .clearLeftThresholdProbability();

            DoubleValue value = DoubleValue.of( next );

            if ( newType == ThresholdType.VALUE )
            {
                builder.setLeftThresholdValue( value );
            }
            else
            {
                builder.setLeftThresholdProbability( value );
            }

            wres.statistics.generated.Threshold migratedThreshold = builder.build();
            Threshold nextThreshold = ThresholdBuilder.builder()
                                                      .threshold( migratedThreshold )
                                                      .type( newType )
                                                      .build();
            migrated.add( nextThreshold );
        }

        LOGGER.debug( "Migrated the following thresholds: {}.", migrated );

        return Collections.unmodifiableSet( migrated );
    }

    /**
     * Attempts to migrate an external source of thresholds. If the thresholds are contained in a sidecar file, these
     * are read and returned. If a threshold service is configured, the service declaration is migrated, but no
     * thresholds are read.
     *
     * @param thresholdSource the external source of thresholds
     * @param thresholds the thresholds
     * @param builder the builder, which may be updated with threshold service declaration
     * @param inline is true to migrate external CSV thresholds inline, false to use a {@link ThresholdSource}
     * @return the thresholds, if any
     */

    private static Set<Threshold> migrateExternalThresholds( ThresholdsConfig.Source thresholdSource,
                                                             ThresholdsConfig thresholds,
                                                             EvaluationDeclarationBuilder builder,
                                                             boolean inline )
    {
        // Web service?
        if ( thresholdSource.getFormat() == ThresholdFormat.WRDS )
        {
            DeclarationMigrator.migrateThresholdsAsThresholdSource( thresholdSource, thresholds, builder );
            return Collections.emptySet();
        }
        // Attempt to read a source of thresholds in CSV format
        else
        {
            return DeclarationMigrator.migrateCsvThresholds( thresholdSource, thresholds, builder, inline );
        }
    }

    /**
     * Migrates an external source of CSV thresholds.
     * @param thresholdSource the external source of thresholds
     * @param thresholds the thresholds
     * @param builder the builder, which may be updated with threshold service declaration
     * @param inline is true to migrate external CSV thresholds inline, false to use a {@link ThresholdSource}
     * @return the thresholds, if any
     */

    private static Set<Threshold> migrateCsvThresholds( ThresholdsConfig.Source thresholdSource,
                                                        ThresholdsConfig thresholds,
                                                        EvaluationDeclarationBuilder builder,
                                                        boolean inline )
    {
        if ( inline )
        {
            return DeclarationMigrator.migrateCsvThresholdsInline( thresholdSource, thresholds, builder );
        }
        else
        {
            return migrateThresholdsAsThresholdSource( thresholdSource, thresholds, builder );
        }
    }

    /**
     * Migrates an external source of CSV thresholds inline to the declaration.
     * @param thresholdSource the external source of thresholds
     * @param thresholds the thresholds
     * @param builder the builder, which may be updated with threshold service declaration
     * @return the thresholds, if any
     */

    private static Set<Threshold> migrateCsvThresholdsInline( ThresholdsConfig.Source thresholdSource,
                                                              ThresholdsConfig thresholds,
                                                              EvaluationDeclarationBuilder builder )
    {
        wres.config.xml.generated.ThresholdType thresholdType = DeclarationMigrator.getThresholdType( thresholds );
        ThresholdType canonicalType = ThresholdType.valueOf( thresholdType.name() );

        try
        {
            String unit = builder.unit();
            if ( Objects.nonNull( thresholdSource.getUnit() ) && !thresholdSource.getUnit()
                                                                                 .isBlank() )
            {
                unit = thresholdSource.getUnit();
            }

            // Defaults to left in the old-style declaration schema
            DatasetOrientation featureNameFrom = DatasetOrientation.valueOf( thresholdSource.getFeatureNameFrom()
                                                                                            .name() );

            Set<Threshold> migrated = new LinkedHashSet<>();
            Map<String, Set<wres.statistics.generated.Threshold>> external =
                    CsvThresholdReader.readThresholds( thresholds, unit );
            for ( Map.Entry<String, Set<wres.statistics.generated.Threshold>> nextEntry : external.entrySet() )
            {
                String featureName = nextEntry.getKey();
                Geometry feature = Geometry.newBuilder()
                                           .setName( featureName )
                                           .build();
                Set<wres.statistics.generated.Threshold> toMigrate = nextEntry.getValue();
                Set<Threshold> innerMigrated = toMigrate.stream()
                                                        .map( next -> new Threshold( next,
                                                                                     canonicalType,
                                                                                     feature,
                                                                                     featureNameFrom ) )
                                                        .collect( Collectors.toCollection( LinkedHashSet::new ) );
                migrated.addAll( innerMigrated );
            }

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Read these thresholds from {}: {}.", thresholdSource.getValue(), migrated );
            }

            return Collections.unmodifiableSet( migrated );
        }
        catch ( IOException e )
        {
            throw new DeclarationException( "Encountered an error when attempting to migrate an external source "
                                            + "of thresholds from " + thresholdSource.getValue(), e );
        }
    }

    /**
     * Migrates an external source of thresholds as a {@link ThresholdSource}.
     * @param thresholdSource the external source of thresholds
     * @param thresholds the thresholds
     * @param builder the builder, which may be updated with threshold service declaration
     * @return the thresholds, if any
     */

    private static Set<Threshold> migrateThresholdsAsThresholdSource( ThresholdsConfig.Source thresholdSource,
                                                                      ThresholdsConfig thresholds,
                                                                      EvaluationDeclarationBuilder builder )
    {
        LOGGER.debug( "Discovered an external source of thresholds to migrate: {}.", thresholdSource );

        ThresholdSource source = DeclarationMigrator.migrateExternalThresholds( thresholdSource, thresholds );

        // Append the new source to any existing sources and set them
        Set<ThresholdSource> sources = new HashSet<>();
        if ( Objects.nonNull( builder.thresholdSources() ) )
        {
            sources.addAll( builder.thresholdSources() );
        }
        sources.add( source );
        builder.thresholdSources( sources );

        LOGGER.debug( "Migrated this threshold source declaration: {}.", source );

        // No thresholds migrated inline
        return Set.of();
    }

    /**
     * Migrates an external source of thresholds with a URI.
     * @param thresholdSource the threshold source
     * @param thresholds the thresholds
     * @return the migrated threshold source
     */

    private static ThresholdSource migrateExternalThresholds( ThresholdsConfig.Source thresholdSource,
                                                              ThresholdsConfig thresholds )
    {
        wres.config.xml.generated.ThresholdType thresholdType = DeclarationMigrator.getThresholdType( thresholds );
        ThresholdType canonicalType = ThresholdType.valueOf( thresholdType.name() );

        ThresholdOperator operator = DeclarationFactory.DEFAULT_THRESHOLD_OPERATOR;
        if ( Objects.nonNull( thresholds.getOperator() ) )
        {
            wres.config.xml.generated.ThresholdOperator oldOperator = thresholds.getOperator();
            wres.statistics.generated.Threshold.ThresholdOperator canonicalOperator =
                    ProjectConfigs.getThresholdOperator( oldOperator );
            operator = ThresholdOperator.valueOf( canonicalOperator.name() );
        }

        DatasetOrientation featureNameFrom = DeclarationFactory.DEFAULT_THRESHOLD_DATASET_ORIENTATION;
        if ( Objects.nonNull( thresholdSource.getFeatureNameFrom() ) )
        {
            featureNameFrom = DatasetOrientation.valueOf( thresholdSource.getFeatureNameFrom()
                                                                         .name() );
        }

        ThresholdOrientation applyTo = DeclarationFactory.DEFAULT_THRESHOLD_ORIENTATION;
        if ( Objects.nonNull( thresholds.getApplyTo() ) )
        {
            wres.statistics.generated.Threshold.ThresholdDataType dataType =
                    wres.statistics.generated.Threshold.ThresholdDataType.valueOf( thresholds.getApplyTo()
                                                                                             .name() );
            applyTo = ThresholdOrientation.valueOf( dataType.name() );
        }

        // Missing value?
        Double missingValue = null;

        if ( Objects.nonNull( thresholdSource.getMissingValue() ) )
        {
            missingValue = Double.parseDouble( thresholdSource.getMissingValue() );
        }

        return ThresholdSourceBuilder.builder()
                                     .uri( thresholdSource.getValue() )
                                     .type( canonicalType )
                                     .operator( operator )
                                     .applyTo( applyTo )
                                     .unit( thresholdSource.getUnit() )
                                     .featureNameFrom( featureNameFrom )
                                     .missingValue( missingValue )
                                     .parameter( thresholdSource.getParameterToMeasure() )
                                     .provider( thresholdSource.getProvider() )
                                     .ratingProvider( thresholdSource.getRatingProvider() )
                                     .build();
    }

    /**
     * Inspects the metrics and looks for a single/global set of thresholds containing the same collection of
     * {@link ThresholdsConfig} across all {@link MetricsConfig}. If there are no global thresholds, returns an empty
     * list. In that case, no thresholds are defined or there are different thresholds for different groups of metrics.
     *
     * @param metrics the metrics to inspect
     * @return the global thresholds, if available
     */

    private static List<ThresholdsConfig> getGlobalThresholds( List<MetricsConfig> metrics )
    {
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        for ( MetricsConfig nextMetrics : metrics )
        {
            List<ThresholdsConfig> innerThresholds = nextMetrics.getThresholds();
            if ( !thresholds.isEmpty()
                 && !thresholds.equals( innerThresholds ) )
            {
                return Collections.emptyList();
            }
            else
            {
                thresholds = innerThresholds;
            }
        }

        return Collections.unmodifiableList( thresholds );
    }

    /**
     * @param thresholds the thresholds
     * @return the threshold type
     */
    private static wres.config.xml.generated.ThresholdType getThresholdType( ThresholdsConfig thresholds )
    {
        // Defaults to probability
        wres.config.xml.generated.ThresholdType thresholdType = wres.config.xml.generated.ThresholdType.PROBABILITY;
        if ( Objects.nonNull( thresholds.getType() ) )
        {
            thresholdType = thresholds.getType();
        }
        return thresholdType;
    }

    /**
     * Do not construct.
     */

    private DeclarationMigrator()
    {
    }
}
