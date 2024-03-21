package wres.config.yaml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;
import wres.statistics.MessageFactory;

/**
 * A utility class for working with {@link EvaluationDeclaration}.
 * @author James Brown
 */
public class DeclarationUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DeclarationUtilities.class );

    /** Re-used message. */
    private static final String CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION = "Cannot determine time "
                                                                                         + "windows from missing "
                                                                                         + "declaration.";

    /**
     * Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation.
     * Returns at least one {@link TimeWindow}.
     *
     * @param declaration the declaration, cannot be null
     * @return a set of one or more time windows for evaluation
     * @throws NullPointerException if any required input is null
     */

    public static Set<TimeWindow> getTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        TimePools leadDurationPools = declaration.leadTimePools();
        TimePools referenceDatesPools = declaration.referenceDatePools();
        TimePools validDatesPools = declaration.validDatePools();

        // Has explicit pooling windows
        if ( Objects.nonNull( leadDurationPools )
             || Objects.nonNull( referenceDatesPools )
             || Objects.nonNull( validDatesPools ) )
        {
            // All dimensions
            if ( Objects.nonNull( referenceDatesPools ) && Objects.nonNull( validDatesPools )
                 && Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for reference dates and valid dates and lead durations." );

                return DeclarationUtilities.getReferenceDatesValidDatesAndLeadDurationTimeWindows( declaration );
            }
            // Reference dates and valid dates
            else if ( Objects.nonNull( referenceDatesPools ) && Objects.nonNull( validDatesPools ) )
            {
                LOGGER.debug( "Building time windows for reference dates and valid dates." );

                return DeclarationUtilities.getReferenceDatesAndValidDatesTimeWindows( declaration );
            }
            // Reference dates and lead durations
            else if ( Objects.nonNull( referenceDatesPools ) && Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for reference dates and lead durations." );

                return DeclarationUtilities.getReferenceDatesAndLeadDurationTimeWindows( declaration );
            }
            // Valid dates and lead durations
            else if ( Objects.nonNull( validDatesPools ) && Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for valid dates and lead durations." );

                return DeclarationUtilities.getValidDatesAndLeadDurationTimeWindows( declaration );
            }
            // Reference dates
            else if ( Objects.nonNull( referenceDatesPools ) )
            {
                LOGGER.debug( "Building time windows for reference dates." );

                return DeclarationUtilities.getReferenceDatesTimeWindows( declaration );
            }
            // Lead durations
            else if ( Objects.nonNull( leadDurationPools ) )
            {
                LOGGER.debug( "Building time windows for lead durations." );

                return DeclarationUtilities.getLeadDurationTimeWindows( declaration );
            }
            // Valid dates
            else
            {
                LOGGER.debug( "Building time windows for valid dates." );

                return DeclarationUtilities.getValidDatesTimeWindows( declaration );
            }
        }
        // One big pool
        else
        {
            LOGGER.debug( "Building one big time window." );

            return Collections.singleton( DeclarationUtilities.getOneBigTimeWindow( declaration ) );
        }
    }

    /**
     * <p>Builds a {@link TimeWindow} whose {@link TimeWindow#getEarliestReferenceTime()} and
     * {@link TimeWindow#getLatestReferenceTime()} return the {@code earliest} and {@code latest} bookends of the
     * {@link EvaluationDeclaration#referenceDates()}, respectively, whose {@link TimeWindow#getEarliestValidTime()}
     * and {@link TimeWindow#getLatestValidTime()} return the {@code earliest} and {@code latest} bookends of the
     * {@link EvaluationDeclaration#validDates()}, respectively, and whose {@link TimeWindow#getEarliestLeadDuration()}
     * and {@link TimeWindow#getLatestLeadDuration()} return the {@code minimum} and {@code maximum} bookends of the
     * {@link EvaluationDeclaration#leadTimes()}, respectively.
     *
     * <p>If any of these variables are missing from the input, defaults are used, which represent the
     * computationally-feasible limiting values. For example, the smallest and largest possible instant is
     * {@link Instant#MIN} and {@link Instant#MAX}, respectively. The smallest and largest possible {@link Duration} is
     * {@link MessageFactory#DURATION_MIN} and {@link MessageFactory#DURATION_MAX}, respectively.
     *
     * @param declaration the declaration
     * @return a time window
     * @throws NullPointerException if the pairConfig is null
     */

    public static TimeWindow getOneBigTimeWindow( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Instant earliestReferenceTime = Instant.MIN;
        Instant latestReferenceTime = Instant.MAX;
        Instant earliestValidTime = Instant.MIN;
        Instant latestValidTime = Instant.MAX;
        Duration smallestLeadDuration = MessageFactory.DURATION_MIN;
        Duration largestLeadDuration = MessageFactory.DURATION_MAX;

        // Reference datetimes
        if ( Objects.nonNull( declaration.referenceDates() ) )
        {
            if ( Objects.nonNull( declaration.referenceDates()
                                             .minimum() ) )
            {
                earliestReferenceTime = declaration.referenceDates()
                                                   .minimum();
            }
            if ( Objects.nonNull( declaration.referenceDates()
                                             .maximum() ) )
            {
                latestReferenceTime = declaration.referenceDates()
                                                 .maximum();
            }
        }

        // Valid datetimes
        if ( Objects.nonNull( declaration.validDates() ) )
        {
            if ( Objects.nonNull( declaration.validDates()
                                             .minimum() ) )
            {
                earliestValidTime = declaration.validDates()
                                               .minimum();
            }
            if ( Objects.nonNull( declaration.validDates()
                                             .maximum() ) )
            {
                latestValidTime = declaration.validDates()
                                             .maximum();
            }
        }

        // Lead durations
        if ( Objects.nonNull( declaration.leadTimes() ) )
        {
            if ( Objects.nonNull( declaration.leadTimes()
                                             .minimum() ) )
            {
                smallestLeadDuration = declaration.leadTimes()
                                                  .minimum();
            }
            if ( Objects.nonNull( declaration.leadTimes()
                                             .maximum() ) )
            {
                largestLeadDuration = declaration.leadTimes()
                                                 .maximum();
            }
        }

        return MessageFactory.getTimeWindow( earliestReferenceTime,
                                             latestReferenceTime,
                                             earliestValidTime,
                                             latestValidTime,
                                             smallestLeadDuration,
                                             largestLeadDuration );
    }

    /**
     * @param evaluation the evaluation
     * @return whether a baseline dataset has been declared
     * @throws NullPointerException if the input is null
     */
    public static boolean hasBaseline( EvaluationDeclaration evaluation )
    {
        Objects.requireNonNull( evaluation );

        return Objects.nonNull( evaluation.baseline() );
    }

    /**
     * Determines whether the declaration contains feature groups.
     * @param declaration the declaration, not null
     * @return whether the declaration contains feature groups
     * @throws NullPointerException if the declaration is null
     */
    public static boolean hasFeatureGroups( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        return Objects.nonNull( declaration.featureGroups() )
               || ( Objects.nonNull( declaration.featureService() )
                    && declaration.featureService()
                                  .featureGroups()
                                  .stream()
                                  .anyMatch( FeatureServiceGroup::pool ) );
    }

    /**
     * Determines whether any datasets have an unknown data type.
     *
     * @param evaluation the evaluation
     * @return whether any data types are unknown
     * @throws NullPointerException if the input is null
     */

    public static boolean hasMissingDataTypes( EvaluationDeclaration evaluation )
    {
        Objects.requireNonNull( evaluation );

        return Objects.isNull( evaluation.left()
                                         .type() )
               || Objects.isNull( evaluation.right()
                                            .type() )
               || ( DeclarationUtilities.hasBaseline( evaluation )
                    && Objects.isNull( evaluation.baseline()
                                                 .dataset()
                                                 .type() ) )
               || evaluation.covariates()
                            .stream()
                            .anyMatch( c -> Objects.isNull( c.dataset()
                                                             .type() ) );
    }

    /**
     * Returns the features from all contexts in the declaration that declare features explicitly. Does not consider
     * features that are declared implicitly, such as those associated with featureful thresholds.
     * @param declaration the declaration
     * @return the features from all contexts
     * @throws NullPointerException if the input is null
     */

    public static Set<GeometryTuple> getFeatures( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        Set<GeometryTuple> tuples = new HashSet<>();
        if ( Objects.nonNull( declaration.features() ) )
        {
            Set<GeometryTuple> singletons = declaration.features()
                                                       .geometries();
            tuples.addAll( singletons );
        }

        if ( Objects.nonNull( declaration.featureGroups() ) )
        {
            Set<GeometryTuple> grouped = declaration.featureGroups()
                                                    .geometryGroups()
                                                    .stream()
                                                    .flatMap( next -> next.getGeometryTuplesList()
                                                                          .stream() )
                                                    .collect( Collectors.toSet() );
            tuples.addAll( grouped );
        }

        return Collections.unmodifiableSet( tuples );
    }

    /**
     * Creates a geometry from a WKT string and optional Spatial Reference system Identifier (SRID).
     * @param wktString the WKT string
     * @param srid the optional SRID
     * @return the geometry
     */
    public static org.locationtech.jts.geom.Geometry getGeometry( String wktString, Long srid )
    {
        WKTReader reader = new WKTReader();
        org.locationtech.jts.geom.Geometry mask;
        try
        {
            mask = reader.read( wktString );
            if ( Objects.nonNull( srid ) )
            {
                mask.setSRID( srid.intValue() );
            }
        }
        catch ( ParseException e )
        {
            throw new IllegalArgumentException( "Failed to parse the mask geometry: " + wktString );
        }

        return mask;
    }

    /**
     * Returns the thresholds from all contexts in the declaration.
     * @param declaration the declaration
     * @return the features from all contexts
     * @throws NullPointerException if the input is null
     */

    public static Set<Threshold> getThresholds( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        Set<Threshold> thresholds = new HashSet<>();

        thresholds.addAll( declaration.thresholds() );
        thresholds.addAll( declaration.probabilityThresholds() );
        thresholds.addAll( declaration.classifierThresholds() );
        thresholds.addAll( declaration.thresholdSets() );

        // Map from metric parameters to thresholds
        Function<MetricParameters, Set<Threshold>> parMapper = parameters ->
        {
            Set<Threshold> metricThresholds = new HashSet<>();
            metricThresholds.addAll( parameters.classifierThresholds() );
            metricThresholds.addAll( parameters.thresholds() );
            metricThresholds.addAll( parameters.probabilityThresholds() );
            return metricThresholds;
        };

        Set<Threshold> byMetric = declaration.metrics()
                                             .stream()
                                             .map( Metric::parameters )
                                             .filter( Objects::nonNull )
                                             .map( parMapper )
                                             .flatMap( Set::stream )
                                             .collect( Collectors.toSet() );

        thresholds.addAll( byMetric );

        return Collections.unmodifiableSet( thresholds );
    }

    /**
     * Returns the time scales associated with all declared data sources, if any.
     * @param declaration the declaration
     * @return the time scales
     * @throws NullPointerException if the declaration is null
     */

    public static Set<TimeScale> getSourceTimeScales( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        Set<TimeScale> timeScales = new HashSet<>();

        // Left timescale?
        if ( Objects.nonNull( declaration.left()
                                         .timeScale() ) )
        {
            TimeScale left = declaration.left()
                                        .timeScale()
                                        .timeScale();
            timeScales.add( left );
        }

        // Right timescale?
        if ( Objects.nonNull( declaration.right()
                                         .timeScale() ) )
        {
            TimeScale right = declaration.right()
                                         .timeScale()
                                         .timeScale();
            timeScales.add( right );
        }

        // Baseline timescale?
        if ( DeclarationUtilities.hasBaseline( declaration )
             && Objects.nonNull( declaration.baseline()
                                            .dataset()
                                            .timeScale() ) )
        {
            TimeScale baseline = declaration.baseline()
                                            .dataset()
                                            .timeScale()
                                            .timeScale();
            timeScales.add( baseline );
        }

        return Collections.unmodifiableSet( timeScales );
    }

    /**
     * Determines whether the dataset is a forecast dataset
     * @param dataset the dataset
     * @return whether the dataset is a forecast dataset
     * @throws NullPointerException if the dataset is null
     */
    public static boolean isForecast( Dataset dataset )
    {
        Objects.requireNonNull( dataset );

        return Objects.nonNull( dataset.type() ) && dataset.type() == DataType.ENSEMBLE_FORECASTS
               || dataset.type() == DataType.SINGLE_VALUED_FORECASTS;
    }

    /**
     * Returns the earliest analysis duration associated with the project.
     *
     * @param declaration the project declaration
     * @return the earliest analysis duration
     * @throws NullPointerException if the declaration is null
     */

    public static Duration getEarliestAnalysisDuration( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        Duration returnMe = null;

        if ( Objects.nonNull( declaration.analysisTimes() ) )
        {
            AnalysisTimes durations = declaration.analysisTimes();
            returnMe = durations.minimum();
        }

        if ( Objects.isNull( returnMe ) )
        {
            returnMe = MessageFactory.DURATION_MIN;
        }

        return returnMe;
    }

    /**
     * Returns the latest analysis duration associated with the project.
     *
     * @param declaration the project declaration
     * @return the latest analysis duration
     * @throws NullPointerException if the declaration is null
     */

    public static Duration getLatestAnalysisDuration( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        Duration returnMe = null;

        if ( Objects.nonNull( declaration.analysisTimes() ) )
        {
            AnalysisTimes durations = declaration.analysisTimes();
            returnMe = durations.maximum();
        }

        if ( Objects.isNull( returnMe ) )
        {
            returnMe = MessageFactory.DURATION_MAX;
        }

        return returnMe;
    }

    /**
     * @param declaration the project declaration
     * @return The earliest possible day in a season or null
     * @throws NullPointerException if the declaration is null
     */

    public static MonthDay getStartOfSeason( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        MonthDay earliest = null;

        Season season = declaration.season();

        if ( Objects.nonNull( season ) )
        {
            earliest = season.minimum();
        }

        return earliest;
    }

    /**
     * @param declaration the project declaration
     * @return The latest possible day in a season or null
     * @throws NullPointerException if the declaration is null
     */

    public static MonthDay getEndOfSeason( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        MonthDay latest = null;

        Season season = declaration.season();

        if ( Objects.nonNull( season ) )
        {
            latest = season.maximum();
        }

        return latest;
    }

    /**
     * Return <code>true</code> if the declaration uses probability thresholds, otherwise <code>false</code>.
     *
     * @param declaration the project declaration
     * @return whether the project uses probability thresholds
     * @throws NullPointerException if the declaration is null
     */
    public static boolean hasProbabilityThresholds( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        // Top-level probability thresholds?
        if ( !declaration.probabilityThresholds().isEmpty() )
        {
            return true;
        }

        // Threshold sets with probability thresholds?
        if ( declaration.thresholdSets()
                        .stream()
                        .anyMatch( next -> next.type() == ThresholdType.PROBABILITY ) )
        {
            return true;
        }

        // Threshold sources with probability thresholds?
        if ( declaration.thresholdSources()
                        .stream()
                        .anyMatch( next -> next.type() == ThresholdType.PROBABILITY ) )
        {
            return true;
        }

        // Individual metrics with probability thresholds?
        return declaration.metrics()
                          .stream()
                          .map( Metric::parameters )
                          .filter( Objects::nonNull )
                          .anyMatch( next -> !next.probabilityThresholds().isEmpty() );
    }

    /**
     * Looks for a variable name.
     * @param dataset the dataset
     * @return the declared variable name or null if no variable was declared
     * @throws NullPointerException if the dataset is null
     */

    public static String getVariableName( Dataset dataset )
    {
        Objects.requireNonNull( dataset );

        String variableName = null;

        if ( Objects.nonNull( dataset.variable() ) )
        {
            variableName = dataset.variable()
                                  .name();
        }

        return variableName;
    }

    /**
     * Returns <code>true</code> if a generated baseline is required, otherwise <code>false</code>.
     *
     * @param baselineDataset the declaration to inspect
     * @return true if a generated baseline is required
     */

    public static boolean hasGeneratedBaseline( BaselineDataset baselineDataset )
    {
        return Objects.nonNull( baselineDataset )
               && Objects.nonNull( baselineDataset.generatedBaseline() );
    }

    /**
     * Gets a human-friendly enum name from an enum string. The reverse of {@link #toEnumName(String)}.
     * @param enumString the enum string
     * @return a human-friendly name with spaces instead of underscores and all lower case
     * @throws NullPointerException if the input is null
     */

    public static String fromEnumName( String enumString )
    {
        Objects.requireNonNull( enumString );

        return enumString.replace( "_", " " )
                         .toLowerCase();
    }

    /**
     * Returns an enumeration name from an informal string. The reverse of {@link DeclarationUtilities#fromEnumName(String)}.
     * @param name the name
     * @return the enum-friendly name
     * @throws NullPointerException if the input is null
     */

    public static String toEnumName( String name )
    {
        Objects.requireNonNull( name );

        return name.toUpperCase()
                   .replace( " ", "_" );
    }

    /**
     * Returns a string representation of the duration in hours if possible, otherwise seconds. Warns if this will
     * result in a loss of precision.
     *
     * @param duration the duration to serialize
     * @return the duration string and associated units
     */

    public static Pair<Long, String> getDurationInPreferredUnits( Duration duration )
    {
        // Loss of precision?
        if ( duration.toNanosPart() > 0 )
        {
            LOGGER.warn( "Received a duration of {}, which  is not exactly divisible by hours or seconds. The "
                         + "nanosecond part of this duration is {} and will not be serialized.", duration,
                         duration.toNanosPart() );
        }

        // Integer number of hours?
        if ( duration.toSeconds() % 3600L == 0 )
        {
            return Pair.of( duration.toHours(), "hours" );
        }

        return Pair.of( duration.toSeconds(), "seconds" );
    }

    /**
     * Acquires the feature names for the required data orientation.
     * @param features the features
     * @param orientation the data orientation
     * @return the feature names
     */

    public static Set<String> getFeatureNamesFor( Set<GeometryTuple> features, DatasetOrientation orientation )
    {
        return switch ( orientation )
        {
            case LEFT -> features.stream()
                                 .map( GeometryTuple::getLeft )
                                 .map( Geometry::getName )
                                 .collect( Collectors.toSet() );
            case RIGHT -> features.stream()
                                  .map( GeometryTuple::getRight )
                                  .map( Geometry::getName )
                                  .collect( Collectors.toSet() );
            case BASELINE -> features.stream()
                                     .map( GeometryTuple::getBaseline )
                                     .map( Geometry::getName )
                                     .collect( Collectors.toSet() );
            default -> throw new IllegalArgumentException( "Unrecognized dataset orientation in this context: "
                                                           + orientation );
        };
    }

    /**
     * Acquires the feature authority for the required data orientation.
     * @param evaluation the evaluation
     * @param orientation the data orientation
     * @return the feature authority or null if none was discovered
     * @throws NullPointerException if either input is null
     */

    public static FeatureAuthority getFeatureAuthorityFor( EvaluationDeclaration evaluation,
                                                           DatasetOrientation orientation )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( orientation );

        return switch ( orientation )
        {
            case LEFT -> evaluation.left()
                                   .featureAuthority();
            case RIGHT -> evaluation.right()
                                    .featureAuthority();
            case BASELINE -> evaluation.baseline()
                                       .dataset()
                                       .featureAuthority();
            default -> throw new IllegalArgumentException( "Unrecognized dataset orientation in this context: "
                                                           + orientation );
        };
    }

    /**
     * <p>Groups the metrics by their common parameters. Only considers parameters that are involved in slicing or
     * transforming pairs, such as thresholds or the type of ensemble average to calculate. In short, the metrics
     * that belong to a single group represent an atomic set for processing because they require common pairs.
     *
     * <p>In addition, expands any time-series summary statistics into their equivalent, top-level metrics, ready
     * to process.
     *
     * @param metrics the metrics to group
     * @return the fully expanded metrics, grouped by common parameters (that are used for slicing or transforming)
     */

    public static Set<Set<Metric>> getMetricGroupsForProcessing( Set<Metric> metrics )
    {
        Objects.requireNonNull( metrics );

        // Create the grouping function
        Function<Metric, MetricParameters> classifier = metric ->
        {
            MetricParametersBuilder builder = MetricParametersBuilder.builder();
            if ( Objects.nonNull( metric.parameters() ) )
            {
                MetricParameters existing = metric.parameters();
                builder.thresholds( existing.thresholds() );
                builder.probabilityThresholds( existing.probabilityThresholds() );
                builder.classifierThresholds( existing.classifierThresholds() );
                builder.ensembleAverageType( existing.ensembleAverageType() );
            }
            return builder.build();
        };

        // Group the metrics
        Map<MetricParameters, Set<Metric>> grouped = metrics.stream()
                                                            .collect( Collectors.groupingBy( classifier,
                                                                                             Collectors.toSet() ) );

        LOGGER.debug( "Grouped the metrics into {} groups whose data transformation parameters are consistent. Each "
                      + "of these groups is an atomic collection of metrics for processing. The metric groups are: {}.",
                      grouped.size(),
                      grouped );

        Set<Set<Metric>> toExpand = Set.copyOf( grouped.values() );

        return DeclarationUtilities.expand( toExpand );
    }

    /**
     * Determines whether the input is a valid path to a readable file.
     *
     * @param fileSystem the file system
     * @param possibleFile a possible path to a readable file
     * @return whether the input is a valid path to a readable file
     */

    public static boolean isReadableFile( FileSystem fileSystem, String possibleFile )
    {
        if ( Objects.isNull( fileSystem ) || Objects.isNull( possibleFile ) )
        {
            return false;
        }

        try
        {
            Path path = fileSystem.getPath( possibleFile );
            LOGGER.debug( "Inspecting path {} for a readable file.", path );
            return Files.isReadable( path );
        }
        catch ( InvalidPathException | SecurityException e )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                String line = DeclarationUtilities.getFirstLine( possibleFile );
                LOGGER.debug( "The supplied string is not a valid path to a readable file. The first line is: {}",
                              line );
            }
            return false;
        }
    }

    /**
     * Makes a best guess at the type of reference time from the data source type.
     * @param dataType the data type
     * @return the reference time type
     * @throws NullPointerException if the input is null
     */

    public static ReferenceTime.ReferenceTimeType getReferenceTimeType( DataType dataType )
    {
        Objects.requireNonNull( dataType );

        return switch ( dataType )
        {
            case ANALYSES, SIMULATIONS, OBSERVATIONS -> ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME;
            case ENSEMBLE_FORECASTS, SINGLE_VALUED_FORECASTS -> ReferenceTime.ReferenceTimeType.T0;
        };
    }

    /**
     * Infers the threshold type from the named context or node name in a declaration string.
     * @param context the named context
     * @return the threshold type
     */

    public static ThresholdType getThresholdType( String context )
    {
        if ( context.contains( "classifier" ) )
        {
            return ThresholdType.PROBABILITY_CLASSIFIER;
        }
        else if ( context.contains( "probability" ) )
        {
            return ThresholdType.PROBABILITY;
        }

        return ThresholdType.VALUE;
    }

    /**
     * Adds the URIs to the supplied declaration, correlating the source names with any existing sources, as needed.
     * Correlation of sources is based on the presence of the path element of a declared source URI within a supplied
     * URI.
     *
     * @param evaluation the existing evaluation declaration, required
     * @param leftSources the left sources, required
     * @param rightSources the right sources, required
     * @param baselineSources the baseline sources, optional
     * @throws IllegalArgumentException if baseline sources are provided and there is no baseline dataset
     * @throws NullPointerException if any required input is null
     * @return the adjusted declaration
     */

    public static EvaluationDeclaration addDataSources( EvaluationDeclaration evaluation,
                                                        List<URI> leftSources,
                                                        List<URI> rightSources,
                                                        List<URI> baselineSources )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( leftSources );
        Objects.requireNonNull( rightSources );
        Objects.requireNonNull( baselineSources );

        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( evaluation );
        DatasetBuilder leftBuilder = DatasetBuilder.builder();
        DatasetBuilder rightBuilder = DatasetBuilder.builder();

        // Datasets present?
        if ( Objects.nonNull( builder.left() ) )
        {
            leftBuilder = DatasetBuilder.builder( evaluation.left() );
        }
        if ( Objects.nonNull( builder.right() ) )
        {
            rightBuilder = DatasetBuilder.builder( evaluation.right() );
        }

        // Adjust the left and right datasets
        DeclarationUtilities.addDataSources( leftBuilder, leftSources, DatasetOrientation.LEFT );
        DeclarationUtilities.addDataSources( rightBuilder, rightSources, DatasetOrientation.RIGHT );

        // Add them
        builder.left( leftBuilder.build() )
               .right( rightBuilder.build() );

        // Baseline sources to add?
        if ( !baselineSources.isEmpty() )
        {
            DatasetBuilder baselineBuilder = DatasetBuilder.builder();
            BaselineDataset baseline = evaluation.baseline();

            // Existing baseline?
            if ( DeclarationUtilities.hasBaseline( evaluation )
                 && Objects.nonNull( baseline.dataset() ) )
            {
                baselineBuilder = DatasetBuilder.builder( baseline.dataset() );
            }
            // No, so set an empty one
            else
            {
                baseline = BaselineDatasetBuilder.builder()
                                                 .dataset( baselineBuilder.build() )
                                                 .build();
            }

            DeclarationUtilities.addDataSources( baselineBuilder, baselineSources, DatasetOrientation.BASELINE );
            BaselineDataset adjustedBaseline = BaselineDatasetBuilder.builder( baseline ) // Existing, if any
                                                                     .dataset( baselineBuilder.build() )
                                                                     .build();
            builder.baseline( adjustedBaseline );
        }

        return builder.build();
    }

    /**
     * Adds the prescribed thresholds to the declaration.
     *
     * @param declaration the declaration to adjust
     * @param thresholds the thresholds to add
     * @return the adjusted declaration with thresholds added
     * @throws NullPointerException if any input is null
     */

    public static EvaluationDeclaration addThresholds( EvaluationDeclaration declaration,
                                                       Set<Threshold> thresholds )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( thresholds );

        LOGGER.debug( "Adding {} thresholds to the supplied declaration: {}.", thresholds.size(), thresholds );

        // Group the thresholds by type
        Map<ThresholdType, Set<Threshold>> thresholdsByType = DeclarationUtilities.groupThresholdsByType( thresholds );
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );

        Set<Threshold> valueThresholds = new HashSet<>();
        Set<Threshold> probabilityThresholds = new HashSet<>();
        Set<Threshold> classifierThresholds = new HashSet<>();

        // Add the ordinary value thresholds to their global context
        if ( thresholdsByType.containsKey( ThresholdType.VALUE ) )
        {
            Set<Threshold> supplied = thresholdsByType.get( ThresholdType.VALUE );
            valueThresholds.addAll( supplied );
            valueThresholds.addAll( builder.thresholds() );
            builder.thresholds( valueThresholds );
        }

        // Add the probability thresholds to their global context
        if ( thresholdsByType.containsKey( ThresholdType.PROBABILITY ) )
        {
            Set<Threshold> supplied = thresholdsByType.get( ThresholdType.PROBABILITY );
            probabilityThresholds.addAll( supplied );
            probabilityThresholds.addAll( builder.probabilityThresholds() );
            builder.probabilityThresholds( probabilityThresholds );
        }

        // Add the classifier thresholds to their global context
        if ( thresholdsByType.containsKey( ThresholdType.PROBABILITY_CLASSIFIER ) )
        {
            Set<Threshold> supplied = thresholdsByType.get( ThresholdType.PROBABILITY_CLASSIFIER );
            classifierThresholds.addAll( supplied );
            classifierThresholds.addAll( builder.classifierThresholds() );
            builder.classifierThresholds( classifierThresholds );
        }

        // Add the thresholds to the individual metrics and return
        DeclarationInterpolator.addThresholdsToMetrics( thresholdsByType, builder, false, true );

        return builder.build();
    }

    /**
     * Removes from the declaration any features for which featureful thresholds are undefined. Features are matched on
     * name only.
     * @param declaration the declaration
     * @return the adjusted declaration
     * @throws NullPointerException if the declaration is null
     */

    public static EvaluationDeclaration removeFeaturesWithoutThresholds( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        if ( Objects.isNull( declaration.features() )
             && Objects.isNull( declaration.featureGroups() ) )
        {
            LOGGER.debug( "No features were discovered to filter against thresholds." );
        }

        Set<Threshold> thresholds = DeclarationUtilities.getThresholds( declaration );

        // Get the names of left-ish features with thresholds
        Set<String> leftFeatureNamesWithThresholds = thresholds.stream()
                                                               .filter( n -> n.featureNameFrom()
                                                                             == DatasetOrientation.LEFT )
                                                               .map( n -> n.feature()
                                                                           .getName() )
                                                               .collect( Collectors.toSet() );
        Set<String> rightFeatureNamesWithThresholds = thresholds.stream()
                                                                .filter( n -> n.featureNameFrom()
                                                                              == DatasetOrientation.RIGHT )
                                                                .map( n -> n.feature()
                                                                            .getName() )
                                                                .collect( Collectors.toSet() );
        Set<String> baselineFeatureNamesWithThresholds = thresholds.stream()
                                                                   .filter( n -> n.featureNameFrom()
                                                                                 == DatasetOrientation.BASELINE )
                                                                   .map( n -> n.feature()
                                                                               .getName() )
                                                                   .collect( Collectors.toSet() );

        // Create a filter. Only filter when the above sets contain some features
        Predicate<GeometryTuple> retain = geoTuple ->
        {
            if ( geoTuple.hasLeft()
                 && !leftFeatureNamesWithThresholds.isEmpty()
                 && !leftFeatureNamesWithThresholds.contains( geoTuple.getLeft()
                                                                      .getName() ) )
            {
                return false;
            }

            if ( geoTuple.hasRight()
                 && !rightFeatureNamesWithThresholds.isEmpty()
                 && !rightFeatureNamesWithThresholds.contains( geoTuple.getRight()
                                                                       .getName() ) )
            {
                return false;
            }

            return !DeclarationUtilities.hasBaseline( declaration )
                   || !geoTuple.hasBaseline()
                   || baselineFeatureNamesWithThresholds.isEmpty()
                   || baselineFeatureNamesWithThresholds.contains( geoTuple.getBaseline()
                                                                           .getName() );
        };

        // Remove the features in all contexts
        return DeclarationUtilities.removeFeaturesWithoutThresholds( declaration, retain );
    }

    /**
     * Determines whether the declaration string is an old-style declaration string.
     * @param mediaType the media type associated with the declaration string
     * @param declarationString the declaration string
     * @return whether the string is an old-style declaration string
     * @throws NullPointerException if either input is null
     */

    public static boolean isOldDeclarationString( MediaType mediaType, String declarationString )
    {
        Objects.requireNonNull( mediaType );
        Objects.requireNonNull( declarationString );

        String withoutWhiteSpace = declarationString.trim();

        // Permissive check because a string without <?xml version="1.0" encoding="UTF-8"?> will still parse correctly,
        // even though the content type will not be detected correctly. The first check deals with that scenario and
        // the second check deals with a correctly detected content type.
        return withoutWhiteSpace.startsWith( "<project" )
               || ( "application".equals( mediaType.getType() )
                    && "xml".equals( mediaType.getSubtype() ) )
               || withoutWhiteSpace.startsWith( "<?xml" ); // When tika fails
    }

    /**
     * Inspects the string for MIME type.
     * @param declarationString the declaration
     * @return the media type
     * @throws IOException if the content could not be parsed for detection
     */

    public static MediaType getMediaType( String declarationString ) throws IOException
    {
        String withoutWhitespace = declarationString.trim();
        try ( InputStream inputStream = new ByteArrayInputStream( withoutWhitespace.getBytes() ) )
        {
            Metadata metadata = new Metadata();
            TikaConfig tikaConfig = new TikaConfig();
            Detector detector = tikaConfig.getDetector();
            MediaType detectedMediaType = detector.detect( inputStream, metadata );

            LOGGER.debug( "The detected MIME type of the declaration string was {} and the subtype was {}.",
                          detectedMediaType.getType(),
                          detectedMediaType.getSubtype() );

            return detectedMediaType;
        }
        catch ( TikaException e )
        {
            throw new IOException( "Failed to detect the MIME type of the declaration string: " + declarationString );
        }
    }

    /**
     * Returns the declared dataset for a given orientation. Cannot be applied to {@link DatasetOrientation#COVARIATE},
     * which may contain up to N datasets.
     * @param declaration the declaration
     * @param orientation the orientation
     * @return the dataset
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the requested orientation cannot be delivered
     */

    public static Dataset getDeclaredDataset( EvaluationDeclaration declaration, DatasetOrientation orientation )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( orientation );

        switch ( orientation )
        {
            case LEFT ->
            {
                return declaration.left();
            }
            case RIGHT ->
            {
                return declaration.right();
            }
            case BASELINE ->
            {
                if ( !hasBaseline( declaration ) )
                {
                    throw new IllegalArgumentException( "The declaration does not contain a 'baseline' dataset: "
                                                        + orientation );
                }
                return declaration.baseline()
                                  .dataset();
            }
            default -> throw new IllegalArgumentException( "Unexpected dataset orientation in this context: "
                                                           + orientation );
        }
    }

    /**
     * @param builder the builder
     * @return whether a baseline dataset has been declared
     * @throws NullPointerException if the input is null
     */
    static boolean hasBaseline( EvaluationDeclarationBuilder builder )
    {
        Objects.requireNonNull( builder );

        return Objects.nonNull( builder.baseline() );
    }

    /**
     * Inspects the dataset for an explicitly declared feature authority, else attempts to interpolate the authority
     * from the other information present.
     *
     * @param dataset the dataset
     * @return the set of explicitly declared or implicitly declared feature authorities associated with the dataset
     * @throws NullPointerException if the input is null
     */

    static Set<FeatureAuthority> getFeatureAuthorities( Dataset dataset )
    {
        Objects.requireNonNull( dataset );

        // Explicit authority?
        if ( Objects.nonNull( dataset.featureAuthority() ) )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Discovered an explicitly declared feature authority of '{}' for the dataset labelled "
                              + "'{}'.",
                              dataset.featureAuthority(), dataset.label() );
            }

            return Set.of( dataset.featureAuthority() );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Found no explicitly declared feature authority for the dataset labelled '{}' Will attempt "
                          + "to infer the authority from the other information present.",
                          dataset.label() );
        }

        // Try to work out the authority from the sources
        return dataset.sources()
                      .stream()
                      .map( DeclarationUtilities::getFeatureAuthorityFromSource )
                      .filter( Objects::nonNull )
                      .collect( Collectors.toUnmodifiableSet() );

    }

    /**
     * @param declaration the declaration
     * @return whether analysis durations have been declared
     */

    static boolean hasAnalysisTimes( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return hasAnalysisTimes( builder );
    }

    /**
     * @param builder the builder
     * @return whether analysis durations have been declared
     */

    static boolean hasAnalysisTimes( EvaluationDeclarationBuilder builder )
    {
        return Objects.nonNull( builder.analysisTimes() ) && (
                Objects.nonNull( builder.analysisTimes()
                                        .minimum() )
                || Objects.nonNull( builder.analysisTimes()
                                           .maximum() ) );
    }

    /**
     * Groups the thresholds by threshold type.
     * @param thresholds the thresholds
     * @return the thresholds grouped by type
     */

    static Map<ThresholdType, Set<Threshold>> groupThresholdsByType( Set<Threshold> thresholds )
    {
        return thresholds.stream()
                         .filter( t -> Objects.nonNull( t.type() ) )
                         .collect( Collectors.groupingBy( Threshold::type,
                                                          Collectors.mapping( Function.identity(),
                                                                              Collectors.toCollection(
                                                                                      LinkedHashSet::new ) ) ) );
    }

    /**
     * Returns a string representation of each ensemble declaration item discovered.
     * @param declaration the declaration
     * @return the ensemble declaration was found
     */

    static Set<String> getEnsembleDeclaration( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return DeclarationUtilities.getEnsembleDeclaration( builder );
    }

    /**
     * Returns a string representation of each forecast declaration item discovered.
     * @param declaration the declaration
     * @return the forecast declaration strings, if any
     */

    static Set<String> getForecastDeclaration( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return DeclarationUtilities.getForecastDeclaration( builder );
    }

    /**
     * Returns a string representation of each ensemble declaration item discovered.
     * @param builder the builder
     * @return the ensemble declaration was found
     */

    static Set<String> getEnsembleDeclaration( EvaluationDeclarationBuilder builder )
    {
        Set<String> ensembleDeclaration = new TreeSet<>();

        // Explicit ensemble type declared for the predicted data?
        if ( builder.right()
                    .type() == DataType.ENSEMBLE_FORECASTS )
        {
            ensembleDeclaration.add( "The 'type' of 'predicted' data was 'ensemble forecasts'." );
        }

        // Explicit ensemble type declared for the baseline data?
        if ( DeclarationUtilities.hasBaseline( builder )
             && builder.baseline()
                       .dataset()
                       .type() == DataType.ENSEMBLE_FORECASTS )
        {
            ensembleDeclaration.add( "The 'type' of 'baseline' data was 'ensemble forecasts'." );
        }

        // Ensemble filter on predicted dataset?
        if ( Objects.nonNull( builder.right()
                                     .ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the predicted dataset." );
        }

        // Ensemble filter on baseline dataset?
        if ( DeclarationUtilities.hasBaseline( builder )
             && Objects.nonNull( builder.baseline()
                                        .dataset()
                                        .ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the baseline dataset." );
        }

        // Any source that contains an interface with the word ensemble?
        Set<String> ensembleInterfaces = DeclarationUtilities.getSourcesWithEnsembleInterface( builder );
        if ( !ensembleInterfaces.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered one or more data sources whose interfaces are ensemble-like: "
                                     + ensembleInterfaces
                                     + "." );
        }

        // Ensemble average declared?
        if ( Objects.nonNull( builder.ensembleAverageType() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_average' was declared." );
        }

        // Ensemble metrics?  Ignore metrics that also belong to the single-valued group
        Set<String> ensembleMetrics = DeclarationUtilities.getMetricType( builder,
                                                                          MetricConstants.SampleDataGroup.ENSEMBLE,
                                                                          MetricConstants.SampleDataGroup.SINGLE_VALUED );
        if ( !ensembleMetrics.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered metrics that require ensemble forecasts: " + ensembleMetrics + "." );
        }

        // Discrete probability metrics?
        Set<String> discreteProbabilityMetrics =
                DeclarationUtilities.getMetricType( builder,
                                                    MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY,
                                                    null );
        if ( !discreteProbabilityMetrics.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered metrics that focus on discrete probability forecasts and these can "
                                     + "only be obtained from ensemble forecasts, currently: "
                                     + discreteProbabilityMetrics + "." );
        }

        return Collections.unmodifiableSet( ensembleDeclaration );
    }

    /**
     * Returns a string representation of each forecast declaration item discovered.
     * @param builder the builder
     * @return the forecast declaration strings, if any
     */

    static Set<String> getForecastDeclaration( EvaluationDeclarationBuilder builder )
    {
        Set<String> forecastDeclaration = new TreeSet<>();

        // Reference times?
        if ( Objects.nonNull( builder.referenceDates() ) )
        {
            forecastDeclaration.add( "Discovered a 'reference_dates' filter." );
        }

        // Reference time pools?
        if ( Objects.nonNull( builder.referenceDatePools() ) )
        {
            forecastDeclaration.add( "Discovered 'reference_date_pools'." );
        }

        // Lead times?
        if ( Objects.nonNull( builder.leadTimes() ) )
        {
            forecastDeclaration.add( "Discovered 'lead_times' filter." );
        }

        // Lead time pools?
        if ( Objects.nonNull( builder.leadTimePools() ) )
        {
            forecastDeclaration.add( "Discovered 'lead_time_pool'." );
        }

        // One or more sources with a forecast-like interface
        Set<String> forecastInterfaces = DeclarationUtilities.getSourcesWithForecastInterface( builder );
        if ( !forecastInterfaces.isEmpty() )
        {
            forecastDeclaration.add( "Discovered one or more data sources whose interfaces are forecast-like: "
                                     + forecastInterfaces
                                     + "." );
        }

        return Collections.unmodifiableSet( forecastDeclaration );
    }

    /**
     * Looks for the first line in the input and returns it.
     * @param lines the lines
     * @return the first line
     */
    static String getFirstLine( String lines )
    {
        String line = lines;
        String[] split = lines.split( "\\R" );
        if ( split.length > 0 )
        {
            line = split[0];
        }
        return line;
    }

    /**
     * @param builder the builder
     * @param groupType the group the metric should be part of
     * @param notInGroupType the optional group the metric should not be part of, in case it belongs to several groups
     * @return whether there are any metrics with the designated type
     */

    private static Set<String> getMetricType( EvaluationDeclarationBuilder builder,
                                              MetricConstants.SampleDataGroup groupType,
                                              MetricConstants.SampleDataGroup notInGroupType )
    {
        return builder.metrics()
                      .stream()
                      .filter( next -> next.name()
                                           .isInGroup( groupType )
                                       && ( Objects.isNull( notInGroupType ) || !next.name()
                                                                                     .isInGroup( notInGroupType ) ) )
                      .map( next -> next.name()
                                        .toString() )
                      .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * @param builder the builder
     * @return return any sources whose interfaces are exclusively forecast-like
     */

    private static Set<String> getSourcesWithForecastInterface( EvaluationDeclarationBuilder builder )
    {
        Set<String> interfaces = new TreeSet<>( DeclarationUtilities.getSourcesWithEnsembleInterface( builder ) );

        List<String> right = builder.right()
                                    .sources()
                                    .stream()
                                    .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                     && next.sourceInterface()
                                                            .getDataTypes()
                                                            .stream()
                                                            .allMatch(
                                                                    DataType::isForecastType ) )
                                    .map( next -> next.sourceInterface().toString() )
                                    .toList();

        interfaces.addAll( right );

        if ( hasBaseline( builder ) )
        {
            List<String> baseline = builder.baseline()
                                           .dataset()
                                           .sources()
                                           .stream()
                                           .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                            && next.sourceInterface()
                                                                   .getDataTypes()
                                                                   .stream()
                                                                   .allMatch( DataType::isForecastType ) )
                                           .map( next -> next.sourceInterface().toString() )
                                           .toList();
            interfaces.addAll( baseline );
        }

        return Collections.unmodifiableSet( interfaces );
    }

    /**
     * @param builder the builder
     * @return the sources with an interface/api that are exclusively ensemble forecasts
     */
    private static Set<String> getSourcesWithEnsembleInterface( EvaluationDeclarationBuilder builder )
    {
        List<String> right = builder.right()
                                    .sources()
                                    .stream()
                                    .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                     && next.sourceInterface()
                                                            .getDataTypes()
                                                            .equals( Set.of(
                                                                    DataType.ENSEMBLE_FORECASTS ) ) )
                                    .map( next -> next.sourceInterface().toString() )
                                    .toList();

        Set<String> ensembleInterfaces = new TreeSet<>( right );

        if ( hasBaseline( builder ) )
        {
            List<String> baseline = builder.right()
                                           .sources()
                                           .stream()
                                           .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                            && next.sourceInterface()
                                                                   .getDataTypes()
                                                                   .equals( Set.of( DataType.ENSEMBLE_FORECASTS ) ) )
                                           .map( next -> next.sourceInterface().toString() )
                                           .toList();
            ensembleInterfaces.addAll( baseline );
        }

        return Collections.unmodifiableSet( ensembleInterfaces );
    }

    /**
     * Inspects the source and tries to determine the feature authority.
     * @param source the source
     * @return the feature authority or null if none could be determined
     */
    private static FeatureAuthority getFeatureAuthorityFromSource( Source source )
    {
        // Inspect the source interface
        FeatureAuthority sourceInterfaceAuthority = null;
        SourceInterface sourceInterface = source.sourceInterface();

        if ( Objects.nonNull( sourceInterface ) )
        {
            sourceInterfaceAuthority = sourceInterface.getFeatureAuthority();
        }

        // Inspect the source URI
        FeatureAuthority sourceUriAuthority = null;
        URI uri = source.uri();
        if ( Objects.nonNull( uri ) && uri.toString()
                                          .contains( "usgs.gov/nwis" ) )
        {
            sourceUriAuthority = FeatureAuthority.USGS_SITE_CODE;
        }

        // Feature authority from source URI is null or both are equal, return the authority from the interface
        if ( Objects.isNull( sourceUriAuthority ) || sourceInterfaceAuthority == sourceUriAuthority )
        {
            return sourceInterfaceAuthority;
        }

        // Authority from the interface is null, return the authority from the source URI
        if ( Objects.isNull( sourceInterfaceAuthority ) )
        {
            return sourceUriAuthority;
        }

        LOGGER.warn( "Discovered a source whose feature authority implied by the source URI contradicts the "
                     + "feature authority implied by the source interface. The source interface says {} and the URI "
                     + "says {}. The feature authority cannot be determined. The source URI is: {}. Please adjust the "
                     + "source as needed.", sourceInterfaceAuthority, sourceUriAuthority, uri );

        return null;
    }

    /**
     * Adds the data sources to the supplied builder, correlating them with existing sources as needed.
     * @param builder the builder
     * @param sources the sources
     * @param orientation the orientation to help with logging
     */

    private static void addDataSources( DatasetBuilder builder, List<URI> sources, DatasetOrientation orientation )
    {
        if ( Objects.isNull( sources ) || sources.isEmpty() )
        {
            LOGGER.debug( "No {} data sources were added to the evaluation.", orientation );
            return;
        }

        // No existing sources to correlate
        if ( Objects.isNull( builder.sources() ) || builder.sources()
                                                           .isEmpty() )
        {
            List<Source> newSources = sources.stream()
                                             .map( uri -> SourceBuilder.builder()
                                                                       .uri( uri )
                                                                       .build() )
                                             .toList();
            builder.sources( newSources );

            LOGGER.debug( "Added the following new sources to an empty list of existing sources for the {} data: {}.",
                          orientation,
                          newSources );

            return;
        }

        // Existing sources to correlate
        // A URI is correlated if the path element of a supplied URI ends with the path element of a declared URI
        // If there are no correlations, the declared URIs are simply added
        List<Source> existingSources = builder.sources();
        List<Source> newSources = new ArrayList<>();
        for ( URI uri : sources )
        {
            // Any correlated sources?
            List<Source> matched = existingSources.stream()
                                                  .filter( next -> uri.getPath()
                                                                      .endsWith( next.uri()
                                                                                     .getPath() ) )
                                                  .toList();

            // If there are matches, add them, preserving the existing source information
            if ( !matched.isEmpty() )
            {
                LOGGER.debug( "While inspecting URI {}, discovered the following correlated URIs among the "
                              + "existing sources: {}", uri, matched );

                matched.forEach( next -> newSources.add( SourceBuilder.builder( next )
                                                                      .uri( uri )
                                                                      .build() ) );
            }
            // Otherwise add the new source with the unmatched URI
            else
            {
                LOGGER.debug( "While inspecting URI {}, discovered no correlated URIs among the existing "
                              + "sources.", uri );

                Source newSource = SourceBuilder.builder()
                                                .uri( uri )
                                                .build();
                newSources.add( newSource );
            }
        }

        LOGGER.debug( "Added the following new sources for the {} data: {}. The new sources were added to the "
                      + "following existing sources {}.",
                      orientation,
                      newSources,
                      existingSources );

        // Add back existing sources that are uncorrelated with new sources
        List<Source> existingSourcesToAdd = new ArrayList<>();
        for ( Source existingSource : existingSources )
        {
            boolean match = newSources.stream()
                                      .anyMatch( next -> next.uri()
                                                             .getPath()
                                                             .endsWith( existingSource.uri()
                                                                                      .getPath() ) );

            if ( !match )
            {
                existingSourcesToAdd.add( existingSource );
            }
        }

        LOGGER.debug( "Retained the following existing sources that were uncorrelated with new sources: {}.",
                      existingSourcesToAdd );
        List<Source> combinedSources = new ArrayList<>( existingSourcesToAdd );
        combinedSources.addAll( newSources );

        builder.sources( combinedSources );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation using
     * the {@link EvaluationDeclaration#leadTimePools()} and the {@link EvaluationDeclaration#leadTimes()}. Returns at
     * least one {@link TimeWindow}.
     *
     * @param declaration the declaration
     * @return the set of lead duration time windows
     * @throws NullPointerException if any required input is null
     */

    private static Set<TimeWindow> getLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        String messageStart = "Cannot determine lead duration time windows ";

        Objects.requireNonNull( declaration, messageStart + "from null declaration." );

        LeadTimeInterval leadHours = declaration.leadTimes();

        Objects.requireNonNull( leadHours, "Cannot determine lead duration time windows without 'lead_times'." );
        Objects.requireNonNull( leadHours.minimum(),
                                "Cannot determine lead duration time windows without a 'minimum' value for "
                                + "'lead_times'." );
        Objects.requireNonNull( leadHours.maximum(),
                                "Cannot determine lead duration time windows without a 'maximum' value for "
                                + "'lead_times'." );

        TimePools leadTimesPoolingWindow = declaration.leadTimePools();

        Objects.requireNonNull( leadTimesPoolingWindow,
                                "Cannot determine lead duration time windows without a 'lead_time_pools'." );

        // Obtain the base window
        TimeWindow baseWindow = DeclarationUtilities.getOneBigTimeWindow( declaration );

        // Period associated with the leadTimesPoolingWindow
        Duration periodOfLeadTimesPoolingWindow = leadTimesPoolingWindow.period();

        // Exclusive lower bound: #56213-104
        Duration earliestLeadDurationExclusive = leadHours.minimum();

        // Inclusive upper bound
        Duration latestLeadDurationInclusive = leadHours.maximum();

        // Duration by which to increment. Defaults to the period associated
        // with the leadTimesPoolingWindow, otherwise the frequency.
        Duration increment = periodOfLeadTimesPoolingWindow;
        if ( Objects.nonNull( leadTimesPoolingWindow.frequency() ) )
        {
            increment = leadTimesPoolingWindow.frequency();
        }

        // Lower bound of the current window
        Duration earliestExclusive = earliestLeadDurationExclusive;

        // Upper bound of the current window
        Duration latestInclusive = earliestExclusive.plus( periodOfLeadTimesPoolingWindow );

        // Create the time windows
        Set<TimeWindow> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound extends past the
        // latestLeadDurationInclusive: #56213-104
        // Window increments are zero?
        if ( Duration.ZERO.equals( increment ) )
        {
            com.google.protobuf.Duration earliest = MessageFactory.parse( earliestExclusive );
            com.google.protobuf.Duration latest = MessageFactory.parse( latestInclusive );
            TimeWindow window = baseWindow.toBuilder()
                                          .setEarliestLeadDuration( earliest )
                                          .setLatestLeadDuration( latest )
                                          .build();
            timeWindows.add( window );
        }
        // Create as many windows as required at the prescribed increment
        else
        {
            while ( latestInclusive.compareTo( latestLeadDurationInclusive ) <= 0 )
            {
                // Add the current time window
                com.google.protobuf.Duration earliest = MessageFactory.parse( earliestExclusive );
                com.google.protobuf.Duration latest = MessageFactory.parse( latestInclusive );
                TimeWindow window = baseWindow.toBuilder()
                                              .setEarliestLeadDuration( earliest )
                                              .setLatestLeadDuration( latest )
                                              .build();
                timeWindows.add( window );

                // Increment from left-to-right: #56213-104
                earliestExclusive = earliestExclusive.plus( increment );
                latestInclusive = latestInclusive.plus( increment );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation using
     * the {@link EvaluationDeclaration#referenceDatePools()} and the {@link EvaluationDeclaration#referenceDates()}.
     * Returns at least one {@link TimeWindow}.
     *
     * @param declaration the declaration
     * @return the set of reference time windows
     * @throws NullPointerException if the declaration is null or any required input within it is null
     */

    private static Set<TimeWindow> getReferenceDatesTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, "Cannot determine reference time windows from missing "
                                             + "declaration." );
        Objects.requireNonNull( declaration.referenceDates(),
                                "Cannot determine reference time windows without 'reference_dates'." );
        Objects.requireNonNull( declaration.referenceDates()
                                           .minimum(),
                                "Cannot determine reference time windows without the 'minimum' for the "
                                + "'reference_dates'." );
        Objects.requireNonNull( declaration.referenceDates()
                                           .maximum(),
                                "Cannot determine reference time windows without the 'maximum' for the "
                                + "'reference_dates'." );
        Objects.requireNonNull( declaration.referenceDatePools(),
                                "Cannot determine reference time windows without 'reference_date_pools'." );

        // Base window from which to generate a sequence of windows
        TimeWindow baseWindow = DeclarationUtilities.getOneBigTimeWindow( declaration );

        return DeclarationUtilities.getTimeWindowsForDateSequence( declaration.referenceDates(),
                                                                   declaration.referenceDatePools(),
                                                                   baseWindow,
                                                                   true );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation using
     * the {@link EvaluationDeclaration#validDatePools()} and the {@link EvaluationDeclaration#validDates()}.
     * Returns at least one {@link TimeWindow}.
     *
     * @param declaration the declaration
     * @return the set of reference time windows
     * @throws NullPointerException if the declaration is null or any required input within it is null
     */

    private static Set<TimeWindow> getValidDatesTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, "Cannot determine valid time windows from missing declaration." );
        Objects.requireNonNull( declaration.validDates(),
                                "Cannot determine valid time windows without 'valid_dates'." );
        Objects.requireNonNull( declaration.validDates()
                                           .minimum(),
                                "Cannot determine valid time windows without the 'minimum' for the "
                                + "'valid_dates'." );
        Objects.requireNonNull( declaration.validDates()
                                           .maximum(),
                                "Cannot determine valid time windows without the 'maximum' for the "
                                + "'valid_dates'." );
        Objects.requireNonNull( declaration.validDatePools(),
                                "Cannot determine valid time windows without 'valid_date_pools'." );

        // Base window from which to generate a sequence of windows
        TimeWindow baseWindow = DeclarationUtilities.getOneBigTimeWindow( declaration );

        return DeclarationUtilities.getTimeWindowsForDateSequence( declaration.validDates(),
                                                                   declaration.validDatePools(),
                                                                   baseWindow,
                                                                   false );
    }

    /**
     * <p>Generates a set of time windows based on a sequence of datetimes.
     *
     * @param dates the date constraints
     * @param pools the sequence of datetimes to generate
     * @param baseWindow the basic time window from which each pool in the sequence begins
     * @param areReferenceTimes is true if the dates are reference dates, false for valid dates
     * @return the set of reference time windows
     * @throws NullPointerException if any input is null
     */

    private static Set<TimeWindow> getTimeWindowsForDateSequence( TimeInterval dates,
                                                                  TimePools pools,
                                                                  TimeWindow baseWindow,
                                                                  boolean areReferenceTimes )
    {
        Objects.requireNonNull( dates );
        Objects.requireNonNull( pools );
        Objects.requireNonNull( baseWindow );

        // Period associated with the reference time pool
        Duration periodOfPoolingWindow = pools.period();

        // Exclusive lower bound: #56213-104
        Instant earliestInstantExclusive = dates.minimum();

        // Inclusive upper bound
        Instant latestInstantInclusive = dates.maximum();

        // Duration by which to increment. Defaults to the period associated with the reference time pools, otherwise
        // the frequency.
        Duration increment = periodOfPoolingWindow;
        if ( Objects.nonNull( pools.frequency() ) )
        {
            increment = pools.frequency();
        }

        // Lower bound of the current window
        Instant earliestExclusive = earliestInstantExclusive;

        // Upper bound of the current window
        Instant latestInclusive = earliestExclusive.plus( periodOfPoolingWindow );

        // Create the time windows
        Set<TimeWindow> timeWindows = new HashSet<>();

        // Increment left-to-right and stop when the right bound
        // extends past the latestInstantInclusive: #56213-104
        while ( latestInclusive.compareTo( latestInstantInclusive ) <= 0 )
        {
            TimeWindow timeWindow = DeclarationUtilities.getTimeWindowFromDates( earliestExclusive,
                                                                                 latestInclusive,
                                                                                 baseWindow,
                                                                                 areReferenceTimes );

            // Add the current time window
            timeWindows.add( timeWindow );

            // Increment left-to-right: #56213-104
            earliestExclusive = earliestExclusive.plus( increment );
            latestInclusive = latestInclusive.plus( increment );
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * Returns a time window from the inputs.
     *
     * @param earliestExclusive the earliest exclusive time
     * @param latestInclusive the latest inclusive time
     * @param baseWindow the base window with default times
     * @param areReferenceTimes is true if the earliestExclusive and latestInclusive are reference times, false for
     *                          valid times
     * @return a time window
     */

    private static TimeWindow getTimeWindowFromDates( Instant earliestExclusive,
                                                      Instant latestInclusive,
                                                      TimeWindow baseWindow,
                                                      boolean areReferenceTimes )
    {
        Timestamp earliest = MessageFactory.parse( earliestExclusive );
        Timestamp latest = MessageFactory.parse( latestInclusive );

        // Reference dates
        if ( areReferenceTimes )
        {
            return baseWindow.toBuilder()
                             .setEarliestReferenceTime( earliest )
                             .setLatestReferenceTime( latest )
                             .build();
        }
        // Valid dates
        else
        {
            return baseWindow.toBuilder()
                             .setEarliestValidTime( earliest )
                             .setLatestValidTime( latest )
                             .build();
        }
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation using
     * the {@link EvaluationDeclaration#leadTimePools()}, the {@link EvaluationDeclaration#leadTimes()}, the
     * {@link EvaluationDeclaration#referenceDatePools()} and the {@link EvaluationDeclaration#referenceDates()}.
     * Returns at least one {@link TimeWindow}.
     *
     * @param declaration the declaration
     * @return the set of lead duration and reference time windows
     * @throws NullPointerException if the declaration is null
     */

    private static Set<TimeWindow> getReferenceDatesAndLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindow> leadDurationWindows = DeclarationUtilities.getLeadDurationTimeWindows( declaration );

        Set<TimeWindow> referenceDatesWindows = DeclarationUtilities.getReferenceDatesTimeWindows( declaration );

        // Create a new window for each combination of reference dates and lead duration
        Set<TimeWindow> timeWindows =
                new HashSet<>( leadDurationWindows.size() * referenceDatesWindows.size() );
        for ( TimeWindow nextReferenceWindow : referenceDatesWindows )
        {
            for ( TimeWindow nextLeadWindow : leadDurationWindows )
            {
                TimeWindow window = nextReferenceWindow.toBuilder()
                                                       .setEarliestLeadDuration( nextLeadWindow.getEarliestLeadDuration() )
                                                       .setLatestLeadDuration( nextLeadWindow.getLatestLeadDuration() )
                                                       .build();
                timeWindows.add( window );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation using
     * the {@link EvaluationDeclaration#referenceDatePools()}, the {@link EvaluationDeclaration#referenceDates()}, the
     * {@link EvaluationDeclaration#validDatePools()} and the {@link EvaluationDeclaration#validDates()}. Returns at
     * least one {@link TimeWindow}.
     *
     * @param declaration the declaration
     * @return the set of reference time and valid time windows
     * @throws NullPointerException if the declaration is null
     */

    private static Set<TimeWindow> getReferenceDatesAndValidDatesTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindow> validDatesWindows = DeclarationUtilities.getValidDatesTimeWindows( declaration );

        Set<TimeWindow> referenceDatesWindows = DeclarationUtilities.getReferenceDatesTimeWindows( declaration );

        // Create a new window for each combination of reference dates and lead duration
        Set<TimeWindow> timeWindows = new HashSet<>( validDatesWindows.size() * referenceDatesWindows.size() );
        for ( TimeWindow nextValidWindow : validDatesWindows )
        {
            for ( TimeWindow nextReferenceWindow : referenceDatesWindows )
            {
                TimeWindow window =
                        nextValidWindow.toBuilder()
                                       .setEarliestReferenceTime( nextReferenceWindow.getEarliestReferenceTime() )
                                       .setLatestReferenceTime( nextReferenceWindow.getLatestReferenceTime() )
                                       .build();
                timeWindows.add( window );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation using
     * the {@link EvaluationDeclaration#leadTimePools()} ()}, the {@link EvaluationDeclaration#leadTimes()}, the
     * {@link EvaluationDeclaration#validDatePools()} ()} and the {@link EvaluationDeclaration#validDates()}. Returns
     * at least one {@link TimeWindow}.
     *
     * @param declaration the declaration
     * @return the set of lead duration and valid dates time windows
     * @throws NullPointerException if the pairConfig is null
     */

    private static Set<TimeWindow> getValidDatesAndLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindow> leadDurationWindows = DeclarationUtilities.getLeadDurationTimeWindows( declaration );

        Set<TimeWindow> validDatesWindows = DeclarationUtilities.getValidDatesTimeWindows( declaration );

        // Create a new window for each combination of valid dates and lead duration
        Set<TimeWindow> timeWindows = new HashSet<>( leadDurationWindows.size() * validDatesWindows.size() );
        for ( TimeWindow nextValidWindow : validDatesWindows )
        {
            for ( TimeWindow nextLeadWindow : leadDurationWindows )
            {
                TimeWindow window =
                        nextValidWindow.toBuilder()
                                       .setEarliestLeadDuration( nextLeadWindow.getEarliestLeadDuration() )
                                       .setLatestLeadDuration( nextLeadWindow.getLatestLeadDuration() )
                                       .build();
                timeWindows.add( window );
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * <p>Consumes a {@link EvaluationDeclaration} and returns a {@link Set} of {@link TimeWindow} for evaluation using
     * the {@link EvaluationDeclaration#leadTimePools()}, the {@link EvaluationDeclaration#leadTimes()}, the
     * {@link EvaluationDeclaration#referenceDatePools()} the {@link EvaluationDeclaration#referenceDates()}, the
     * {@link EvaluationDeclaration#validDatePools()} and the {@link EvaluationDeclaration#validDates()}. Returns at
     * least one {@link TimeWindow}.
     *
     * @param declaration the declaration
     * @return the set of lead duration, reference time and valid time windows
     * @throws NullPointerException if the declaration is null
     */

    private static Set<TimeWindow> getReferenceDatesValidDatesAndLeadDurationTimeWindows( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration, CANNOT_DETERMINE_TIME_WINDOWS_FROM_MISSING_DECLARATION );

        Set<TimeWindow> leadDurationWindows = DeclarationUtilities.getLeadDurationTimeWindows( declaration );
        Set<TimeWindow> referenceDatesWindows = DeclarationUtilities.getReferenceDatesTimeWindows( declaration );
        Set<TimeWindow> validDatesWindows = DeclarationUtilities.getValidDatesTimeWindows( declaration );

        // Create a new window for each combination of reference dates and lead duration
        Set<TimeWindow> timeWindows = new HashSet<>( leadDurationWindows.size() * referenceDatesWindows.size() );
        for ( TimeWindow nextReferenceWindow : referenceDatesWindows )
        {
            for ( TimeWindow nextValidWindow : validDatesWindows )
            {
                for ( TimeWindow nextLeadWindow : leadDurationWindows )
                {
                    TimeWindow window =
                            nextValidWindow.toBuilder()
                                           .setEarliestReferenceTime( nextReferenceWindow.getEarliestReferenceTime() )
                                           .setLatestReferenceTime( nextReferenceWindow.getLatestReferenceTime() )
                                           .setEarliestLeadDuration( nextLeadWindow.getEarliestLeadDuration() )
                                           .setLatestLeadDuration( nextLeadWindow.getLatestLeadDuration() )
                                           .build();
                    timeWindows.add( window );
                }
            }
        }

        return Collections.unmodifiableSet( timeWindows );
    }

    /**
     * Expands the supplied metrics, replacing any timing error summary statistics with equivalent top-level metrics.
     * @param metrics the metrics
     * @return the expanded metrics
     */

    private static Set<Set<Metric>> expand( Set<Set<Metric>> metrics )
    {
        // Expand any time-series summary statistics into top-level metrics
        Set<Set<Metric>> expanded = new HashSet<>();

        for ( Set<Metric> nextMetrics : metrics )
        {
            Set<Metric> innerMetrics = new HashSet<>( nextMetrics );
            for ( Metric metric : nextMetrics )
            {
                if ( Objects.nonNull( metric.parameters() )
                     && !metric.parameters()
                               .summaryStatistics()
                               .isEmpty() )
                {
                    Set<MetricConstants> nextSummaryStats = metric.parameters()
                                                                  .summaryStatistics()
                                                                  .stream()
                                                                  .map( n -> MetricConstants.valueOf( n.getStatistic()
                                                                                                       .name() ) )
                                                                  .collect( Collectors.toSet() );
                    MetricConstants parent = metric.name();
                    Set<Metric> summaryExpanded =
                            nextSummaryStats.stream()
                                            .map( n -> DeclarationUtilities.expand( n, parent ) )
                                            .collect( Collectors.toSet() );
                    innerMetrics.addAll( summaryExpanded );
                }
            }
            innerMetrics = Collections.unmodifiableSet( innerMetrics );
            expanded.add( innerMetrics );
        }

        LOGGER.debug( "Expanded these metrics: {}. The expanded metrics are: {}.", metrics, expanded );

        return Collections.unmodifiableSet( expanded );
    }

    /**
     * Expands a time-series summary statistic into a top-level metric.
     * @param summaryStatistic the summary statistic
     * @return the corresponding top-level metric
     */
    private static Metric expand( MetricConstants summaryStatistic, MetricConstants parent )
    {
        Optional<Metric> metric = parent.getChildren()
                                        .stream()
                                        .filter( next -> next.getChild() == summaryStatistic )
                                        .map( n -> MetricBuilder.builder()
                                                                .name( n )
                                                                .build() )
                                        .findFirst();
        if ( metric.isEmpty() )
        {
            throw new DeclarationException( "Could not correlate the '"
                                            + DeclarationUtilities.fromEnumName( summaryStatistic.name() )
                                            + "' with a top-level metric that belongs to the '"
                                            + DeclarationUtilities.fromEnumName( parent.name() )
                                            + "'. Please use a summary statistic that is valid in this context. "
                                            + "The valid summary statistics are: "
                                            + parent.getChildren()
                                            + "." );
        }

        return metric.get();
    }

    /**
     * Removes from the declaration any features that do not meet the filter. The logging assumes that the filter
     * identifies features for which featureful thresholds are available, although the method itself is more general.
     * If needed in other contexts, then the method should be renamed and the precise context injected.
     * @param declaration the declaration
     * @param retain the features for which featureful thresholds are available
     * @return the adjusted declaration
     * @throws NullPointerException if the declaration is null
     */

    private static EvaluationDeclaration removeFeaturesWithoutThresholds( EvaluationDeclaration declaration,
                                                                          Predicate<GeometryTuple> retain )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );

        // Filter the declared features
        if ( Objects.nonNull( declaration.features() ) )
        {
            Set<GeometryTuple> features = declaration.features()
                                                     .geometries();
            Set<GeometryTuple> filtered = features.stream()
                                                  .filter( retain )
                                                  .collect( Collectors.toSet() );
            // Set the new features
            Features filteredFeatures = new Features( filtered );
            builder.features( filteredFeatures );

            if ( LOGGER.isWarnEnabled() && filtered.size() != features.size() )
            {
                Set<GeometryTuple> copy = new HashSet<>( features );
                copy.removeAll( filtered );

                LOGGER.warn( "Discovered {} feature(s) for which thresholds were not available. These features "
                             + "have been removed from the evaluation. The features are: {}.", copy.size(),
                             copy.stream()
                                 .map( DeclarationFactory.PROTBUF_STRINGIFIER )
                                 .toList() );
            }
        }

        // Filter the grouped features
        // Adjust the feature groups, if any
        if ( Objects.nonNull( builder.featureGroups() ) )
        {
            Set<GeometryGroup> originalGroups = builder.featureGroups()
                                                       .geometryGroups();
            Set<GeometryGroup> adjustedGroups = new HashSet<>();
            Set<GeometryGroup> groupsWithAdjustments = new HashSet<>();

            // Iterate the groups and adjust as needed
            for ( GeometryGroup nextGroup : originalGroups )
            {
                List<GeometryTuple> nextTuples = nextGroup.getGeometryTuplesList();
                List<GeometryTuple> adjusted = nextTuples.stream()
                                                         .filter( retain )
                                                         .toList();

                // Adjustments made?
                if ( adjusted.size() != nextTuples.size() )
                {
                    GeometryGroup adjustedGroup = nextGroup.toBuilder()
                                                           .clearGeometryTuples()
                                                           .addAllGeometryTuples( adjusted )
                                                           .build();
                    adjustedGroups.add( adjustedGroup );
                    groupsWithAdjustments.add( adjustedGroup );
                }
                else
                {
                    adjustedGroups.add( nextGroup );
                }
            }

            if ( LOGGER.isWarnEnabled() && !groupsWithAdjustments.isEmpty() )
            {
                LOGGER.warn( "Discovered {} feature group(s) where thresholds were not available for one or more of "
                             + "their component features. These features have been removed from the evaluation. "
                             + "Features were removed from the following feature groups: {}.",
                             groupsWithAdjustments.size(),
                             groupsWithAdjustments.stream()
                                                  .map( GeometryGroup::getRegionName )
                                                  .toList() );
            }

            FeatureGroups finalFeatureGroups = new FeatureGroups( adjustedGroups );
            builder.featureGroups( finalFeatureGroups );
        }

        return builder.build();
    }

    /**
     * Do not construct.
     */
    private DeclarationUtilities()
    {
    }
}
