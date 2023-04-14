package wres.config.yaml;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdType;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;

/**
 * A utility class for working with {@link EvaluationDeclaration}.
 * @author James Brown
 */
public class DeclarationUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DeclarationUtilities.class );

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
     * Returns the features from all contexts in the declaration.
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
                };
    }

    /**
     * Acquires the feature authority for the required data orientation.
     * @param evaluation the evaluation
     * @param orientation the data orientation
     * @return the feature authority or null if none was discovered
     */

    public static FeatureAuthority getFeatureAuthorityFor( EvaluationDeclaration evaluation,
                                                           DatasetOrientation orientation )
    {
        return switch ( orientation )
                {
                    case LEFT -> evaluation.left()
                                           .featureAuthority();
                    case RIGHT -> evaluation.right()
                                            .featureAuthority();
                    case BASELINE -> evaluation.baseline()
                                               .dataset()
                                               .featureAuthority();
                };
    }

    /**
     * Groups the metrics by their common parameters. Only considers parameters that are involved in slicing or
     * transforming pairs, such as thresholds or the type of ensemble average to calculate. In short, the metrics
     * that belong to a single group represent an atomic set for processing because they require common pairs.
     *
     * @param metrics the metrics to group
     * @return the metrics grouped by common parameters (that are used for slicing or transforming)
     */

    public static Set<Set<Metric>> getMetricGroupsForProcessing( Set<Metric> metrics )
    {
        Objects.requireNonNull( metrics );

        // Create the grouping function
        Function<Metric, MetricParameters> classifier = metric ->
        {
            MetricParametersBuilder builder = MetricParametersBuilder.builder();
            if( Objects.nonNull( metric.parameters() ) )
            {
                MetricParameters existing = metric.parameters();
                builder.valueThresholds( existing.valueThresholds() );
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
                      + "of these groups is an atomic set for processing. The metric groups are: {}.",
                      grouped.size(),
                      grouped );

        return Set.copyOf( grouped.values() );
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

    static boolean hasAnalysisDurations( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return hasAnalysisDurations( builder );
    }

    /**
     * @param builder the builder
     * @return whether analysis durations have been declared
     */

    static boolean hasAnalysisDurations( EvaluationDeclarationBuilder builder )
    {
        return Objects.nonNull( builder.analysisDurations() ) && (
                Objects.nonNull( builder.analysisDurations().minimumExclusive() )
                || Objects.nonNull( builder.analysisDurations().maximum() ) );
    }

    /**
     * Groups the thresholds by threshold type.
     * @param thresholds the thresholds
     * @return the thresholds grouped by type
     */

    static Map<ThresholdType, Set<Threshold>> groupThresholdsByType( Set<Threshold> thresholds )
    {
        return thresholds.stream()
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
        return getEnsembleDeclaration( builder );
    }

    /**
     * Returns a string representation of each forecast declaration item discovered.
     * @param declaration the declaration
     * @return the forecast declaration strings, if any
     */

    static Set<String> getForecastDeclaration( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return getForecastDeclaration( builder );
    }

    /**
     * Returns a string representation of each ensemble declaration item discovered.
     * @param builder the builder
     * @return the ensemble declaration was found
     */

    static Set<String> getEnsembleDeclaration( EvaluationDeclarationBuilder builder )
    {
        Set<String> ensembleDeclaration = new TreeSet<>();

        // Ensemble filter on predicted dataset?
        if ( Objects.nonNull( builder.right().ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the predicted dataset." );
        }

        // Ensemble filter on baseline dataset?
        if ( hasBaseline( builder ) && Objects.nonNull( builder.baseline()
                                                               .dataset()
                                                               .ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the baseline dataset." );
        }

        // Any source that contains an interface with the word ensemble?
        Set<String> ensembleInterfaces = DeclarationUtilities.getSourcesWithEnsembleInterface( builder );
        if ( !ensembleInterfaces.isEmpty() )
        {
            ensembleDeclaration.add(
                    "Discovered one or more data sources whose interfaces are ensemble-like: " + ensembleInterfaces
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
     * Do not construct.
     */
    private DeclarationUtilities()
    {
    }
}
