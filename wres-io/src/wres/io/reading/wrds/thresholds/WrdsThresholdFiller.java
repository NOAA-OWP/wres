package wres.io.reading.wrds.thresholds;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdService;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.units.UnitMapper;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.wrds.geography.WrdsLocation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Threshold;

/**
 * Fills a supplied declaration with thresholds acquired from a Water Resources Data Service (WRDS).
 * @author James Brown
 */
public class WrdsThresholdFiller
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsThresholdFiller.class );

    /** The threshold reader. */
    private static final WrdsThresholdReader READER = WrdsThresholdReader.of();

    /**
     * Attempts to acquire thresholds from WRDS and populate them in the supplied declaration, as needed.
     * @param evaluation the declaration to adjust
     * @param unitMapper a unit mapper to map the threshold values to correct units
     * @return the adjusted declaration, including any thresholds acquired from WRDS
     */
    public static EvaluationDeclaration fillThresholds( EvaluationDeclaration evaluation, UnitMapper unitMapper )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( unitMapper );

        ThresholdService service = evaluation.thresholdService();

        // No threshold service request?
        if ( Objects.isNull( service )
             || Objects.isNull( service.uri() ) )
        {
            LOGGER.debug( "Did not discover a request for WRDS thresholds in the supplied declaration. The threshold "
                          + "service declaration was: {}.", service );
            return evaluation;
        }

        // Acquire the feature names for which thresholds are required
        DatasetOrientation orientation = service.featureNameFrom();

        if ( Objects.isNull( orientation ) )
        {
            throw new ThresholdReadingException( "The 'feature_name_from' is missing from the 'threshold_service' "
                                                 + "declaration, which is not allowed because the feature service "
                                                 + "request must use feature names with a prescribed feature "
                                                 + "authority." );
        }

        // If the orientation for service thresholds is 'BASELINE', then a baseline must be present
        if ( orientation == DatasetOrientation.BASELINE && !DeclarationUtilities.hasBaseline( evaluation ) )
        {
            throw new ThresholdReadingException( "The 'threshold_service' declaration requested that feature names "
                                                 + "with an orientation of '"
                                                 + DatasetOrientation.BASELINE
                                                 + "' are used to correlate features with thresholds, but no "
                                                 + "'baseline' dataset was discovered. Please add a 'baseline' dataset "
                                                 + "or fix the 'feature_name_from' in the 'threshold_service' "
                                                 + "declaration." );
        }

        // Get the feature authority for this data orientation
        FeatureAuthority featureAuthority = DeclarationUtilities.getFeatureAuthorityFor( evaluation, orientation );

        // Feature authority must be present if the URI is web-like and not a local file
        URI serviceUri = service.uri();
        if ( ReaderUtilities.isWebSource( serviceUri )
             && Objects.isNull( featureAuthority ) )
        {
            throw new ThresholdReadingException( "When attempting to request thresholds from the WRDS, could not "
                                                 + "determine the feature authority to use for the feature names "
                                                 + "associated with the '"
                                                 + orientation
                                                 + "' data, which are needed to "
                                                 + "correlate the thresholds with features. Please clarify by adding "
                                                 + "a 'feature_authority' to the '"
                                                 + orientation
                                                 + "' dataset. " );
        }

        // Assemble the features that require thresholds
        Set<GeometryTuple> tuples = DeclarationUtilities.getFeatures( evaluation );

        // No features?
        if ( tuples.isEmpty() )
        {
            throw new ThresholdReadingException( "While attempting to read thresholds from the WRDS feature service, "
                                                 + "discovered no features in the declaration for which thresholds "
                                                 + "could be acquired. Please add some features to the declaration "
                                                 + "using 'features', 'feature_groups' or 'feature_service' and try "
                                                 + "again." );
        }

        // Baseline orientation and some feature tuples present that are missing a baseline feature?
        if ( orientation == DatasetOrientation.BASELINE
             && tuples.stream()
                      .anyMatch( next -> !next.hasBaseline() ) )
        {
            throw new ThresholdReadingException( "Discovered declaration for a 'threshold_service', which requests "
                                                 + "thresholds whose feature names have an orientation of '"
                                                 + DatasetOrientation.BASELINE
                                                 + "'. However, some features were discovered with a missing '"
                                                 + DatasetOrientation.BASELINE
                                                 + "' feature name. Please fix the 'feature_name_from' in the "
                                                 + "'threshold_service' declaration or supply fully composed feature "
                                                 + "tuples with an appropriate feature for the '"
                                                 + DatasetOrientation.BASELINE
                                                 + "' dataset." );
        }

        // Get the adjusted feature names mapped to the original names
        Map<String, String> featureNames = WrdsThresholdFiller.getFeatureNames( tuples,
                                                                                orientation,
                                                                                featureAuthority,
                                                                                serviceUri );

        // Continue to read the thresholds
        Map<WrdsLocation, Set<Threshold>> thresholds = READER.readThresholds( service,
                                                                              unitMapper,
                                                                              featureNames.keySet(),
                                                                              featureAuthority );

        LOGGER.trace( "Read the following thresholds from WRDS: {}.", thresholds );

        // Validate the thresholds against the features
        WrdsThresholdFiller.validate( featureNames, thresholds, featureAuthority );

        // Adjust the declaration and return it
        return WrdsThresholdFiller.getAdjustedDeclaration( evaluation,
                                                           thresholds,
                                                           featureAuthority,
                                                           orientation,
                                                           featureNames );
    }

    /**
     * Returns the requested feature names mapped against the original names. The requested names will differ in some
     * cases. For example, when requesting names with the {@link FeatureAuthority#NWS_LID}, the names must be
     * "Handbook 5" names, which contain up to five characters.
     *
     * @param tuples the feature tuples
     * @param orientation the dataset orientation
     * @param featureAuthority the feature authority
     * @param serviceUri the service URI
     * @return the feature names to use when forming a request to a web service
     */

    private static Map<String, String> getFeatureNames( Set<GeometryTuple> tuples,
                                                        DatasetOrientation orientation,
                                                        FeatureAuthority featureAuthority,
                                                        URI serviceUri )
    {
        Set<String> oneSided = DeclarationUtilities.getFeatureNamesFor( tuples, orientation );
        Map<String, String> names;
        // Only handbook 5 names are allowed in this context, so use up to the first 5 characters of an NWS LID only
        if ( featureAuthority == FeatureAuthority.NWS_LID && ReaderUtilities.isWebSource( serviceUri ) )
        {
            names = oneSided.stream()
                            .collect( Collectors.toUnmodifiableMap( n -> n.substring( 0, Math.min( n.length(), 5 ) ),
                                                                    Function.identity() ) );
        }
        else
        {
            names = oneSided.stream()
                            .collect( Collectors.toUnmodifiableMap( Function.identity(), Function.identity() ) );
        }

        return names;
    }

    /**
     * Adjusts the declaration to include the prescribed thresholds.
     * @param evaluation the evaluation declaration to adjust
     * @param thresholds the thresholds
     * @param featureAuthority the feature authority to help with feature naming
     * @param orientation the orientation of the dataset to which the feature names apply
     * @param featureNames the feature names
     * @return the adjusted declaration
     */
    private static EvaluationDeclaration getAdjustedDeclaration( EvaluationDeclaration evaluation,
                                                                 Map<WrdsLocation, Set<Threshold>> thresholds,
                                                                 FeatureAuthority featureAuthority,
                                                                 DatasetOrientation orientation,
                                                                 Map<String, String> featureNames )
    {
        // Ordered, mapped thresholds
        Set<wres.config.yaml.components.Threshold> mappedThresholds = new HashSet<>();

        Set<Geometry> featuresWithThresholds = new HashSet<>();
        for ( Map.Entry<WrdsLocation, Set<Threshold>> nextEntry : thresholds.entrySet() )
        {
            WrdsLocation location = nextEntry.getKey();
            Set<Threshold> nextThresholds = nextEntry.getValue();

            String featureName = WrdsLocation.getNameForAuthority( featureAuthority, location );
            String originalFeatureName = featureNames.get( featureName );

            Geometry feature = Geometry.newBuilder()
                                       .setName( originalFeatureName )
                                       .build();
            featuresWithThresholds.add( feature );
            Set<wres.config.yaml.components.Threshold> nextMappedThresholds =
                    nextThresholds.stream()
                                  .map( next -> ThresholdBuilder.builder()
                                                                .threshold( next )
                                                                .type( ThresholdType.VALUE )
                                                                .feature( feature )
                                                                .featureNameFrom( orientation )
                                                                .build() )
                                  .collect( Collectors.toSet() );
            mappedThresholds.addAll( nextMappedThresholds );
        }

        // Add any existing thresholds
        mappedThresholds.addAll( evaluation.valueThresholds() );

        LOGGER.debug( "Added {} value thresholds obtained from the WRDS to the evaluation declaration.",
                      mappedThresholds.size() );

        // Add all the thresholds to the declaration
        EvaluationDeclarationBuilder adjusted = EvaluationDeclarationBuilder.builder( evaluation )
                                                                            .valueThresholds( mappedThresholds );

        // Add the thresholds to the individual metrics too
        Set<Metric> metrics = adjusted.metrics();
        Set<Metric> adjustedMetrics = new HashSet<>();
        for ( Metric metric : metrics )
        {
            MetricParameters parameters = metric.parameters();
            MetricParametersBuilder parametersBuilder = MetricParametersBuilder.builder();
            Set<wres.config.yaml.components.Threshold> valueThresholds = new HashSet<>();
            if ( Objects.nonNull( parameters ) )
            {
                parametersBuilder = MetricParametersBuilder.builder( metric.parameters() );
                valueThresholds.addAll( parameters.valueThresholds() );
            }

            valueThresholds.addAll( mappedThresholds );
            parametersBuilder.valueThresholds( valueThresholds );

            MetricParameters adjustedParameters = parametersBuilder.build();
            Metric adjustedMetric = MetricBuilder.builder()
                                                 .name( metric.name() )
                                                 .parameters( adjustedParameters )
                                                 .build();

            adjustedMetrics.add( adjustedMetric );
        }

        adjusted.metrics( adjustedMetrics );

        // Remove any features for which the threshold service returned no thresholds
        return WrdsThresholdFiller.removeFeaturesWithoutThresholds( adjusted, featuresWithThresholds, orientation );
    }

    /**
     * Removes any features from the declaration for which no thresholds were returned from the threshold service. Warn
     * when this occurs.
     *
     * @param builder the declaration builder
     * @param featuresWithThresholds the features that have thresholds
     * @param orientation the orientation of the dataset to which the feature names apply
     * @return the adjusted declaration
     */

    private static EvaluationDeclaration removeFeaturesWithoutThresholds( EvaluationDeclarationBuilder builder,
                                                                          Set<Geometry> featuresWithThresholds,
                                                                          DatasetOrientation orientation )
    {
        // Start with the singletons
        Set<GeometryTuple> singletons = builder.features()
                                               .geometries();

        Predicate<GeometryTuple> filter =
                next -> featuresWithThresholds.contains( WrdsThresholdFiller.getFeatureFor( next,
                                                                                            orientation ) );
        Set<GeometryTuple> adjustedSingletons
                = singletons.stream()
                            .filter( filter )
                            .collect( Collectors.toSet() );

        if ( LOGGER.isWarnEnabled() && adjustedSingletons.size() != singletons.size() )
        {
            Set<GeometryTuple> copy = new HashSet<>( singletons );
            copy.removeAll( adjustedSingletons );

            LOGGER.warn( "Discovered {} feature(s) for which thresholds were not available from WRDS. These features "
                         + "have been removed from the evaluation. The features are: {}.", copy.size(),
                         copy.stream()
                             .map( DeclarationFactory.PROTBUF_STRINGIFIER )
                             .toList() );
        }

        Features adjustedFeatures = new Features( adjustedSingletons );
        builder.features( adjustedFeatures );

        // Adjust the feature groups, if any
        if ( Objects.nonNull( builder.featureGroups() ) )
        {
            Set<GeometryGroup> originalGroups = builder.featureGroups()
                                                       .geometryGroups();
            Set<GeometryGroup> adjustedGroups = new HashSet<>();

            // Iterate the groups and adjust as needed
            for ( GeometryGroup nextGroup : originalGroups )
            {
                List<GeometryTuple> nextTuples = nextGroup.getGeometryTuplesList();
                List<GeometryTuple> adjusted = nextTuples.stream()
                                                         .filter( filter )
                                                         .toList();

                // Adjustments made?
                if ( adjusted.size() != nextTuples.size() )
                {
                    GeometryGroup adjustedGroup = nextGroup.toBuilder()
                                                           .clearGeometryTuples()
                                                           .addAllGeometryTuples( adjusted )
                                                           .build();
                    adjustedGroups.add( adjustedGroup );
                }
                else
                {
                    adjustedGroups.add( nextGroup );
                }
            }

            if ( LOGGER.isWarnEnabled() && !adjustedGroups.isEmpty() )
            {
                Set<GeometryGroup> copy = new HashSet<>( originalGroups );
                copy.removeAll( adjustedGroups );

                LOGGER.warn( "Discovered {} feature group(s) for which thresholds were not available from the WRDS for "
                             + "one or more of their component features. These features have been removed from the "
                             + "evaluation. Features were removed from the following feature groups: {}.",
                             copy.size(),
                             copy.stream()
                                 .map( GeometryGroup::getRegionName )
                                 .toList() );
            }

            FeatureGroups finalFeatureGroups = new FeatureGroups( adjustedGroups );
            builder.featureGroups( finalFeatureGroups );
        }

        return builder.build();
    }

    /**
     * Acquires the feature for the specified data orientation.
     * @param featureTuple the feature tuple
     * @param orientation the data orientation
     * @return the feature
     */

    private static Geometry getFeatureFor( GeometryTuple featureTuple, DatasetOrientation orientation )
    {
        return switch ( orientation )
                {
                    case LEFT -> featureTuple.getLeft();
                    case RIGHT -> featureTuple.getRight();
                    case BASELINE -> featureTuple.getBaseline();
                };
    }

    /**
     * Validates the thresholds against features
     * @param featureNames the feature names
     * @param thresholds the thresholds
     * @param featureAuthority the feature authority
     */

    private static void validate( Map<String, String> featureNames,
                                  Map<WrdsLocation, Set<Threshold>> thresholds,
                                  FeatureAuthority featureAuthority )
    {
        // No external thresholds declared
        if ( thresholds.isEmpty() )
        {
            LOGGER.debug( "No external thresholds to validate." );

            return;
        }

        LOGGER.debug( "Attempting to reconcile the {} features to evaluate with the {} features for which external "
                      + "thresholds are available.",
                      featureNames.size(),
                      thresholds.size() );

        // Identify the features that have thresholds
        Set<WrdsLocation> thresholdFeatures = thresholds.keySet();
        Set<String> thresholdFeatureNames =
                thresholdFeatures.stream()
                                 .map( n -> WrdsLocation.getNameForAuthority( featureAuthority, n ) )
                                 .collect( Collectors.toSet() );
        Set<String> featureNamesWithThresholds = new TreeSet<>( featureNames.keySet() );
        featureNamesWithThresholds.retainAll( thresholdFeatureNames );
        Set<String> featureNamesWithoutThresholds = new TreeSet<>( featureNames.keySet() );
        featureNamesWithoutThresholds.removeAll( thresholdFeatureNames );

        Set<String> thresholdNamesWithoutFeatures = new TreeSet<>( thresholdFeatureNames );
        thresholdNamesWithoutFeatures.removeAll( featureNames.keySet() );

        if ( ( !featureNamesWithoutThresholds.isEmpty() || !thresholdNamesWithoutFeatures.isEmpty() )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
                         "While attempting to reconcile the features to ",
                         "evaluate with the features for which thresholds ",
                         "are available, found ",
                         featureNames.size(),
                         " features to evaluate and ",
                         featureNamesWithThresholds.size(),
                         " features for which thresholds were found, but ",
                         featureNamesWithoutThresholds.size(),
                         " features for which thresholds could not be ",
                         "reconciled with features to evaluate. Features without ",
                         "thresholds will be skipped. If the number of features ",
                         "without thresholds is larger than expected, ensure that ",
                         "the source of feature names (featureNameFrom) is properly ",
                         "declared for the external thresholds. The ",
                         "declared features without thresholds are: ",
                         featureNamesWithoutThresholds,
                         ". The feature names associated with thresholds for which no features were declared are: ",
                         thresholdNamesWithoutFeatures );
        }

        if ( featureNamesWithoutThresholds.size() == featureNames.size() )
        {
            throw new ThresholdReadingException( "Failed to discover any features for which thresholds were "
                                                 + "available from the external sources declared. Add some thresholds "
                                                 + "for one or more of the declared features, declare some features "
                                                 + "for which thresholds are available or remove the declaration of "
                                                 + "thresholds altogether. The names of features encountered without "
                                                 + "thresholds are: "
                                                 + thresholdNamesWithoutFeatures
                                                 + ". Thresholds were not discovered for any of the following declared "
                                                 + "features: "
                                                 + featureNamesWithoutThresholds
                                                 + "." );
        }

        LOGGER.info( "Discovered {} features to evaluate for which external thresholds were available and {} "
                     + "features with external thresholds that could not be evaluated (e.g., because there was "
                     + "no data for these features).",
                     featureNamesWithThresholds.size(),
                     thresholdNamesWithoutFeatures.size() );
    }

    /**
     * Do not construct.
     */
    private WrdsThresholdFiller()
    {
    }
}
