package wres.io.thresholds.wrds;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdService;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.units.UnitMapper;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.thresholds.ThresholdReadingException;
import wres.statistics.generated.Geometry;
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

        if ( Objects.isNull( featureAuthority ) )
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

        Set<String> featureNames = DeclarationUtilities.getFeatureNamesFor( tuples, orientation );

        // Continue to read the thresholds
        Map<WrdsLocation, Set<Threshold>> thresholds = READER.readThresholds( service,
                                                                              unitMapper,
                                                                              featureNames,
                                                                              featureAuthority );

        LOGGER.trace( "Read the following thresholds from WRDS: {}.", thresholds );

        // Adjust the declaration and return it
        return WrdsThresholdFiller.getAdjustedDeclaration( evaluation,
                                                           thresholds,
                                                           featureAuthority,
                                                           orientation );
    }

    /**
     * Adjusts the declaration to include the prescribed thresholds.
     * @param evaluation the evaluation declaration to adjust
     * @param thresholds the thresholds
     * @param featureAuthority the feature authority to help with feature naming
     * @return the adjusted declaration
     */
    private static EvaluationDeclaration getAdjustedDeclaration( EvaluationDeclaration evaluation,
                                                                 Map<WrdsLocation, Set<Threshold>> thresholds,
                                                                 FeatureAuthority featureAuthority,
                                                                 DatasetOrientation orientation )
    {
        // Ordered, mapped thresholds
        Set<wres.config.yaml.components.Threshold> mappedThresholds = new HashSet<>();

        for ( Map.Entry<WrdsLocation, Set<Threshold>> nextEntry : thresholds.entrySet() )
        {
            WrdsLocation location = nextEntry.getKey();
            Set<Threshold> nextThresholds = nextEntry.getValue();

            String featureName = WrdsLocation.getNameForAuthority( featureAuthority, location );
            Geometry feature = Geometry.newBuilder().setName( featureName )
                                       .build();
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

        return adjusted.build();
    }

    /**
     * Do not construct.
     */
    private WrdsThresholdFiller()
    {
    }
}
