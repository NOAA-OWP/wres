package wres.reading.wrds.geography;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.datamodel.space.FeatureTuple;
import wres.reading.PreReadException;
import wres.reading.ReaderUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * <p>When feature tuples are declared sparsely (i.e., with some missing features) or declared implicitly, by
 * including a feature service, this class helps to render the features explicit. In other words, if a declaration says
 * "left feature is ABC" but leaves the right feature out, this class can help figure out the right feature name and
 * return objects that can be used for the remainder of the evaluation.
 *
 * @author Jesse Bickel
 * @author James Brown
 */

public class FeatureFiller
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureFiller.class );

    /** Maximum URL length. */
    private static final int MAX_SAFE_URL_LENGTH = 2000;

    /** Delimiter. */
    private static final String DELIMITER = "/";

    /**
     * Takes a dense or sparse declaration with regard to features and returns a dense declaration with regard to
     * features. Looks up the features from a feature service if the feature service is declared.
     *
     * @param evaluation The project declaration to fill out.
     * @return A project declaration, potentially with new List of Feature, but the same project declaration when
     *         feature declaration was fully dense.
     * @throws DeclarationException When fillFeatures cannot proceed with the given declaration due to incongruent
     *         declaration. This should be a user/caller exception.
     * @throws PreReadException When there is a problem getting data from the declared featureService or a problem
     *         with the returned data itself. In some cases will be a user/caller exception and in others an upstream
     *         service provider exception.
     * @throws NullPointerException When projectDeclaration or required contents is null. Required: inputs, left,
     *         right, pair, feature. This should be a programming error, because we expect basic config validation to
     *         occur prior to the call to fillFeatures.
     * @throws UnsupportedOperationException When this code could not handle a situation given. This should be a
     *         programming error. prior to the call to fillFeatures.
     */

    public static EvaluationDeclaration fillFeatures( EvaluationDeclaration evaluation )
    {
        Objects.requireNonNull( evaluation );

        wres.config.yaml.components.FeatureService featureService = evaluation.featureService();
        boolean requiresFeatureRequests = Objects.nonNull( evaluation.featureService() );

        // In many cases, no need to declare features, such as evaluations where
        // all the feature names are identical in sources on both sides or in
        // gridded evaluations.
        if ( !requiresFeatureRequests )
        {
            if ( ( Objects.isNull( evaluation.features() ) || evaluation.features()
                                                                        .geometries()
                                                                        .isEmpty() )
                 && ( Objects.isNull( evaluation.featureGroups() ) || evaluation.featureGroups()
                                                                                .geometryGroups()
                                                                                .isEmpty() ) )
            {
                LOGGER.debug( "No need to fill features: empty features and no requests required." );
                return evaluation;
            }

            Set<GeometryTuple> sparse = DeclarationInterpolator.getSparseFeaturesToInterpolate( evaluation );

            // Determine whether there are any sparse features either in a grouped or singleton context
            if ( sparse.isEmpty() )
            {
                LOGGER.debug( "No need to fill features, no sparse features and no requests needed, returning the "
                              + "input declaration." );
                return evaluation;
            }
            else
            {
                throw new DeclarationException( "Discovered "
                                                + sparse.size()
                                                + " sparsely declared geographic features, which could not be "
                                                + "interpolated because no feature service was declared. Please "
                                                + "declare a 'feature_service' or declare the same 'feature_authority' "
                                                + "for each dataset (to allow interpolation without a feature service) "
                                                + "or fully declare all features." );
            }
        }

        // Figure out if the same authority is used on multiple sides. If so, consolidate the features.
        FeatureAuthority leftAuthority =
                FeatureFiller.determineAuthority( evaluation.left() );
        FeatureAuthority rightAuthority =
                FeatureFiller.determineAuthority( evaluation.right() );
        FeatureAuthority baselineAuthority = null;

        boolean hasBaseline = Objects.nonNull( evaluation.baseline() );
        if ( hasBaseline )
        {
            baselineAuthority = FeatureFiller.determineAuthority( evaluation.baseline()
                                                                            .dataset() );
        }

        return FeatureFiller.fillFeatures( evaluation,
                                           featureService,
                                           leftAuthority,
                                           rightAuthority,
                                           baselineAuthority );
    }

    /**
     * Fills the supplied declaration with features.
     * @param evaluation the declaration
     * @param featureService the feature service
     * @param leftAuthority the left feature naming authority
     * @param rightAuthority the right feature naming authority
     * @param baselineAuthority the baseline feature naming authority
     * @return the filled declaration
     */

    private static EvaluationDeclaration fillFeatures( EvaluationDeclaration evaluation,
                                                       wres.config.yaml.components.FeatureService featureService,
                                                       FeatureAuthority leftAuthority,
                                                       FeatureAuthority rightAuthority,
                                                       FeatureAuthority baselineAuthority )
    {
        // Is this an actual feature service request or a response from a filesystem? If the latter, then any other
        // service declaration, such as groups, must be ignored and the response read as singleton features
        Set<GeometryTuple> filledSingletonFeatures;
        Set<GeometryGroup> filledGroupedFeatures = Collections.emptySet();
        if ( !ReaderUtilities.isWebSource( featureService.uri() ) )
        {
            LOGGER.warn( "While reading data from a feature service, discovered a URI that looks like a file path, {}."
                         + " This is allowed, but the response will be read as a plain list of features and all other "
                         + "feature service declaration, including 'groups', 'group' and 'pool', will be ignored.",
                         featureService.uri() );

            filledSingletonFeatures = FeatureFiller.readWrdsFeatures( featureService.uri(),
                                                                      leftAuthority,
                                                                      rightAuthority,
                                                                      baselineAuthority,
                                                                      DeclarationUtilities.hasBaseline( evaluation ) );
        }
        else
        {
            // Explicitly declared singleton features, plus any implicitly declared with "group" declaration
            filledSingletonFeatures = FeatureFiller.fillSingletonFeatures( evaluation,
                                                                           featureService,
                                                                           leftAuthority,
                                                                           rightAuthority,
                                                                           baselineAuthority );

            LOGGER.debug( "Filled these singleton features: {}", filledSingletonFeatures );

            // Explicitly declared feature groups
            filledGroupedFeatures = FeatureFiller.fillGroupedFeatures( evaluation,
                                                                       featureService,
                                                                       leftAuthority,
                                                                       rightAuthority,
                                                                       baselineAuthority );

            LOGGER.debug( "Filled these grouped features: {}", filledGroupedFeatures );
        }

        // No features?
        if ( filledSingletonFeatures.isEmpty()
             && filledGroupedFeatures.isEmpty() )
        {
            throw new PreReadException( "No geographic features found to evaluate." );
        }

        // Set the features and feature groups
        Features features = FeaturesBuilder.builder()
                                           .geometries( filledSingletonFeatures )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( filledGroupedFeatures )
                                                          .build();
        return EvaluationDeclarationBuilder.builder( evaluation )
                                           .features( features )
                                           .featureGroups( featureGroups )
                                           .build();
    }

    /**
     * Densifies singleton feature groups obtained from {@link EvaluationDeclaration#features()} and adds any
     * implicitly declared features attached to the {@link EvaluationDeclaration#featureService()} where the
     * {@link FeatureServiceGroup#pool()}} returns {@code false}.
     *
     * @param evaluation The evaluation.
     * @param featureService The element containing location service details.
     * @param leftAuthority The left authority, not null.
     * @param rightAuthority The right authority, not null.
     * @param baselineAuthority The baseline authority, possibly null.
     * @return A new list of features based on the given args.
     */

    private static Set<GeometryTuple> fillSingletonFeatures( EvaluationDeclaration evaluation,
                                                             wres.config.yaml.components.FeatureService featureService,
                                                             FeatureAuthority leftAuthority,
                                                             FeatureAuthority rightAuthority,
                                                             FeatureAuthority baselineAuthority )
    {
        // Any explicitly declared singleton features?
        Set<GeometryTuple> features = new HashSet<>();
        if ( Objects.nonNull( evaluation.features() ) )
        {
            features.addAll( evaluation.features()
                                       .geometries() );
        }

        Set<GeometryTuple> filledFeatures =
                FeatureFiller.fillFeatures( evaluation,
                                            featureService,
                                            features,
                                            leftAuthority,
                                            rightAuthority,
                                            baselineAuthority );

        // Add in group requests from the feature service
        Set<GeometryTuple> consolidatedFeatures = new HashSet<>( filledFeatures );

        // Add any implicitly declared features
        if ( Objects.nonNull( featureService ) && !featureService.featureGroups()
                                                                 .isEmpty() )
        {
            LOGGER.debug( "Discovered implicitly declared singleton features." );

            boolean hasBaseline = DeclarationUtilities.hasBaseline( evaluation );

            // Combine all the features from groups that are not to be pooled
            Set<GeometryTuple> fromGroups =
                    featureService.featureGroups()
                                  .stream()
                                  .filter( next -> !next.pool() )
                                  .flatMap( nextGroup -> FeatureFiller.getFeatureGroup( featureService,
                                                                                        nextGroup,
                                                                                        leftAuthority,
                                                                                        rightAuthority,
                                                                                        baselineAuthority,
                                                                                        hasBaseline )
                                                                      .stream() )
                                  .collect( Collectors.toSet() );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Found {} features within groups that should be treated as singletons.",
                              fromGroups.size() );
            }

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Found the following features within groups that should be treated as singletons: {}.",
                              fromGroups );
            }

            consolidatedFeatures.addAll( fromGroups );
        }

        return Collections.unmodifiableSet( consolidatedFeatures );
    }

    /**
     * Densifies any explicitly declared feature groups obtained from {@link EvaluationDeclaration#featureGroups()}
     * and adds any implicitly declared groups attached to the {@link EvaluationDeclaration#featureService()} where
     * the {@link FeatureServiceGroup#pool()} returns {@code true}.
     *
     * @param evaluation The evaluation.
     * @param featureService The element containing location service details.
     * @param leftAuthority The left authority, not null.
     * @param rightAuthority The right authority, not null.
     * @param baselineAuthority The baseline authority, possibly null.
     * @return A new list of grouped features based on the given args.
     */

    private static Set<GeometryGroup> fillGroupedFeatures( EvaluationDeclaration evaluation,
                                                           wres.config.yaml.components.FeatureService featureService,
                                                           FeatureAuthority leftAuthority,
                                                           FeatureAuthority rightAuthority,
                                                           FeatureAuthority baselineAuthority )
    {
        // Any explicitly declared grouped features?
        Set<GeometryGroup> featureGroups = new HashSet<>();
        if ( Objects.nonNull( evaluation.featureGroups() ) )
        {
            featureGroups.addAll( evaluation.featureGroups()
                                            .geometryGroups() );
        }

        LOGGER.debug( "Discovered {} feature groups with features to densify.", featureGroups.size() );

        Set<GeometryGroup> densifiedGroups = new HashSet<>();

        // Iterate through the groups and densify them
        for ( GeometryGroup nextGroup : featureGroups )
        {
            Set<GeometryTuple> features = new HashSet<>( nextGroup.getGeometryTuplesList() );

            Set<GeometryTuple> filledFeatures =
                    FeatureFiller.fillFeatures( evaluation,
                                                featureService,
                                                features,
                                                leftAuthority,
                                                rightAuthority,
                                                baselineAuthority );

            LOGGER.debug( "Densified feature group {}.", nextGroup.getRegionName() );

            GeometryGroup densifiedGroup = GeometryGroup.newBuilder()
                                                        .addAllGeometryTuples( filledFeatures )
                                                        .setRegionName( nextGroup.getRegionName() )
                                                        .build();

            densifiedGroups.add( densifiedGroup );
        }

        // Add any implicitly declared feature groups
        if ( Objects.nonNull( featureService ) && !featureService.featureGroups()
                                                                 .isEmpty() )
        {
            LOGGER.debug( "Discovered implicitly declared grouped features." );

            boolean hasBaseline = DeclarationUtilities.hasBaseline( evaluation );

            // Combine all the features from groups that are not to be pooled
            for ( FeatureServiceGroup nextGroup : featureService.featureGroups() )
            {
                if ( Boolean.TRUE.equals( nextGroup.pool() ) )
                {
                    Set<GeometryTuple> featuresToGroup = FeatureFiller.getFeatureGroup( featureService,
                                                                                        nextGroup,
                                                                                        leftAuthority,
                                                                                        rightAuthority,
                                                                                        baselineAuthority,
                                                                                        hasBaseline );

                    GeometryGroup group = GeometryGroup.newBuilder()
                                                       .addAllGeometryTuples( featuresToGroup )
                                                       .setRegionName( nextGroup.value() )
                                                       .build();

                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "Found implicit declaration for a feature pool called {} with type {}, which "
                                      + "contained {} features.",
                                      nextGroup.value(),
                                      nextGroup.group(),
                                      featuresToGroup.size() );
                    }

                    densifiedGroups.add( group );
                }
            }
        }

        return Collections.unmodifiableSet( densifiedGroups );
    }

    /**
     * Given a base uri and sparse features, generate fully-specified features, using the service to correlate features
     * that are not fully specified.
     *
     * @param evaluation The evaluation.
     * @param featureService The element containing location service details.
     * @param sparseFeatures The declared/sparse features.
     * @param leftAuthority The left authority, not null.
     * @param rightAuthority The right authority, not null.
     * @param baselineAuthority The baseline authority, possibly null.
     * @return A new list of features based on the given args.
     */

    private static Set<GeometryTuple> fillFeatures( EvaluationDeclaration evaluation,
                                                    wres.config.yaml.components.FeatureService featureService,
                                                    Set<GeometryTuple> sparseFeatures,
                                                    FeatureAuthority leftAuthority,
                                                    FeatureAuthority rightAuthority,
                                                    FeatureAuthority baselineAuthority )
    {
        boolean projectHasBaseline = Objects.nonNull( baselineAuthority );

        // Double-check for sparsity
        Set<GeometryTuple> actuallySparse
                = sparseFeatures.stream()
                                .filter( next -> !FeatureFiller.isFullyDeclared( next,
                                                                                 projectHasBaseline ) )
                                .collect( Collectors.toSet() );

        LOGGER.debug( "Attempting to fill these features: {}", actuallySparse );

        FillRequirements fillRequirements = FeatureFiller.getFillRequirements( actuallySparse,
                                                                               projectHasBaseline );

        LOGGER.debug( "Determined these fill requirements: {}", fillRequirements );

        Set<GeometryTuple> hasLeftNeedsRight = fillRequirements.hasLeftNeedsRight();
        Set<GeometryTuple> hasLeftNeedsBaseline = fillRequirements.hasLeftNeedsBaseline();
        Set<GeometryTuple> hasRightNeedsLeft = fillRequirements.hasRightNeedsLeft();
        Set<GeometryTuple> hasRightNeedsBaseline = fillRequirements.hasRightNeedsBaseline();
        Set<GeometryTuple> hasBaselineNeedsLeft = fillRequirements.hasBaselineNeedsLeft();
        Set<GeometryTuple> hasBaselineNeedsRight = fillRequirements.hasBaselineNeedsRight();

        // Map of from/to authority to set of strings to look up using from/to.
        // A means to consolidate "from NWS LID to USGS site code" regardless of
        // whether left or right or baseline duplicates dimensions. For example,
        // if left is nws_lid, right is usgs_site_code, and baseline is nws_lid,
        // we don't need to look up the usgs_site_code twice, nor do we need to
        // look up the nws_lid twice.
        Map<Pair<FeatureAuthority, FeatureAuthority>, Set<String>> needsLookup = new HashMap<>();

        // Need an intermediate map from original feature to new l/r/b values
        // because the same Feature may have needed two different new values,
        // which are found independently below.
        Map<GeometryTuple, String> withNewLeft = new HashMap<>( hasRightNeedsLeft.size()
                                                                + hasBaselineNeedsLeft.size() );
        Map<GeometryTuple, String> withNewRight = new HashMap<>( hasLeftNeedsRight.size()
                                                                 + hasBaselineNeedsRight.size() );
        Map<GeometryTuple, String> withNewBaseline = new HashMap<>( hasLeftNeedsBaseline.size()
                                                                    + hasRightNeedsBaseline.size() );

        // Update the features to lookup for each from/to authority pairing
        Function<GeometryTuple, String> leftGetter = f -> f.getLeft()
                                                           .getName();
        Function<GeometryTuple, String> rightGetter = f -> f.getRight()
                                                            .getName();
        Function<GeometryTuple, String> baselineGetter = f -> f.getBaseline()
                                                               .getName();
        FeatureFiller.setLookupMap( needsLookup,
                                    hasLeftNeedsRight,
                                    leftAuthority,
                                    rightAuthority,
                                    leftGetter );
        FeatureFiller.setLookupMap( needsLookup,
                                    hasLeftNeedsBaseline,
                                    leftAuthority,
                                    baselineAuthority,
                                    leftGetter );
        FeatureFiller.setLookupMap( needsLookup,
                                    hasRightNeedsLeft,
                                    rightAuthority,
                                    leftAuthority,
                                    rightGetter );
        FeatureFiller.setLookupMap( needsLookup,
                                    hasRightNeedsBaseline,
                                    rightAuthority,
                                    baselineAuthority,
                                    rightGetter );
        FeatureFiller.setLookupMap( needsLookup,
                                    hasBaselineNeedsLeft,
                                    baselineAuthority,
                                    leftAuthority,
                                    baselineGetter );
        FeatureFiller.setLookupMap( needsLookup,
                                    hasBaselineNeedsRight,
                                    baselineAuthority,
                                    rightAuthority,
                                    baselineGetter );

        LOGGER.debug( "These need lookups: {}", needsLookup );

        for ( Map.Entry<Pair<FeatureAuthority, FeatureAuthority>, Set<String>> nextEntry : needsLookup.entrySet() )
        {
            Pair<FeatureAuthority, FeatureAuthority> fromAndTo = nextEntry.getKey();
            Set<String> namesToLookUp = nextEntry.getValue();
            FeatureAuthority from = fromAndTo.getKey();
            FeatureAuthority to = fromAndTo.getValue();
            Map<String, String> found = FeatureService.bulkLookup( evaluation,
                                                                   featureService,
                                                                   from,
                                                                   to,
                                                                   namesToLookUp );

            LOGGER.debug( "Bulk lookup produced these features: {}", found );

            ToAuthority toAuthority = new ToAuthority( to, found );

            // Go through the has-this-needs-that lists
            if ( from == leftAuthority )
            {
                LOGGER.debug( "Filling features with a left authority of {}", leftAuthority );
                FeatureFiller.fromLeft( toAuthority,
                                        rightAuthority,
                                        baselineAuthority,
                                        hasLeftNeedsRight,
                                        hasLeftNeedsBaseline,
                                        withNewRight,
                                        withNewBaseline );
            }

            // No if/else, because both could be true.
            if ( from == rightAuthority )
            {
                LOGGER.debug( "Filling features with a right authority of {}", rightAuthority );
                FeatureFiller.fromRight( toAuthority,
                                         leftAuthority,
                                         baselineAuthority,
                                         hasRightNeedsLeft,
                                         hasRightNeedsBaseline,
                                         withNewLeft,
                                         withNewBaseline );
            }

            if ( from == baselineAuthority )
            {
                LOGGER.debug( "Filling features with a baseline authority of {}", baselineAuthority );
                FeatureFiller.fromBaseline( toAuthority,
                                            leftAuthority,
                                            rightAuthority,
                                            hasBaselineNeedsLeft,
                                            hasBaselineNeedsRight,
                                            withNewRight,
                                            withNewLeft );
            }
        }

        LOGGER.debug( "New left names: {}", withNewLeft );
        LOGGER.debug( "New right names: {}", withNewRight );
        LOGGER.debug( "New baseline names: {}", withNewBaseline );

        return FeatureFiller.getConsolidatedFeatures( withNewLeft,
                                                      withNewRight,
                                                      withNewBaseline,
                                                      sparseFeatures,
                                                      Objects.nonNull( baselineAuthority ) );
    }

    /**
     * Determines which features need to be filled.
     * @param sparseFeatures the features to inspect
     * @param projectHasBaseline whether the project has a baseline
     * @return the fill requirements
     */

    private static FillRequirements getFillRequirements( Set<GeometryTuple> sparseFeatures,
                                                         boolean projectHasBaseline )
    {
        FillRequirements featuresToFill = new FillRequirements( new HashSet<>(),
                                                                new HashSet<>(),
                                                                new HashSet<>(),
                                                                new HashSet<>(),
                                                                new HashSet<>(),
                                                                new HashSet<>() );

        // Go through the features, finding what was declared and not.
        for ( GeometryTuple feature : sparseFeatures )
        {
            boolean filled = FeatureFiller.setFillRequirement( feature, featuresToFill, projectHasBaseline );

            if ( !filled )
            {
                // There can be a feature with a custom feature dimension, but then that feature name must be
                // specified. However, the custom name cannot be used to look up another feature name.
                throw new IllegalStateException( "Each sparse feature must be added to a request, this one was not: "
                                                 + feature );
            }
        }

        return featuresToFill;
    }

    /**
     * Determines and sets the fill requirement for the specified feature.
     * @param feature the feature to inspect
     * @param featuresToFill the record of fill requirements to mutate
     * @param projectHasBaseline whether the project has a baseline
     * @return whether sparse features were added to the fill requirement
     */

    private static boolean setFillRequirement( GeometryTuple feature,
                                               FillRequirements featuresToFill,
                                               boolean projectHasBaseline )
    {
        boolean addedToSearchRequests = false;
        String leftName = feature.getLeft()
                                 .getName();
        String rightName = feature.getRight()
                                  .getName();
        String baselineName = feature.getBaseline()
                                     .getName();

        boolean leftPresent = feature.hasLeft() && !leftName.isBlank();
        boolean rightPresent = feature.hasRight()
                               && !rightName.isBlank();
        boolean baselinePresent = feature.hasBaseline()
                                  && !baselineName.isBlank();


        if ( leftPresent && !rightPresent )
        {
            featuresToFill.hasLeftNeedsRight()
                          .add( feature );
            addedToSearchRequests = true;
        }

        if ( projectHasBaseline && leftPresent && !baselinePresent )
        {
            featuresToFill.hasLeftNeedsBaseline()
                          .add( feature );
            addedToSearchRequests = true;
        }

        if ( rightPresent && !leftPresent )
        {
            featuresToFill.hasRightNeedsLeft()
                          .add( feature );
            addedToSearchRequests = true;
        }

        if ( projectHasBaseline && rightPresent && !baselinePresent )
        {
            featuresToFill.hasRightNeedsBaseline()
                          .add( feature );
        }

        if ( projectHasBaseline && baselinePresent && !leftPresent )
        {
            featuresToFill.hasBaselineNeedsLeft()
                          .add( feature );
            addedToSearchRequests = true;
        }

        if ( projectHasBaseline && baselinePresent && !rightPresent )
        {
            featuresToFill.hasBaselineNeedsRight()
                          .add( feature );
        }

        return addedToSearchRequests;
    }

    /**
     * Sets the features to fill in the supplied map.
     * @param needsLookup the map of features to mutate, indexed by from/to feature authority
     * @param featuresToFill the features to fill
     * @param fromAuthority the authority of the available feature
     * @param toAuthority the authority of the missing feature
     * @param nameGetter the feature name getter
     */

    private static void setLookupMap( Map<Pair<FeatureAuthority, FeatureAuthority>, Set<String>> needsLookup,
                                      Set<GeometryTuple> featuresToFill,
                                      FeatureAuthority fromAuthority,
                                      FeatureAuthority toAuthority,
                                      Function<GeometryTuple, String> nameGetter )
    {
        for ( GeometryTuple feature : featuresToFill )
        {
            Pair<FeatureAuthority, FeatureAuthority> fromToAuthority =
                    Pair.of( fromAuthority, toAuthority );

            needsLookup.putIfAbsent( fromToAuthority, new HashSet<>( featuresToFill.size() ) );

            needsLookup.get( fromToAuthority )
                       .add( nameGetter.apply( feature ) );
        }

        LOGGER.debug( "Discovered these features requiring lookup: {}", needsLookup );
    }

    /**
     * Looks for right and baseline features to pair with left features.
     * @param toAuthority the authority to map to
     * @param rightAuthority the right feature authority
     * @param baselineAuthority the baseline feature authority
     * @param hasLeftNeedsRight the left features that need right features
     * @param hasLeftNeedsBaseline the left features that need baseline features
     * @param withNewRight the map to mutate with new right features
     * @param withNewBaseline the map to mutate with new baseline features
     */
    private static void fromLeft( ToAuthority toAuthority,
                                  FeatureAuthority rightAuthority,
                                  FeatureAuthority baselineAuthority,
                                  Set<GeometryTuple> hasLeftNeedsRight,
                                  Set<GeometryTuple> hasLeftNeedsBaseline,
                                  Map<GeometryTuple, String> withNewRight,
                                  Map<GeometryTuple, String> withNewBaseline )
    {
        FeatureAuthority to = toAuthority.to();
        Map<String, String> found = toAuthority.found();

        if ( to.equals( rightAuthority ) )
        {
            FeatureFiller.fromLeftToRight( hasLeftNeedsRight, withNewRight, found );
        }

        // No if/else, because both could be true.
        if ( to.equals( baselineAuthority ) )
        {
            FeatureFiller.fromLeftToBaseline( hasLeftNeedsBaseline, withNewBaseline, found );
        }
    }

    /**
     * Looks for right features to pair with left features, updating the supplied map with the new pairings.
     * @param hasLeftNeedsRight the left features that need a right feature
     * @param withNewRight the map to mutate with new right features
     * @param found the overall map of features found
     */

    private static void fromLeftToRight( Set<GeometryTuple> hasLeftNeedsRight,
                                         Map<GeometryTuple, String> withNewRight,
                                         Map<String, String> found )
    {
        for ( GeometryTuple needed : hasLeftNeedsRight )
        {
            String rightName = found.get( needed.getLeft()
                                                .getName() );

            if ( Objects.nonNull( rightName ) )
            {
                String foundBefore =
                        withNewRight.put( needed, rightName );

                if ( Objects.nonNull( foundBefore ) )
                {
                    LOGGER.debug( "Overwrote previously-found right feature {} with {} for original {} from left.",
                                  foundBefore,
                                  rightName,
                                  needed );
                }
            }
            else
            {
                LOGGER.debug( "Not putting null value from left into right for {}",
                              needed );
            }
        }
    }

    /**
     * Looks for baseline features to pair with left features, updating the supplied map with the new pairings.
     * @param hasLeftNeedsBaseline the left features that need a baseline feature
     * @param withNewBaseline the map to mutate with new baseline features
     * @param found the overall map of features found
     */

    private static void fromLeftToBaseline( Set<GeometryTuple> hasLeftNeedsBaseline,
                                            Map<GeometryTuple, String> withNewBaseline,
                                            Map<String, String> found )
    {
        for ( GeometryTuple needed : hasLeftNeedsBaseline )
        {
            String baselineName = found.get( needed.getLeft()
                                                   .getName() );

            if ( Objects.nonNull( baselineName ) )
            {
                String foundBefore = withNewBaseline.put( needed, baselineName );

                if ( Objects.nonNull( foundBefore ) )
                {
                    LOGGER.debug( "Overwrote previously-found baseline feature {} with {} for original {} from left.",
                                  foundBefore,
                                  baselineName,
                                  needed );
                }
            }
            else
            {
                LOGGER.debug( "Not putting null from left into baseline for {}",
                              needed );
            }
        }
    }

    /**
     * Looks for left and baseline features to pair with right features.
     * @param toAuthority the authority to map to
     * @param leftAuthority the left feature authority
     * @param baselineAuthority the baseline feature authority
     * @param hasRightNeedsLeft the right features that need left features
     * @param hasRightNeedsBaseline the right features that need baseline features
     * @param withNewLeft the map to mutate with new left features
     * @param withNewBaseline the map to mutate with new baseline features
     */
    private static void fromRight( ToAuthority toAuthority,
                                   FeatureAuthority leftAuthority,
                                   FeatureAuthority baselineAuthority,
                                   Set<GeometryTuple> hasRightNeedsLeft,
                                   Set<GeometryTuple> hasRightNeedsBaseline,
                                   Map<GeometryTuple, String> withNewLeft,
                                   Map<GeometryTuple, String> withNewBaseline )
    {
        FeatureAuthority to = toAuthority.to();
        Map<String, String> found = toAuthority.found();

        if ( to.equals( leftAuthority ) )
        {
            FeatureFiller.fromRightToLeft( hasRightNeedsLeft, withNewLeft, found );
        }

        // No if/else, because both could be true.
        if ( to.equals( baselineAuthority ) )
        {
            FeatureFiller.fromRightToBaseline( hasRightNeedsBaseline, withNewBaseline, found );
        }
    }

    /**
     * Looks for left features to pair with right features, updating the supplied map with the new pairings.
     * @param hasRightNeedsLeft the right features that need a left feature
     * @param withNewLeft the map to mutate with new left features
     * @param found the overall map of features found
     */

    private static void fromRightToLeft( Set<GeometryTuple> hasRightNeedsLeft,
                                         Map<GeometryTuple, String> withNewLeft,
                                         Map<String, String> found )
    {
        for ( GeometryTuple needed : hasRightNeedsLeft )
        {
            String leftName = found.get( needed.getRight()
                                               .getName() );

            if ( Objects.nonNull( leftName ) )
            {
                String foundBefore =
                        withNewLeft.put( needed, leftName );

                if ( Objects.nonNull( foundBefore ) )
                {
                    LOGGER.debug( "Overwrote previously-found left feature {} with {} for original {} from right.",
                                  foundBefore,
                                  leftName,
                                  needed );
                }
            }
            else
            {
                LOGGER.debug( "Not putting null value from left into right for {}",
                              needed );
            }
        }
    }

    /**
     * Looks for baseline features to pair with right features, updating the supplied map with the new pairings.
     * @param hasRightNeedsBaseline the right features that need a baseline feature
     * @param withNewBaseline the map to mutate with new baseline features
     * @param found the overall map of features found
     */

    private static void fromRightToBaseline( Set<GeometryTuple> hasRightNeedsBaseline,
                                             Map<GeometryTuple, String> withNewBaseline,
                                             Map<String, String> found )
    {
        for ( GeometryTuple needed : hasRightNeedsBaseline )
        {
            String baselineName = found.get( needed.getRight()
                                                   .getName() );

            if ( Objects.nonNull( baselineName ) )
            {
                String foundBefore =
                        withNewBaseline.put( needed, baselineName );

                if ( Objects.nonNull( foundBefore ) )
                {
                    LOGGER.debug( "Overwrote previously-found baseline feature {} with {} for original {} from right",
                                  foundBefore,
                                  baselineName,
                                  needed );
                }
            }
            else
            {
                LOGGER.debug( "Not putting null value from right into baseline for {}",
                              needed );
            }
        }
    }

    /**
     * Looks for left and right features to pair with baseline features.
     * @param toAuthority the authority to map to
     * @param leftAuthority the left feature authority
     * @param rightAuthority the right feature authority
     * @param hasBaselineNeedsLeft the baseline features that need left features
     * @param hasBaselineNeedsRight the baseline features that need right features
     * @param withNewRight the map to mutate with new right features
     * @param withNewLeft the map to mutate with new left features
     */
    private static void fromBaseline( ToAuthority toAuthority,
                                      FeatureAuthority leftAuthority,
                                      FeatureAuthority rightAuthority,
                                      Set<GeometryTuple> hasBaselineNeedsLeft,
                                      Set<GeometryTuple> hasBaselineNeedsRight,
                                      Map<GeometryTuple, String> withNewRight,
                                      Map<GeometryTuple, String> withNewLeft )
    {
        FeatureAuthority to = toAuthority.to();
        Map<String, String> found = toAuthority.found();

        if ( to.equals( leftAuthority ) )
        {
            FeatureFiller.fromBaselineToLeft( hasBaselineNeedsLeft, withNewLeft, found );
        }

        if ( to.equals( rightAuthority ) )
        {
            FeatureFiller.fromBaselineToRight( hasBaselineNeedsRight, withNewRight, found );
        }
    }

    /**
     * Looks for left features to pair with baseline features, updating the supplied map with the new pairings.
     * @param hasBaselineNeedsLeft the baseline features that need a left feature
     * @param withNewLeft the map to mutate with new left features
     * @param found the overall map of features found
     */

    private static void fromBaselineToLeft( Set<GeometryTuple> hasBaselineNeedsLeft,
                                            Map<GeometryTuple, String> withNewLeft,
                                            Map<String, String> found )
    {
        for ( GeometryTuple needed : hasBaselineNeedsLeft )
        {
            String leftName = found.get( needed.getBaseline()
                                               .getName() );

            if ( Objects.nonNull( leftName ) )
            {
                String foundBefore =
                        withNewLeft.put( needed, leftName );

                if ( Objects.nonNull( foundBefore ) )
                {
                    LOGGER.debug( "Overwrote previously-found left feature {} with {} for original {} from baseline.",
                                  foundBefore,
                                  leftName,
                                  needed );
                }
            }
            else
            {
                LOGGER.debug( "Not putting null value from baseline into left for {}",
                              needed );
            }
        }
    }

    /**
     * Looks for right features to pair with baseline features, updating the supplied map with the new pairings.
     * @param hasBaselineNeedsRight the baseline features that need a right feature
     * @param withNewRight the map to mutate with new right features
     * @param found the overall map of features found
     */

    private static void fromBaselineToRight( Set<GeometryTuple> hasBaselineNeedsRight,
                                             Map<GeometryTuple, String> withNewRight,
                                             Map<String, String> found )
    {
        for ( GeometryTuple needed : hasBaselineNeedsRight )
        {
            String rightName = found.get( needed.getBaseline()
                                                .getName() );

            if ( Objects.nonNull( rightName ) )
            {
                String foundBefore =
                        withNewRight.put( needed, rightName );

                if ( Objects.nonNull( foundBefore ) )
                {
                    LOGGER.debug( "Overwrote previously-found right feature {} with {} for original {} from baseline.",
                                  foundBefore,
                                  rightName,
                                  needed );
                }
            }
            else
            {
                LOGGER.debug( "Not putting null value from baseline into right for {}",
                              needed );
            }
        }
    }

    /**
     * Consolidates the features with new left, right or baseline names.
     * @param withNewLeft the features with new left names
     * @param withNewRight the features with new right names
     * @param withNewBaseline the features with new baseline names
     * @param sparseFeatures the sparse features
     * @param hasBaseline whether there are baseline features
     * @return the consolidated dense features
     */
    private static Set<GeometryTuple> getConsolidatedFeatures( Map<GeometryTuple, String> withNewLeft,
                                                               Map<GeometryTuple, String> withNewRight,
                                                               Map<GeometryTuple, String> withNewBaseline,
                                                               Set<GeometryTuple> sparseFeatures,
                                                               boolean hasBaseline )
    {
        Set<GeometryTuple> consolidatedFeatures = new HashSet<>();

        // Features that could not be correlated/densified
        Set<FeatureTuple> stillSparse = new HashSet<>();

        for ( GeometryTuple feature : sparseFeatures )
        {
            String newLeftName = withNewLeft.get( feature );
            String newRightName = withNewRight.get( feature );
            String newBaselineName = withNewBaseline.get( feature );

            LOGGER.debug( "Discovered these names for feature {}: ({},{},{})",
                          feature,
                          newLeftName,
                          newRightName,
                          newBaselineName );

            GeometryTuple.Builder newFeature = GeometryTuple.newBuilder( feature );
            if ( Objects.nonNull( newLeftName ) )
            {
                newFeature.setLeft( Geometry.newBuilder()
                                            .setName( newLeftName ) );
            }

            if ( Objects.nonNull( newRightName ) )
            {
                newFeature.setRight( Geometry.newBuilder()
                                             .setName( newRightName ) );
            }

            if ( Objects.nonNull( newBaselineName ) )
            {
                newFeature.setBaseline( Geometry.newBuilder()
                                                .setName( newBaselineName ) );
            }

            // Only add dense/correlated feature tuples
            GeometryTuple complete = newFeature.build();
            if ( FeatureFiller.isDense( complete, hasBaseline ) )
            {
                consolidatedFeatures.add( complete );
            }
            else if ( LOGGER.isWarnEnabled() )
            {
                FeatureTuple featureTuple = FeatureTuple.of( complete );
                stillSparse.add( featureTuple );
            }
        }

        // Warn about features that could not be correlated
        if ( !stillSparse.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The evaluation contained {} sparse feature(s). However, {} of these feature(s) could not be "
                         + "correlated using WRDS and will be omitted from the evaluation: {}.",
                         sparseFeatures.size(),
                         stillSparse.size(),
                         stillSparse );
        }

        return Collections.unmodifiableSet( consolidatedFeatures );
    }

    /**
     * @param feature the feature to test
     * @param hasBaseline whether the feature tuple has a baseline
     * @return whether the feature tuple is dense
     */

    private static boolean isDense( GeometryTuple feature, boolean hasBaseline )
    {
        if ( !feature.hasLeft() || !feature.hasRight() )
        {
            return false;
        }

        return !hasBaseline || feature.hasBaseline();
    }

    /**
     * Get features requested within the given feature service for a specified 
     * feature group.
     * @param featureService The feature service to use with a base url, both required.
     * @param featureGroup the feature group, required.
     * @param leftAuthority The left dimension discovered, required.
     * @param rightAuthority The right dimension discovered, required.
     * @param baselineAuthority The baseline dimension discovered, null if none.
     * @param hasBaseline whether the evaluation has a baseline dataset
     * @return A list of fully populated features.
     */
    private static Set<GeometryTuple> getFeatureGroup( wres.config.yaml.components.FeatureService featureService,
                                                       FeatureServiceGroup featureGroup,
                                                       FeatureAuthority leftAuthority,
                                                       FeatureAuthority rightAuthority,
                                                       FeatureAuthority baselineAuthority,
                                                       boolean hasBaseline )
    {
        Objects.requireNonNull( featureGroup );
        Objects.requireNonNull( featureService, "Cannot declare a feature service group without a feature service." );
        Objects.requireNonNull( featureService.uri(), "Cannot declare a feature service group without a base "
                                                      + "url." );
        Objects.requireNonNull( leftAuthority );
        Objects.requireNonNull( rightAuthority );

        URI featureServiceBaseUri = featureService.uri();

        if ( Objects.isNull( featureGroup.group() )
             || Objects.isNull( featureGroup.value() )
             || featureGroup.group()
                            .isBlank()
             || featureGroup.value()
                            .isBlank() )
        {
            throw new DeclarationException( "The type and value of each feature group must be specified. Instead, "
                                            + "found "
                                            + featureGroup );
        }

        String path = featureServiceBaseUri.getPath();
        String fullPath = path + DELIMITER
                          + featureGroup.group()
                          + DELIMITER
                          + featureGroup.value()
                          + DELIMITER
                          + FeatureService.CROSSWALK_ONLY_FLAG;

        URI uri = featureServiceBaseUri.resolve( fullPath )
                                       .normalize();

        if ( uri.toString()
                .length() > MAX_SAFE_URL_LENGTH )
        {
            throw new DeclarationException( "The URL to be created using "
                                            + "this configuration would "
                                            + "be greater than "
                                            + MAX_SAFE_URL_LENGTH
                                            + "characters. Please use a "
                                            + "shorter base URL or group "
                                            + "type or group value." );
        }

        return FeatureFiller.readWrdsFeatures( uri,
                                               leftAuthority,
                                               rightAuthority,
                                               baselineAuthority,
                                               hasBaseline );
    }

    /**
     * Reads features from the WRDS feature service.
     * @param uri the service uri
     * @param leftAuthority the left feature authority
     * @param rightAuthority the right feature authority
     * @param baselineAuthority the baseline feature authority
     * @param hasBaseline whether the evaluation has a baseline dataset
     * @return the features read from WRDS
     */
    private static Set<GeometryTuple> readWrdsFeatures( URI uri,
                                                        FeatureAuthority leftAuthority,
                                                        FeatureAuthority rightAuthority,
                                                        FeatureAuthority baselineAuthority,
                                                        boolean hasBaseline )
    {
        Set<GeometryTuple> features = new HashSet<>();

        List<Location> locations = FeatureService.read( uri );
        Set<String> missingTuples = new TreeSet<>();
        for ( Location location : locations )
        {
            String leftName = Location.getNameForAuthority( leftAuthority, location );
            String rightName = Location.getNameForAuthority( rightAuthority, location );
            String baselineName = Location.getNameForAuthority( baselineAuthority, location );

            // If all names are present, create a feature
            if ( FeatureFiller.isValidFeatureName( leftName )
                 && FeatureFiller.isValidFeatureName( rightName )
                 // Either no baseline name is required or the baseline name is valid: see Redmine issue #116808
                 && ( !hasBaseline || FeatureFiller.isValidFeatureName( baselineName ) ) )
            {
                GeometryTuple.Builder featureFromGroup = GeometryTuple.newBuilder()
                                                                      .setLeft( Geometry.newBuilder()
                                                                                        .setName( leftName ) )
                                                                      .setRight( Geometry.newBuilder()
                                                                                         .setName( rightName ) );

                // Baseline?
                if ( hasBaseline )
                {
                    featureFromGroup.setBaseline( Geometry.newBuilder()
                                                          .setName( baselineName ) );
                }

                features.add( featureFromGroup.build() );
            }
            else if ( LOGGER.isWarnEnabled() )
            {
                StringJoiner joiner = new StringJoiner( ",", "(", ")" );
                joiner.add( leftName )
                      .add( rightName );
                if ( hasBaseline )
                {
                    joiner.add( baselineName );
                }
                missingTuples.add( joiner.toString() );
            }
        }

        // Warn about missing tuples: see Redmine issue #116808
        if ( LOGGER.isWarnEnabled() && !missingTuples.isEmpty() )
        {
            LOGGER.warn( "While reading features from {}, discovered some feature names that were required but "
                         + "unavailable. The following feature tuples had one or more missing names and the resulting "
                         + "feature tuples will not be evaluated: {}.", uri, missingTuples );
        }

        return Collections.unmodifiableSet( features );
    }

    /**
     * Inspects a feature name for validity.
     * @param featureName the feature name
     * @return whether it is valid
     */
    private static boolean isValidFeatureName( String featureName )
    {
        return Objects.nonNull( featureName ) && !featureName.isBlank();
    }

    /**
     * @param potentiallySparseFeature a potentially sparse feature
     * @param hasBaseline whether the evaluation has a baseline
     * @return whether the feature is sparse
     */

    private static boolean isFullyDeclared( GeometryTuple potentiallySparseFeature,
                                            boolean hasBaseline )
    {
        boolean leftAndRightDeclared =
                potentiallySparseFeature.hasLeft()
                && !potentiallySparseFeature.getLeft()
                                            .getName()
                                            .isBlank()
                && potentiallySparseFeature.hasRight()
                && !potentiallySparseFeature.getRight()
                                            .getName()
                                            .isBlank();

        if ( !hasBaseline )
        {
            return leftAndRightDeclared;
        }

        return leftAndRightDeclared
               && potentiallySparseFeature.hasBaseline()
               && !potentiallySparseFeature.getBaseline()
                                           .getName()
                                           .isBlank();
    }

    /**
     * Determine the location authority of the given datasource.
     * @param dataset the data source declaration within projectConfig.
     * @return the authority or null if a custom authority.
     * @throws NullPointerException When any argument is null.
     * @throws UnsupportedOperationException When code cannot handle something.
     */

    private static FeatureAuthority determineAuthority( Dataset dataset )
    {
        Objects.requireNonNull( dataset );

        FeatureAuthority featureAuthority = dataset.featureAuthority();

        if ( Objects.isNull( featureAuthority ) )
        {
            throw new DeclarationException( "Unable to determine the geographic feature authority to use for a source "
                                            + "and cannot ask the service without a feature authority inferred from the "
                                            + "source interface (e.g. WRES knows interface usgs_nwis uses the "
                                            + "usgs_site_code authority) or explicitly declared in the "
                                            + "'feature_authority' attribute of the dataset. Valid values include: "
                                            + FeatureFiller.getValidFeatureAuthorityValues() );

        }

        return featureAuthority;
    }

    /**
     * Convenience method for printing error messages. Displays all valid values of the {@link FeatureAuthority}.
     * @return the valid values of a {@link FeatureAuthority}, concatenated for display
     */
    private static String getValidFeatureAuthorityValues()
    {
        StringJoiner joiner = new StringJoiner( "', '", "'", "'" );

        for ( FeatureAuthority authority : FeatureAuthority.values() )
        {
            joiner.add( authority.toString()
                                 .toLowerCase() );
        }

        return joiner.toString();
    }

    /**
     * Record of features that need to be filled.
     * @param hasLeftNeedsRight the features with a left name requiring a right name
     * @param hasLeftNeedsBaseline the features with a left name requiring a baseline name
     * @param hasRightNeedsLeft the features with a right name requiring a left name
     * @param hasRightNeedsBaseline the features with  right name requiring a baseline name
     * @param hasBaselineNeedsLeft the features with a baseline name requiring a left name
     * @param hasBaselineNeedsRight the features with a baseline name requiring a right name
     */
    private record FillRequirements( Set<GeometryTuple> hasLeftNeedsRight,
                                     Set<GeometryTuple> hasLeftNeedsBaseline,
                                     Set<GeometryTuple> hasRightNeedsLeft,
                                     Set<GeometryTuple> hasRightNeedsBaseline,
                                     Set<GeometryTuple> hasBaselineNeedsLeft,
                                     Set<GeometryTuple> hasBaselineNeedsRight ) {}

    /**
     * A small bag of parameters needed to map to a named feature authority.
     * @param to the authority required
     * @param found the feature mappings to use
     */
    private record ToAuthority( FeatureAuthority to,
                                Map<String, String> found ) {}

    /**
     * Hidden constructor.
     */
    private FeatureFiller()
    {
    }
}
