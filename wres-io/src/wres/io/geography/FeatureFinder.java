package wres.io.geography;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.FeatureDimension;
import wres.config.generated.FeatureGroup;
import wres.config.generated.FeaturePool;
import wres.config.generated.FeatureService;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.reading.PreIngestException;

/**
 * When sparse features are declared, this class helps build the features.
 *
 * In other words, if a declaration says "left feature is ABC" but leaves the
 * right feature out, this class can help figure out the right feature name
 * and return objects that can be used for the remainder of the evaluation.
 *
 * When a group of features is declared with a feature service, this class makes
 * the requests needed and adds the found features to the declaration used by
 * the rest of the WRES pipeline.
 * 
 * TODO: decouple this implementation from a particular feature service, 
 * currently {@link WrdsFeatureService}, by injecting a service interface.
 */

public class FeatureFinder
{
    private static final String DELIMITER = "/";

    private static final String WRES_NOT_READY_TO_LOOK_UP = "WRES not ready to look up ";

    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureFinder.class );

    private static final int MAX_SAFE_URL_LENGTH = 2000;

    private FeatureFinder()
    {
        // Do not construct
    }

    /**
     * Takes a dense or sparse declaration with regard to features and
     * returns a dense declaration with regard to features. Looks up the
     * features from a feature service if the feature service is declared.
     *
     * @param projectDeclaration The project declaration to fill out.
     * @return A project declaration, potentially with new List of Feature, but
     * the same project declaration when feature declaration was fully dense.
     * @throws ProjectConfigException When fillFeatures cannot proceed with the
     * given declaration due to incongruent declaration. This should be a
     * user/caller exception.
     * @throws PreIngestException When there is a problem getting data from the
     * declared featureService or a problem with the returned data itself.
     * In some cases will be a user/caller exception and in others an upstream
     * service provider exception.
     * @throws NullPointerException When projectDeclaration or required contents
     * is null. Required: inputs, left, right, pair, feature. This should be a
     * programming error, because we expect basic config validation to occur
     * prior to the call to fillFeatures.
     * @throws UnsupportedOperationException When this code could not handle a
     * situation given. This should be a programming error.
     * prior to the call to fillFeatures.
     */

    public static ProjectConfig fillFeatures( ProjectConfig projectDeclaration )
    {
        Objects.requireNonNull( projectDeclaration );
        boolean hasBaseline = Objects.nonNull( projectDeclaration.getInputs()
                                                                 .getBaseline() );

        PairConfig pairConfig = projectDeclaration.getPair();
        boolean requiresFeatureRequests = false;

        if ( Objects.nonNull( pairConfig.getFeatureService() )
             && Objects.nonNull( pairConfig.getFeatureService()
                                           .getGroup() ) )
        {
            requiresFeatureRequests = true;
        }

        // In many cases, no need to declare features, such as evaluations where
        // all the feature names are identical in sources on both sides or in
        // gridded evaluations.
        if ( pairConfig.getFeature()
                       .isEmpty()
             && pairConfig.getFeatureGroup()
                          .isEmpty()
             && !requiresFeatureRequests )
        {
            LOGGER.debug( "No need to fill features: empty features and no requests required." );
            return projectDeclaration;
        }

        // Determine whether there are any sparse features either in a grouped or singleton context
        if ( !requiresFeatureRequests
             && FeatureFinder.getSparseFeatures( pairConfig, hasBaseline )
                             .isEmpty() )
        {
            LOGGER.debug( "No need to fill features, no sparse features and no requests needed, returning the input "
                          + "declaration." );
            return projectDeclaration;
        }

        // Figure out if the same authority is used on multiple sides. If so,
        // consolidate lists.
        FeatureDimension leftDimension =
                FeatureFinder.determineDimension( projectDeclaration.getInputs().getLeft() );
        FeatureDimension rightDimension =
                FeatureFinder.determineDimension( projectDeclaration.getInputs().getRight() );
        FeatureDimension baselineDimension = null;
        
        if ( hasBaseline )
        {
            baselineDimension = FeatureFinder.determineDimension( projectDeclaration.getInputs().getBaseline() );
        }

        PairConfig originalPairDeclaration = projectDeclaration.getPair();
        FeatureService declaredFeatureService = originalPairDeclaration.getFeatureService();

        // Explicitly declared singleton features, plus any implicitly declared with "group" declaration
        List<Feature> filledSingletonFeatures = FeatureFinder.fillSingletonFeatures( projectDeclaration,
                                                                                     declaredFeatureService,
                                                                                     leftDimension,
                                                                                     rightDimension,
                                                                                     baselineDimension );

        // Explicitly declared feature groups
        List<FeaturePool> filledGroupedFeatures = FeatureFinder.fillGroupedFeatures( projectDeclaration,
                                                                                     declaredFeatureService,
                                                                                     leftDimension,
                                                                                     rightDimension,
                                                                                     baselineDimension );

        if ( filledSingletonFeatures.isEmpty() && filledGroupedFeatures.isEmpty() )
        {
            throw new PreIngestException( "No geographic features found to evaluate." );
        }

        PairConfig featurefulPairDeclaration = new PairConfig( originalPairDeclaration.getUnit(),
                                                               originalPairDeclaration.getUnitAlias(),
                                                               originalPairDeclaration.getFeatureService(),
                                                               filledSingletonFeatures,
                                                               filledGroupedFeatures,
                                                               originalPairDeclaration.getGridSelection(),
                                                               originalPairDeclaration.isByTimeSeries(),
                                                               originalPairDeclaration.getLeadHours(),
                                                               originalPairDeclaration.getAnalysisDurations(),
                                                               originalPairDeclaration.getDates(),
                                                               originalPairDeclaration.getIssuedDates(),
                                                               originalPairDeclaration.getSeason(),
                                                               originalPairDeclaration.getValues(),
                                                               originalPairDeclaration.getDesiredTimeScale(),
                                                               originalPairDeclaration.getIssuedDatesPoolingWindow(),
                                                               originalPairDeclaration.getValidDatesPoolingWindow(),
                                                               originalPairDeclaration.getLeadTimesPoolingWindow(),
                                                               originalPairDeclaration.getCrossPair(),
                                                               originalPairDeclaration.getLabel() );
        return new ProjectConfig( projectDeclaration.getInputs(),
                                  featurefulPairDeclaration,
                                  projectDeclaration.getMetrics(),
                                  projectDeclaration.getOutputs(),
                                  projectDeclaration.getLabel(),
                                  projectDeclaration.getName() );
    }

    /**
     * @param pairConfig the pair declaration, not null
     * @param hasBaseline is true if the declaration contains a baseline
     * @return the sparse features, including singletons and grouped features
     */

    private static List<Feature> getSparseFeatures( PairConfig pairConfig, boolean hasBaseline )
    {
        List<Feature> featureList = pairConfig.getFeature();
        Predicate<Feature> filter = null;

        // Determine the correct type of filter for the feature
        if ( hasBaseline )
        {
            filter = feature -> Objects.isNull( feature.getLeft() )
                                || Objects.isNull( feature.getRight() )
                                || Objects.isNull( feature.getBaseline() );
        }
        else
        {
            filter = feature -> Objects.isNull( feature.getLeft() )
                                || Objects.isNull( feature.getRight() );
        }

        // Find sparse singletons
        List<Feature> sparseFeatures = featureList.stream()
                                                  .filter( filter )
                                                  .collect( Collectors.toList() );

        // Find sparse grouped features
        List<Feature> sparseGroupedFeatures = pairConfig.getFeatureGroup()
                                                        .stream()
                                                        .flatMap( nextGroup -> nextGroup.getFeature().stream() )
                                                        .filter( filter )
                                                        .collect( Collectors.toList() );
        sparseFeatures.addAll( sparseGroupedFeatures );

        return Collections.unmodifiableList( sparseFeatures );
    }
    
    /**
     * Densifies singleton feature groups obtained from {@link PairConfig#getFeature()}.
     *
     * @param projectDeclaration The project declaration.
     * @param featureService The element containing location service details.
     * @param leftDimension The left dimension, not null.
     * @param rightDimension The right dimension, not null.
     * @param baselineDimension The baseline dimension, possibly null.
     * @return A new list of features based on the given args.
     */
    
    private static List<Feature> fillSingletonFeatures( ProjectConfig projectDeclaration,
                                                        FeatureService featureService,
                                                        FeatureDimension leftDimension,
                                                        FeatureDimension rightDimension,
                                                        FeatureDimension baselineDimension )
    {
        List<Feature> features = projectDeclaration.getPair()
                                                   .getFeature();

        List<Feature> filledFeatures =
                FeatureFinder.fillFeatures( projectDeclaration,
                                            featureService,
                                            features,
                                            leftDimension,
                                            rightDimension,
                                            baselineDimension );

        // Add in group requests from the feature service
        List<Feature> consolidatedFeatures = new ArrayList<>( filledFeatures );

        // Add any implicitly declared features
        if ( Objects.nonNull( featureService ) && Objects.nonNull( featureService.getGroup() ) )
        {
            // Combine all the features from groups that are not to be pooled
            List<Feature> fromGroups =
                    featureService.getGroup()
                                  .stream()
                                  .filter( next -> !next.isPool() )
                                  .flatMap( nextGroup -> FeatureFinder.getFeatureGroup( featureService,
                                                                                        nextGroup,
                                                                                        leftDimension,
                                                                                        rightDimension,
                                                                                        baselineDimension )
                                                                      .stream() )
                                  .collect( Collectors.toList() );

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

        return Collections.unmodifiableList( consolidatedFeatures );
    }

    /**
     * Densifies any explicitly declared feature groups obtained from {@link PairConfig#getFeatureGroup()} and adds any
     * implicitly declared groups.
     *
     * @param projectDeclaration The project declaration.
     * @param featureService The element containing location service details.
     * @param leftDimension The left dimension, not null.
     * @param rightDimension The right dimension, not null.
     * @param baselineDimension The baseline dimension, possibly null.
     * @return A new list of grouped features based on the given args.
     */

    private static List<FeaturePool> fillGroupedFeatures( ProjectConfig projectDeclaration,
                                                          FeatureService featureService,
                                                          FeatureDimension leftDimension,
                                                          FeatureDimension rightDimension,
                                                          FeatureDimension baselineDimension )
    {
        List<FeaturePool> featureGroups = projectDeclaration.getPair()
                                                            .getFeatureGroup();

        LOGGER.debug( "Discovered {} feature groups with features to densify.", featureGroups.size() );

        List<FeaturePool> densifiedGroups = new ArrayList<>();

        // Iterate through the groups and densify them
        for ( FeaturePool nextGroup : featureGroups )
        {
            List<Feature> features = nextGroup.getFeature();

            List<Feature> filledFeatures =
                    FeatureFinder.fillFeatures( projectDeclaration,
                                                featureService,
                                                features,
                                                leftDimension,
                                                rightDimension,
                                                baselineDimension );

            LOGGER.debug( "Densified feature group {}.", nextGroup.getName() );

            FeaturePool densifiedGroup = new FeaturePool( filledFeatures, nextGroup.getName() );

            densifiedGroups.add( densifiedGroup );
        }

        // Add any implicitly declared feature groups
        if ( Objects.nonNull( featureService ) && Objects.nonNull( featureService.getGroup() ) )
        {
            // Combine all the features from groups that are not to be pooled
            for ( FeatureGroup nextGroup : featureService.getGroup() )
            {
                if ( nextGroup.isPool() )
                {
                    List<Feature> featuresToGroup = FeatureFinder.getFeatureGroup( featureService,
                                                                                   nextGroup,
                                                                                   leftDimension,
                                                                                   rightDimension,
                                                                                   baselineDimension );

                    FeaturePool pool = new FeaturePool( featuresToGroup, nextGroup.getValue() );

                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "Found implicit declaration for a feature pool called {} with type {}, which "
                                      + "contained {} features.",
                                      nextGroup.getValue(),
                                      nextGroup.getType(),
                                      featuresToGroup.size() );
                    }

                    densifiedGroups.add( pool );
                }
            }
        }

        return Collections.unmodifiableList( densifiedGroups );
    }
    
    /**
     * Given a base uri and sparse features, generate fully-specified features,
     * using the service to correlate features that are not fully specified.
     *
     * @param projectConfig The project declaration.
     * @param featureService The element containing location service details.
     * @param sparseFeatures The declared/sparse features.
     * @param leftDimension The left dimension, not null.
     * @param rightDimension The right dimension, not null.
     * @param baselineDimension The baseline dimension, possibly null.
     * @return A new list of features based on the given args.
     */

    private static List<Feature> fillFeatures( ProjectConfig projectConfig,
                                               FeatureService featureService,
                                               List<Feature> sparseFeatures,
                                               FeatureDimension leftDimension,
                                               FeatureDimension rightDimension,
                                               FeatureDimension baselineDimension )
    {
        List<Feature> hasLeftNeedsRight = new ArrayList<>();
        List<Feature> hasLeftNeedsBaseline = new ArrayList<>();
        List<Feature> hasRightNeedsLeft = new ArrayList<>();
        List<Feature> hasRightNeedsBaseline = new ArrayList<>();
        List<Feature> hasBaselineNeedsLeft = new ArrayList<>();
        List<Feature> hasBaselineNeedsRight = new ArrayList<>();

        boolean projectHasBaseline = Objects.nonNull( baselineDimension );
        
        // Go through the features, finding what was declared and not.
        for ( Feature feature : sparseFeatures )
        {
            // Simple case: all elements are present.
            if ( FeatureFinder.isFullyDeclared( feature, projectHasBaseline ) )
            {
                continue;
            }

            boolean addedToSearchRequests = false;
            String leftName = feature.getLeft();
            String rightName = feature.getRight();
            String baselineName = feature.getBaseline();

            boolean leftPresent = Objects.nonNull( leftName )
                                  && !leftName.isBlank();
            boolean rightPresent = Objects.nonNull( rightName )
                                   && !rightName.isBlank();
            boolean baselinePresent = Objects.nonNull( baselineName )
                                      && !baselineName.isBlank();


            if ( leftPresent && !rightPresent )
            {
                hasLeftNeedsRight.add( feature );
                addedToSearchRequests = true;
            }

            if ( projectHasBaseline && leftPresent && !baselinePresent )
            {
                hasLeftNeedsBaseline.add( feature );
                addedToSearchRequests = true;
            }

            if ( rightPresent && !leftPresent )
            {
                hasRightNeedsLeft.add( feature );
                addedToSearchRequests = true;
            }

            if ( projectHasBaseline && rightPresent && !baselinePresent )
            {
                hasRightNeedsBaseline.add( feature );
                addedToSearchRequests = true;
            }

            if ( projectHasBaseline && baselinePresent && !leftPresent )
            {
                hasBaselineNeedsLeft.add( feature );
                addedToSearchRequests = true;
            }

            if ( projectHasBaseline && baselinePresent && !rightPresent )
            {
                hasBaselineNeedsRight.add( feature );
                addedToSearchRequests = true;
            }

            if ( !addedToSearchRequests )
            {
                LOGGER.debug( "This feature was not added to search requests: {}",
                              feature );
                // There can be a feature with a custom feature dimension, but
                // then that feature name must be specified. However, the custom
                // name cannot be used to look up another feature name.
                throw new IllegalStateException( "Each sparse feature must be added to a request, this one was not: "
                                                 + feature );
            }
        }


        // Map of from/to dimensions to set of Strings to look up using from/to.
        // A means to consolidate "from NWS LID to USGS site code" regardless of
        // whether left or right or baseline duplicates dimensions. For example,
        // if left is nws_lid, right is usgs_site_code, and baseline is nws_lid,
        // we don't need to look up the usgs_site_code twice, nor do we need to
        // look up the nws_lid twice.
        Map<Pair<FeatureDimension,FeatureDimension>,Set<String>> needsLookup = new HashMap<>();

        // Need an intermediate map from original Feature to new l/r/b values
        // because the same Feature may have needed two different new values,
        // which are found independently below.
        Map<Feature,String> withNewLeft = new HashMap<>( hasRightNeedsLeft.size()
                                                         + hasBaselineNeedsLeft.size() );
        Map<Feature,String> withNewRight = new HashMap<>( hasLeftNeedsRight.size()
                                                          + hasBaselineNeedsRight.size() );
        Map<Feature,String> withNewBaseline = new HashMap<>( hasLeftNeedsBaseline.size()
                                                             + hasRightNeedsBaseline.size() );

        for ( Feature feature : hasLeftNeedsRight )
        {
            Pair<FeatureDimension,FeatureDimension> leftToRight =
                    Pair.of( leftDimension, rightDimension );

            if ( !needsLookup.containsKey( leftToRight ) )
            {
                needsLookup.put( leftToRight, new HashSet<>( hasLeftNeedsRight.size() ) );
            }

            needsLookup.get( leftToRight )
                       .add( feature.getLeft() );
        }

        for ( Feature feature : hasLeftNeedsBaseline )
        {
            Pair<FeatureDimension,FeatureDimension> leftToBaseline =
                    Pair.of( leftDimension, baselineDimension );

            if ( !needsLookup.containsKey( leftToBaseline ) )
            {
                needsLookup.put( leftToBaseline, new HashSet<>( hasLeftNeedsBaseline.size() ) );
            }

            needsLookup.get( leftToBaseline )
                       .add( feature.getLeft() );
        }

        for ( Feature feature : hasRightNeedsLeft )
        {
            Pair<FeatureDimension,FeatureDimension> rightToLeft =
                    Pair.of( rightDimension, leftDimension );

            if ( !needsLookup.containsKey( rightToLeft ) )
            {
                needsLookup.put( rightToLeft, new HashSet<>( hasRightNeedsLeft.size() ) );
            }

            needsLookup.get( rightToLeft )
                       .add( feature.getRight() );
        }

        for ( Feature feature : hasRightNeedsBaseline )
        {
            Pair<FeatureDimension,FeatureDimension> rightToBaseline =
                    Pair.of( rightDimension, baselineDimension );

            if ( !needsLookup.containsKey( rightToBaseline ) )
            {
                needsLookup.put( rightToBaseline, new HashSet<>( hasRightNeedsBaseline.size() ) );
            }

            needsLookup.get( rightToBaseline)
                       .add( feature.getRight() );
        }

        for ( Feature feature : hasBaselineNeedsLeft )
        {
            Pair<FeatureDimension,FeatureDimension> baselineToLeft =
                    Pair.of( baselineDimension, leftDimension );

            if ( !needsLookup.containsKey( baselineToLeft ) )
            {
                needsLookup.put( baselineToLeft, new HashSet<>( hasBaselineNeedsLeft.size() ) );
            }

            needsLookup.get( baselineToLeft )
                       .add( feature.getBaseline() );
        }

        for ( Feature feature : hasBaselineNeedsRight )
        {
            Pair<FeatureDimension,FeatureDimension> baselineToRight =
                    Pair.of( baselineDimension, rightDimension );

            if ( !needsLookup.containsKey( baselineToRight ) )
            {
                needsLookup.put( baselineToRight, new HashSet<>( hasBaselineNeedsRight.size() ) );
            }

            needsLookup.get( baselineToRight )
                       .add( feature.getBaseline() );
        }

        LOGGER.debug( "These need lookups: {}", needsLookup );

        for ( Map.Entry<Pair<FeatureDimension,FeatureDimension>, Set<String>> nextEntry : needsLookup.entrySet() )
        {
            Pair<FeatureDimension,FeatureDimension> fromAndTo = nextEntry.getKey();
            Set<String> namesToLookUp = nextEntry.getValue();
            FeatureDimension from = fromAndTo.getKey();
            FeatureDimension to = fromAndTo.getValue();
            Map<String, String> found = WrdsFeatureService.bulkLookup( projectConfig,
                                                                       featureService,
                                                                       from,
                                                                       to,
                                                                       namesToLookUp );
            // Go through the has-this-needs-that lists
            if ( from.equals( leftDimension ) )
            {
                if ( to.equals( rightDimension ) )
                {
                    for ( Feature needed : hasLeftNeedsRight )
                    {
                        String rightName = found.get( needed.getLeft() );

                        if ( Objects.nonNull( rightName ) )
                        {
                            String foundBefore =
                                    withNewRight.put( needed, rightName );

                            if ( Objects.nonNull( foundBefore ) )
                            {
                                LOGGER.debug(
                                        "Overwrote previously-found right feature {} with {} for original {} from left.",
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

                // No if/else, because both could be true.
                if ( to.equals( baselineDimension ) )
                {
                    for ( Feature needed : hasLeftNeedsBaseline )
                    {
                        String baselineName = found.get( needed.getLeft() );

                        if ( Objects.nonNull( baselineName ) )
                        {
                            String foundBefore = withNewBaseline.put( needed, baselineName );

                            if ( Objects.nonNull( foundBefore ) )
                            {
                                LOGGER.debug(
                                        "Overwrote previously-found baseline feature {} with {} for original {} from left.",
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
            }

            // No if/else, because both could be true.
            if ( from.equals( rightDimension ) )
            {
                if ( to.equals( leftDimension ) )
                {
                    for ( Feature needed : hasRightNeedsLeft )
                    {
                        String leftName = found.get( needed.getRight() );

                        if ( Objects.nonNull( leftName ) )
                        {
                            String foundBefore =
                                    withNewLeft.put( needed, leftName );

                            if ( Objects.nonNull( foundBefore ) )
                            {
                                LOGGER.debug(
                                        "Overwrote previously-found left feature {} with {} for original {} from right.",
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

                // No if/else, because both could be true.
                if ( to.equals( baselineDimension ) )
                {
                    for ( Feature needed : hasRightNeedsBaseline )
                    {
                        String baselineName = found.get( needed.getRight() );

                        if ( Objects.nonNull( baselineName ) )
                        {
                            String foundBefore =
                                    withNewBaseline.put( needed, baselineName );

                            if ( Objects.nonNull( foundBefore ) )
                            {
                                LOGGER.debug(
                                        "Overwrote previously-found baseline feature {} with {} for original {} from right",
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
            }

            if ( from.equals( baselineDimension ) )
            {
                if ( to.equals( leftDimension ) )
                {
                    for ( Feature needed : hasBaselineNeedsLeft )
                    {
                        String leftName = found.get( needed.getBaseline() );

                        if ( Objects.nonNull( leftName ) )
                        {
                            String foundBefore =
                                    withNewLeft.put( needed, leftName );

                            if ( Objects.nonNull( foundBefore ) )
                            {
                                LOGGER.debug(
                                        "Overwrote previously-found left feature {} with {} for original {} from baseline.",
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

                if ( to.equals( rightDimension ) )
                {
                    for ( Feature needed : hasBaselineNeedsRight )
                    {
                        String rightName = found.get( needed.getBaseline() );

                        if ( Objects.nonNull( rightName ) )
                        {
                            String foundBefore =
                                    withNewRight.put( needed, rightName );

                            if ( Objects.nonNull( foundBefore ) )
                            {
                                LOGGER.debug(
                                        "Overwrote previously-found right feature {} with {} for original {} from baseline.",
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
            }
        }

        LOGGER.debug( "New left names: {}", withNewLeft );
        LOGGER.debug( "New right names: {}", withNewRight );
        LOGGER.debug( "New baseline names: {}", withNewBaseline );

        List<Feature> consolidatedFeatures = new ArrayList<>();

        for ( Feature feature : sparseFeatures )
        {
            String leftName = feature.getLeft();
            String rightName = feature.getRight();
            String baselineName = feature.getBaseline();

            String newLeftName = withNewLeft.get( feature );
            String newRightName = withNewRight.get( feature );
            String newBaselineName = withNewBaseline.get( feature );

            if ( Objects.nonNull( newLeftName ) )
            {
                leftName = newLeftName;
            }

            if ( Objects.nonNull( newRightName ) )
            {
                rightName = newRightName;
            }

            if ( Objects.nonNull( newBaselineName ) )
            {
                baselineName = newBaselineName;
            }

            Feature newFeature = new Feature( leftName, rightName, baselineName );

            consolidatedFeatures.add( newFeature );
        }

        return Collections.unmodifiableList( consolidatedFeatures );
    }

    /**
     * Get features requested within the given feature service for a specified 
     * feature group.
     * @param featureService The feature service to use with a base url, both required.
     * @param featureGroup the feature group, required.
     * @param leftDimension The left dimension discovered, required.
     * @param rightDimension The right dimension discovered, required.
     * @param baselineDimension The baseline dimension discovered, null if none.
     * @return A list of fully populated features.
     */
    private static List<Feature> getFeatureGroup( FeatureService featureService,
                                                  FeatureGroup featureGroup,
                                                  FeatureDimension leftDimension,
                                                  FeatureDimension rightDimension,
                                                  FeatureDimension baselineDimension )
    {
        Objects.requireNonNull( featureGroup );
        Objects.requireNonNull( featureService, "Cannot declare a feature service group without a feature service." );
        Objects.requireNonNull( featureService.getBaseUrl(), "Cannot declare a feature service group without a base "
                + "url." );
        Objects.requireNonNull( leftDimension );
        Objects.requireNonNull( rightDimension );
        
        URI featureServiceBaseUri = featureService.getBaseUrl();

        List<Feature> features = new ArrayList<>();

        if ( Objects.isNull( featureGroup.getType() )
             || Objects.isNull( featureGroup.getValue() )
             || featureGroup.getType()
                     .isBlank()
             || featureGroup.getValue()
                     .isBlank() )
        {
            throw new ProjectConfigException( featureGroup,
                                              "The type and value "
                                                     + "of each feature "
                                                     + "group must be "
                                                     + "specified. Instead,"
                                                     + " saw "
                                                     + featureGroup );
        }

        String path = featureServiceBaseUri.getPath();
        String fullPath = path + DELIMITER
                          + featureGroup.getType()
                          + DELIMITER
                          + featureGroup.getValue();
        URI uri = featureServiceBaseUri.resolve( fullPath )
                                       .normalize();

        if ( uri.toString()
                .length() > MAX_SAFE_URL_LENGTH )
        {
            throw new ProjectConfigException( featureGroup,
                                              "The URL to be created using "
                                                     + "this configuration would "
                                                     + "be greater than "
                                                     + MAX_SAFE_URL_LENGTH
                                                     + "characters. Please use a "
                                                     + "shorter base URL or group "
                                                     + "type or group value." );
        }

        //Read the features from either V3 or older API.
        List<WrdsLocation> wrdsLocations = WrdsFeatureService.read( uri );

        for ( WrdsLocation wrdsLocation : wrdsLocations )
        {
            String leftName;
            String rightName;
            String baselineName = null;

            if ( leftDimension.equals( FeatureDimension.NWS_LID ) )
            {
                leftName = wrdsLocation.getNwsLid();
            }
            else if ( leftDimension.equals( FeatureDimension.USGS_SITE_CODE ) )
            {
                leftName = wrdsLocation.getUsgsSiteCode();
            }
            else if ( leftDimension.equals( FeatureDimension.NWM_FEATURE_ID ) )
            {
                leftName = wrdsLocation.getNwmFeatureId();
            }
            else
            {
                throw new UnsupportedOperationException( WRES_NOT_READY_TO_LOOK_UP
                                                         + leftDimension.value() );
            }

            if ( Objects.isNull( leftName ) || leftName.isBlank() )
            {
                LOGGER.debug( "Found null or blank for left of location {}, not adding.",
                              wrdsLocation );
                continue;
            }

            if ( rightDimension.equals( FeatureDimension.NWS_LID ) )
            {
                rightName = wrdsLocation.getNwsLid();
            }
            else if ( rightDimension.equals( FeatureDimension.USGS_SITE_CODE ) )
            {
                rightName = wrdsLocation.getUsgsSiteCode();
            }
            else if ( rightDimension.equals( FeatureDimension.NWM_FEATURE_ID ) )
            {
                rightName = wrdsLocation.getNwmFeatureId();
            }
            else
            {
                throw new UnsupportedOperationException( WRES_NOT_READY_TO_LOOK_UP
                                                         + rightDimension.value() );
            }

            if ( Objects.isNull( rightName ) || rightName.isBlank() )
            {
                LOGGER.debug( "Found null or blank for right of location {}, not adding.",
                              wrdsLocation );
                continue;
            }

            if ( Objects.nonNull( baselineDimension ) )
            {
                if ( baselineDimension.equals( FeatureDimension.NWS_LID ) )
                {
                    baselineName = wrdsLocation.getNwsLid();
                }
                else if ( baselineDimension.equals( FeatureDimension.USGS_SITE_CODE ) )
                {
                    baselineName = wrdsLocation.getUsgsSiteCode();
                }
                else if ( baselineDimension.equals( FeatureDimension.NWM_FEATURE_ID ) )
                {
                    baselineName = wrdsLocation.getNwmFeatureId();
                }
                else
                {
                    throw new UnsupportedOperationException(
                                                             WRES_NOT_READY_TO_LOOK_UP
                                                             + baselineDimension.value() );
                }

                if ( Objects.isNull( baselineName ) || baselineName.isBlank() )
                {
                    LOGGER.debug( "Found null or blank for baseline of location {}, not adding.",
                                  wrdsLocation );
                    continue;
                }
            }

            Feature featureFromGroup = new Feature( leftName, rightName, baselineName );
            features.add( featureFromGroup );
        }

        return Collections.unmodifiableList( features );
    }  
    
    private static boolean isFullyDeclared( Feature potentiallySparseFeature,
                                            boolean projectHasBaseline )
    {
        boolean leftAndRightDeclared =
                Objects.nonNull( potentiallySparseFeature.getLeft() )
                && !potentiallySparseFeature.getLeft()
                                            .isBlank()
                && Objects.nonNull( potentiallySparseFeature.getRight() )
                && !potentiallySparseFeature.getRight()
                                            .isBlank();

        if ( !projectHasBaseline )
        {
            return leftAndRightDeclared;
        }

        return leftAndRightDeclared
               && Objects.nonNull( potentiallySparseFeature.getBaseline() )
               && !potentiallySparseFeature.getBaseline()
                                           .isBlank();
    }


    /**
     * Determine the location authority of the given datasource.
     * @param dataSourceConfig The data source declaration within projectConfig.
     * @return The WrdsLocationDimension or null if custom dimension.
     * @throws ProjectConfigException On inconsistent or incomplete declaration.
     * @throws NullPointerException When any argument is null.
     * @throws UnsupportedOperationException When code cannot handle something.
     */

    private static FeatureDimension determineDimension( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );

        FeatureDimension featureDimension = ConfigHelper.getConcreteFeatureDimension(dataSourceConfig);

        if ( featureDimension == null )
        {
            throw new ProjectConfigException( dataSourceConfig,
                                              "Unable to determine what "
                                              + "geographic feature dimension "
                                              + "to use for source, cannot ask "
                                              + "service without a feature "
                                              + "dimension inferred from source"
                                              + " interface (e.g. WRES knows "
                                              + " interface usgs_nwis uses "
                                              + " usgs_site_code dimension) or "
                                              + "explicitly declared in the "
                                              + "'featureDimension' attribute "
                                              + "of left/right/baseline. Valid "
                                              + "values include: "
                                              + FeatureFinder.getValidFeatureDimensionValues() );

        }

        return featureDimension;
    }

    /**
     * Convenience method for printing error messages. Displays all valid values
     * of the featureDimension.
     * @return The
     */
    private static String getValidFeatureDimensionValues()
    {
        StringJoiner joiner = new StringJoiner( "', '", "'", "'" );

        for ( FeatureDimension dimension : FeatureDimension.values() )
        {
            joiner.add( dimension.value() );
        }

        return joiner.toString();
    }


}
