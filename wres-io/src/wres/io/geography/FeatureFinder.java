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
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.FeatureDimension;
import wres.config.generated.FeatureGroup;
import wres.config.generated.FeatureService;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.geography.wrds.WrdsLocationRootDocument;
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
 */

public class FeatureFinder
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureFinder.class );

    private static final String RESPONSE_FROM_WRDS_AT = "Response from WRDS at ";
    private static final String HAD_NULL_OR_BLANK = " had null or blank ";
    private static final String EXPLANATION_OF_WHY_AND_WHAT_TO_DO =
           "By declaring a feature, WRES interprets it as an intent to use that"
           + " feature in the evaluation. If the corresponding feature cannot "
           + "be found or has no correlation from the featureService, WRES "
           + "prefers to inform you that the declared evaluation including that"
           + " feature cannot be started. Options to resolve include any of: "
           + "investigate the URL above in a web browser, report to the "
           + "geographic feature service team the service issue or the data "
           + "issue (e.g. expected correlation), omit this feature from the "
           + "declaration, include a complete feature correlation declaration, "
           + "or contact the WRES team for help.";
    private static final int MAX_SAFE_URL_LENGTH = 2000;

    private FeatureFinder()
    {
        // Static helper functions, no construction.
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
        if ( projectDeclaration.getPair()
                               .getFeature()
                               .isEmpty()
             && !requiresFeatureRequests )
        {
            LOGGER.debug( "No need to fill features: empty features and no requests required." );
            return projectDeclaration;
        }

        Set<String> leftNames = ConfigHelper.getFeatureNamesForSource( projectDeclaration,
                                                                       projectDeclaration.getInputs()
                                                                                         .getLeft() );
        Set<String> rightNames = ConfigHelper.getFeatureNamesForSource( projectDeclaration,
                                                                        projectDeclaration.getInputs()
                                                                                          .getRight() );
        Set<String> baselineNames = ConfigHelper.getFeatureNamesForSource( projectDeclaration,
                                                                           projectDeclaration.getInputs()
                                                                                             .getBaseline() );
        List<Feature> featureList = projectDeclaration.getPair()
                                                      .getFeature();

        // Common case: features are already declared fully, do no more!
        if ( !hasBaseline
             && leftNames.size() == featureList.size()
             && leftNames.size() == rightNames.size()
             && !requiresFeatureRequests )
        {
            LOGGER.debug( "No baseline, left and right feature count {}",
                          "matches original list. No need to fill features." );
            // There is no baseline. If the three name counts all match, OK.
            return projectDeclaration;
        }
        else if ( hasBaseline
                  && leftNames.size() == featureList.size()
                  && leftNames.size() == rightNames.size()
                  && leftNames.size() == baselineNames.size()
                  && !requiresFeatureRequests )
        {
            LOGGER.debug( "Baseline, left, and right feature count {}",
                          "matches original list. No need to fill features." );
            // There is a baseline. If the four name counts all match, OK.
            return projectDeclaration;
        }

        List<Feature> filledFeatures =
                FeatureFinder.fillFeatures( projectDeclaration,
                                            projectDeclaration.getPair()
                                                              .getFeatureService(),
                                            projectDeclaration.getPair()
                                                              .getFeature(),
                                            hasBaseline );

        if ( filledFeatures.isEmpty() )
        {
            throw new PreIngestException( "No geographic features found to evaluate." );
        }

        PairConfig originalPairDeclaration = projectDeclaration.getPair();

        PairConfig featurefulPairDeclaration = new PairConfig( originalPairDeclaration.getUnit(),
                                                               originalPairDeclaration.getFeatureService(),
                                                               filledFeatures,
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
     * Given a base uri and sparse features, generate fully-specified features,
     * using the service to correlate features that are not fully specified.
     *
     * @param projectConfig The project declaration.
     * @param featureService The element containing location service details.
     * @param sparseFeatures The declared/sparse features.
     * @param projectHasBaseline Whether the project declaration has a baseline.
     * @return A new list of features based on the given args.
     */

    private static List<Feature> fillFeatures( ProjectConfig projectConfig,
                                               FeatureService featureService,
                                               List<Feature> sparseFeatures,
                                               boolean projectHasBaseline )
    {
        List<Feature> hasLeftNeedsRight = new ArrayList<>();
        List<Feature> hasLeftNeedsBaseline = new ArrayList<>();
        List<Feature> hasRightNeedsLeft = new ArrayList<>();
        List<Feature> hasRightNeedsBaseline = new ArrayList<>();
        List<Feature> hasBaselineNeedsLeft = new ArrayList<>();
        List<Feature> hasBaselineNeedsRight = new ArrayList<>();

        // Figure out if the same authority is used on multiple sides. If so,
        // consolidate lists.
        FeatureDimension leftDimension =
                FeatureFinder.determineDimension( projectConfig,
                                                  projectConfig.getInputs()
                                                               .getLeft() );
        FeatureDimension rightDimension =
                FeatureFinder.determineDimension( projectConfig,
                                                  projectConfig.getInputs()
                                                               .getRight() );
        FeatureDimension baselineDimension = null;

        if ( projectHasBaseline )
        {
            baselineDimension = FeatureFinder.determineDimension( projectConfig,
                                                                  projectConfig.getInputs()
                                                                               .getBaseline() );
        }


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

        for ( Pair<FeatureDimension,FeatureDimension> fromAndTo : needsLookup.keySet() )
        {
            Set<String> namesToLookUp = needsLookup.get( fromAndTo );
            FeatureDimension from = fromAndTo.getKey();
            FeatureDimension to = fromAndTo.getValue();
            Map<String,String> found = FeatureFinder.bulkLookup( projectConfig,
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

        // Add in feature groups requested
        List<Feature> featuresFromGroups =
                FeatureFinder.getFeatureGroups( featureService,
                                                leftDimension,
                                                rightDimension,
                                                baselineDimension );
        consolidatedFeatures.addAll( featuresFromGroups );
        return Collections.unmodifiableList( consolidatedFeatures );
    }

    /**
     * Get features requested within the given feature service, or empty list
     * if none are requested.
     * @param featureService The feature service to use, optional.
     * @param leftDimension The left dimension discovered, required.
     * @param rightDimension The right dimension discovered, required.
     * @param baselineDimension The baseline dimension discovered, null if none.
     * @return A list of fully populated features or empty list.
     */
    private static List<Feature> getFeatureGroups( FeatureService featureService,
                                                   FeatureDimension leftDimension,
                                                   FeatureDimension rightDimension,
                                                   FeatureDimension baselineDimension )
    {
        if ( Objects.isNull( featureService )
             || Objects.isNull( featureService.getGroup() )
             || featureService.getGroup()
                              .isEmpty() )
        {
            LOGGER.debug( "No feature groups found declared, returning empty." );
            return Collections.emptyList();
        }

        URI featureServiceBaseUri = featureService.getBaseUrl();

        List<Feature> features = new ArrayList<>();

        for ( FeatureGroup group : featureService.getGroup() )
        {
            String path = featureServiceBaseUri.getPath();
            String fullPath = path + "/" + group.getType()
                              + "/" + group.getValue();
            URI uri = featureServiceBaseUri.resolve( fullPath )
                                           .normalize();

            if ( uri.toString()
                    .length() > MAX_SAFE_URL_LENGTH )
            {
                throw new ProjectConfigException( group,
                                                  "The URL to be created using "
                                                  + "this configuration would "
                                                  + "be greater than "
                                                  + MAX_SAFE_URL_LENGTH
                                                  + "characters. Please use a "
                                                  + "shorter base URL or group "
                                                  + "type or group value." );
            }

            WrdsLocationRootDocument featureData = WrdsLocationReader.read( uri );

            for ( WrdsLocation wrdsLocation : featureData.getLocations() )
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
                    throw new UnsupportedOperationException( "WRES not ready to look up "
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
                    throw new UnsupportedOperationException( "WRES not ready to look up "
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
                                "WRES not ready to look up "
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
     * @param projectConfig The project declaration to use.
     * @param dataSourceConfig The data source declaration within projectConfig.
     * @return The WrdsLocationDimension or null if custom dimension.
     * @throws ProjectConfigException On inconsistent or incomplete declaration.
     * @throws NullPointerException When any argument is null.
     * @throws UnsupportedOperationException When code cannot handle something.
     */

    private static FeatureDimension determineDimension( ProjectConfig projectConfig,
                                                        DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSourceConfig );

        SortedSet<FeatureDimension> dimensions = new TreeSet<>();
        FeatureDimension featureDimension = dataSourceConfig.getFeatureDimension();

        if ( Objects.nonNull( featureDimension ) )
        {
            dimensions.add( featureDimension );
        }

        for ( DataSourceConfig.Source source : dataSourceConfig.getSource() )
        {
            if ( Objects.isNull( featureDimension )
                 && Objects.isNull( source.getInterface() ) )
            {
                throw new ProjectConfigException( source,
                                                  "Either the geographic "
                                                  + "feature dimension ('"
                                                  + "featureDimension' "
                                                  + "attribute) of the dataset "
                                                  + "or the interface on each "
                                                  + "source must be declared "
                                                  + "to determine how to ask "
                                                  + "for location correlations."
                                                  + " Valid values for "
                                                  + "featureDimension include: "
                                                  + FeatureFinder.getValidFeatureDimensionValues() );
            }

            if ( Objects.nonNull( source.getInterface() ) )
            {
                if ( source.getInterface()
                           .equals( InterfaceShortHand.USGS_NWIS ) )
                {
                    dimensions.add( FeatureDimension.USGS_SITE_CODE );
                }
                else if ( source.getInterface()
                                .equals( InterfaceShortHand.WRDS_AHPS ) )
                {
                    dimensions.add( FeatureDimension.NWS_LID );
                }
                else if ( source.getInterface()
                                .equals( InterfaceShortHand.WRDS_NWM ) )
                {
                    dimensions.add( FeatureDimension.NWM_FEATURE_ID );
                }
                else if ( source.getInterface()
                                .toString()
                                .toLowerCase()
                                .startsWith( "nwm_" ) )
                {
                    dimensions.add( FeatureDimension.NWM_FEATURE_ID );
                }
                else
                {
                    throw new UnsupportedOperationException(
                            "Unable to determine what geographic identifiers to use for source "
                            + source
                            + " having interface "
                            + source.getInterface() );
                }
            }
        }

        if ( dimensions.isEmpty() )
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
        else if ( dimensions.size() > 1 )
        {
            throw new ProjectConfigException( dataSourceConfig,
                                              "Cannot have more than one "
                                              + "geographic feature dimension "
                                              + "for a given dataset (e.g. all "
                                              + "sources on the left could use "
                                              + "usgs_site_code but not a mix "
                                              + "of usgs_site_code and nws_lid."
                                              + " Found these: "
                                              + dimensions );
        }

        return dimensions.first();
    }


    /**
     * Given a dimension "from" and dimension "to", look up the set of features.
     * @param projectConfig The declaration to use when printing error message.
     * @param featureService The featureService element, optional unless lookup
     *                       ends up being required.
     * @param from The known feature dimension, in which "featureNames" exist.
     * @param to The unknown feature dimension, the dimension to search in.
     * @param featureNames The names in the "from" dimension to look for in "to"
     * @return The Set of name pairs: "from" as key, "to" as value.
     * @throws ProjectConfigException When a feature service was needed but null
     * @throws PreIngestException When the count of features in response differs
     *                            from the count of feature names requested, or
     *                            when the requested "to" was not found in the
     *                            response.
     * @throws UnsupportedOperationException When unknown "from" or "to" given.
     * @throws NullPointerException When projectConfig or featureNames is null.
     */

    private static Map<String,String> bulkLookup( ProjectConfig projectConfig,
                                                  FeatureService featureService,
                                                  FeatureDimension from,
                                                  FeatureDimension to,
                                                  Set<String> featureNames )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( featureNames );

        Map<String,String> locations = new HashMap<>( featureNames.size() );

        if ( from.equals( to ) )
        {
            // In the case where the original dimension is same as the new
            // dimension, no lookup is needed. Fill out directly.
            for ( String feature : featureNames )
            {
                locations.put( feature, feature );
            }

            LOGGER.debug( "Did not ask a service because from={}, to={}, returned {}",
                          from, to, locations );
            return Collections.unmodifiableMap( locations );
        }

        if ( from.equals( FeatureDimension.CUSTOM ) )
        {
            // We will have no luck asking for features based on an unknown
            // dimension. But this call will happen sometimes, so return empty.
            LOGGER.debug( "Did not ask a service because from={}", from );
            return Collections.emptyMap();
        }

        if ( Objects.isNull( featureService ) )
        {
            throw new ProjectConfigException( projectConfig.getPair(),
                                              "Attempted to look up features "
                                              + "with " + from.value()
                                              + " and missing " + to.value()
                                              + ", but could not because the "
                                              + "'featureService' declaration "
                                              + "was either missing. "
                                              + "Add a <featureService><baseUrl>..."
                                              + "</baseUrl></featureService> with "
                                              + "the non-varying part of the "
                                              + "URL of the feature service to "
                                              + "have WRES ask it for features." );
        }

        if ( Objects.isNull( featureService.getBaseUrl() ) )
        {
            throw new ProjectConfigException( featureService,
                                              "Attempted to look up features "
                                              + "with " + from.value()
                                              + " and missing " + to.value()
                                              + ", but could not because the "
                                              + "'featureService' declaration "
                                              + "was missing a 'baseUrl' tag. "
                                              + "Add a <baseUrl>...</baseUrl> with "
                                              + "the non-varying part of the "
                                              + "URL of the feature service ("
                                              + "inside the <featureService>) "
                                              + "to have WRES ask it for"
                                              + " features." );
        }

        URI featureServiceBaseUri = featureService.getBaseUrl();
        Set<String> batchOfFeatureNames = new HashSet<>();

        // Track how large the URL gets. Base uri, from, plus 2 for the slashes.
        int baseLength = 2 + featureServiceBaseUri.toString()
                                                  .length()
                         + from.toString()
                               .length();
        int totalLength = baseLength;

        for ( String featureName : featureNames )
        {
            int addedLength = featureName.length() + 1;

            if ( totalLength + addedLength <= MAX_SAFE_URL_LENGTH )
            {
                LOGGER.trace( "totalLength {} + addedLength {} <= MAX_SAFE_URL_LENGTH {}",
                              totalLength, addedLength, MAX_SAFE_URL_LENGTH );
                batchOfFeatureNames.add( featureName );
                totalLength += addedLength;
                continue;
            }

            LOGGER.debug( "One more feature name would be unsafe length URL: {}",
                          batchOfFeatureNames );
            // At this point there are more features to go, but we hit the safe
            // URL length limit.
            Map<String,String> batchOfResults =
                    FeatureFinder.getBatchOfFeatures( from,
                                                      to,
                                                      featureServiceBaseUri,
                                                      batchOfFeatureNames );
            locations.putAll( batchOfResults );
            batchOfFeatureNames = new HashSet<>();
            totalLength = baseLength;
        }

        LOGGER.debug( "Last of the feature names to request: {}",
                      batchOfFeatureNames );
        // Get the remaining features.
        Map<String,String> batchOfResults =
                FeatureFinder.getBatchOfFeatures( from,
                                                  to,
                                                  featureServiceBaseUri,
                                                  batchOfFeatureNames );
        locations.putAll( batchOfResults );

        LOGGER.debug( "For from={} and to={}, found these: {}",
                      from, to, locations );
        return Collections.unmodifiableMap( locations );
    }


    /**
     * Make a request for the complete given featureNames, already vetted to be
     * less than the maximum safe URL length when to, featureServiceBaseUri,
     * and featureNames are joined together with slashes and commas.
     * @param from The known feature dimension, in which "featureNames" exist.
     * @param to The unknown feature dimension, the dimension to search in.
     * @param featureServiceBaseUri The base URI from which to build a full URI.
     * @param featureNames The names in the "from" dimension to look for in "to"
     * @return The Set of name pairs: "from" as key, "to" as value.
     * @throws PreIngestException When the count of features in response differs
     *                            from the count of feature names requested, or
     *                            when the requested "to" was not found in the
     *                            response.
     * @throws NullPointerException When any argument is null.
     * @throws UnsupportedOperationException When unknown "from" or "to" given.
     * @throws IllegalArgumentException When any arg is null.
     */
    private static Map<String,String> getBatchOfFeatures( FeatureDimension from,
                                                          FeatureDimension to,
                                                          URI featureServiceBaseUri,
                                                          Set<String> featureNames )
    {
        Objects.requireNonNull( from );
        Objects.requireNonNull( to );
        Objects.requireNonNull( featureServiceBaseUri );
        Objects.requireNonNull( featureNames );

        if ( featureNames.isEmpty() )
        {
            throw new IllegalArgumentException( "Encountered an empty batch of feature names." );
        }

        Map<String,String> batchOfLocations = new HashMap<>( featureNames.size() );
        StringJoiner joiner = new StringJoiner( "," );
        featureNames.forEach( joiner::add );
        String commaDelimitedFeatures = joiner.toString();

        // Add to request set. (Request directly for now)
        String path = featureServiceBaseUri.getPath();
        String fullPath = path + "/" + from.toString()
                                           .toLowerCase()
                          + "/" + commaDelimitedFeatures;
        URI uri = featureServiceBaseUri.resolve( fullPath )
                                       .normalize();

        WrdsLocationRootDocument featureData = WrdsLocationReader.read( uri );
        int countOfLocations = featureData.getLocations()
                                          .size();

        if ( countOfLocations != featureNames.size() )
        {
            throw new PreIngestException( "Response from WRDS at " + uri
                                          + " did not include exactly "
                                          + featureNames.size() + " locations, "
                                          + " but had "
                                          + countOfLocations
                                          + ". "
                                          + EXPLANATION_OF_WHY_AND_WHAT_TO_DO );
        }

        List<WrdsLocation> fromWasNullOrBlank = new ArrayList<>( 2 );
        List<WrdsLocation> toWasNullOrBlank = new ArrayList<>( 2 );

        for ( WrdsLocation location : featureData.getLocations() )
        {
            String original;
            String found;

            if ( from.equals( FeatureDimension.NWS_LID ) )
            {
                String nwsLid = location.getNwsLid();

                if ( Objects.isNull( nwsLid ) || nwsLid.isBlank() )
                {
                    fromWasNullOrBlank.add( location );
                }

                original = nwsLid;
            }
            else if ( from.equals( FeatureDimension.USGS_SITE_CODE ) )
            {
                String usgsSiteCode = location.getUsgsSiteCode();

                if ( Objects.isNull( usgsSiteCode )
                     || usgsSiteCode.isBlank() )
                {
                    fromWasNullOrBlank.add( location );
                }

                original = usgsSiteCode;
            }
            else if ( from.equals( FeatureDimension.NWM_FEATURE_ID ) )
            {
                String nwmFeatureId = location.getNwmFeatureId();

                if ( Objects.isNull( nwmFeatureId )
                     || nwmFeatureId.isBlank() )
                {
                    fromWasNullOrBlank.add( location );
                }

                original = nwmFeatureId;
            }
            else
            {
                throw new UnsupportedOperationException( "Unknown geographic feature dimension "
                                                         + from );
            }

            if ( to.equals( FeatureDimension.NWS_LID ) )
            {
                String nwsLid = location.getNwsLid();

                if ( Objects.isNull( nwsLid ) || nwsLid.isBlank() )
                {
                    toWasNullOrBlank.add( location );
                }

                found = nwsLid;
            }
            else if ( to.equals( FeatureDimension.USGS_SITE_CODE ) )
            {
                String usgsSiteCode = location.getUsgsSiteCode();

                if ( Objects.isNull( usgsSiteCode )
                     || usgsSiteCode.isBlank() )
                {
                    toWasNullOrBlank.add( location );
                }

                found = usgsSiteCode;
            }
            else if ( to.equals( FeatureDimension.NWM_FEATURE_ID ) )
            {
                String nwmFeatureId = location.getNwmFeatureId();

                if ( Objects.isNull( nwmFeatureId )
                     || nwmFeatureId.isBlank() )
                {
                    toWasNullOrBlank.add( location );
                }

                found = nwmFeatureId;
            }
            else
            {
                throw new UnsupportedOperationException( "Unknown geographic feature dimension"
                                                         + to );
            }

            batchOfLocations.put( original, found );
        }

        if ( !fromWasNullOrBlank.isEmpty()
             || !toWasNullOrBlank.isEmpty() )
        {
            throw new PreIngestException( "From the response at " + uri
                                          + " the following were missing "
                                          + from.value() + " (from) values: "
                                          + fromWasNullOrBlank + " and/or "
                                          + "the following were missing "
                                          + to.value() + " (to) values: "
                                          + toWasNullOrBlank
                                          + ". "
                                          + EXPLANATION_OF_WHY_AND_WHAT_TO_DO );
        }

        return Collections.unmodifiableMap( batchOfLocations );
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
