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
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.geography.wrds.WrdsLocationRootDocument;
import wres.io.reading.PreIngestException;
import wres.io.utilities.NoDataException;

/**
 * When sparse features are declared, this class helps build the features.
 *
 * In other words, if a declaration says "left feature is ABC" but leaves the
 * right feature out, this class can help figure out the right feature name
 * and return objects that can be used for the remainder of the evaluation.
 */
public class FeatureFinder
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureFinder.class );

    private static final String RESPONSE_FROM_WRDS_AT = "Response from WRDS at ";
    private static final String HAD_NULL_OR_BLANK = " had null or blank ";

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
     * @throws NullPointerException When projectDeclaration or required contents
     * is null. Required: inputs, left, right, pair, feature.
     */

    public static ProjectConfig fillFeatures( ProjectConfig projectDeclaration )
    {
        Objects.requireNonNull( projectDeclaration );
        boolean hasBaseline = Objects.nonNull( projectDeclaration.getInputs()
                                                                 .getBaseline() );

        // In many cases, no need to declare features, such as evaluations where
        // all the feature names are identical in sources on both sides or in
        // gridded evaluations.
        if ( projectDeclaration.getPair()
                               .getFeature()
                               .isEmpty() )
        {
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
             && leftNames.size() == rightNames.size() )
        {
            LOGGER.debug( "No baseline, left and right feature count {}",
                          "matches original list. No need to fill features." );
            // There is no baseline. If the three name counts all match, OK.
            return projectDeclaration;
        }
        else if ( hasBaseline
                  && leftNames.size() == featureList.size()
                  && leftNames.size() == rightNames.size()
                  && leftNames.size() == baselineNames.size() )
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
                                                               originalPairDeclaration.getMask(),
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
     * @param featureServiceBaseUri The URI prefix to the location service.
     * @param sparseFeatures The declared/sparse features.
     * @param projectHasBaseline Whether the project declaration has a baseline.
     * @return A new list of features based on the given args.
     */

    private static List<Feature> fillFeatures( ProjectConfig projectConfig,
                                               URI featureServiceBaseUri,
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
            Map<String,String> found = FeatureFinder.bulkLookup( featureServiceBaseUri,
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
     * @param from The known feature dimension, in which "featureNames" exist.
     * @param to The unknown feature dimension, the dimension to search in.
     * @param featureNames The names in the "from" dimension to look for in "to"
     * @return The Set of name pairs: "from" as key, "to" as value.
     */

    private static Map<String,String> bulkLookup( URI featureServiceBaseUri,
                                                  FeatureDimension from,
                                                  FeatureDimension to,
                                                  Set<String> featureNames )
    {
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
                                          + countOfLocations );
        }

        for ( WrdsLocation location : featureData.getLocations() )
        {
            String original;
            String found;

            if ( from.equals( FeatureDimension.NWS_LID ) )
            {
                String nwsLid = location.getNwsLid();

                if ( Objects.isNull( nwsLid ) || nwsLid.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + "NWS LID." );
                }

                original = nwsLid;
            }
            else if ( from.equals( FeatureDimension.USGS_SITE_CODE ) )
            {
                String usgsSiteCode = location.getUsgsSiteCode();

                if ( Objects.isNull( usgsSiteCode )
                     || usgsSiteCode.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + " USGS Site Code." );
                }

                original = usgsSiteCode;
            }
            else if ( from.equals( FeatureDimension.NWM_FEATURE_ID ) )
            {
                String nwmFeatureId = location.getNwmFeatureId();

                if ( Objects.isNull( nwmFeatureId )
                     || nwmFeatureId.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + " NWM Feature ID." );
                }

                original = nwmFeatureId;
            }
            else
            {
                throw new UnsupportedOperationException(
                        "Unknown feature location authority" );
            }

            if ( to.equals( FeatureDimension.NWS_LID ) )
            {
                String nwsLid = location.getNwsLid();

                if ( Objects.isNull( nwsLid ) || nwsLid.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + "NWS LID." );
                }

                found = nwsLid;
            }
            else if ( to.equals( FeatureDimension.USGS_SITE_CODE ) )
            {
                String usgsSiteCode = location.getUsgsSiteCode();

                if ( Objects.isNull( usgsSiteCode )
                     || usgsSiteCode.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + " USGS Site Code." );
                }

                found = usgsSiteCode;
            }
            else if ( to.equals( FeatureDimension.NWM_FEATURE_ID ) )
            {
                String nwmFeatureId = location.getNwmFeatureId();

                if ( Objects.isNull( nwmFeatureId )
                     || nwmFeatureId.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + " NWM Feature ID." );
                }

                found = nwmFeatureId;
            }
            else
            {
                throw new UnsupportedOperationException(
                        "Unknown feature location authority" );
            }

            locations.put( original, found );
        }

        LOGGER.debug( "For from={} and to={}, found these: {}",
                      from, to, locations );
        return Collections.unmodifiableMap( locations );
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
