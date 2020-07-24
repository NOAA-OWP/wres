package wres.io.geography;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
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
    private static final String NWM_DEFAULT_VERSION = "2.0";

    private enum WrdsLocationAuthority
    {
        NWS_LID,
        USGS_SITE_CODE,
        NWM_FEATURE_ID
    }

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

        // Go through the features, finding what was declared and not.
        for ( Feature feature : sparseFeatures )
        {
            // Simple case: all elements are present.
            if ( FeatureFinder.isFullyDeclared( feature, projectHasBaseline ) )
            {
                continue;
            }

            String leftName = feature.getLeft();
            String rightName = feature.getRight();
            String baselineName = feature.getBaseline();

            boolean leftPresent = Objects.nonNull( leftName )
                                  && !leftName.isBlank();
            boolean rightPresent = Objects.nonNull( rightName )
                                   && !rightName.isBlank();
            boolean baselinePresent = Objects.nonNull( baselineName )
                                      && !baselineName.isBlank();

            boolean leftFoundByRight = false;
            boolean rightFoundByLeft = false;
            boolean baselineFoundByLeft = false;

            if ( leftPresent && !rightPresent )
            {
                hasLeftNeedsRight.add( feature );
                rightFoundByLeft = true;
            }

            if ( projectHasBaseline && leftPresent && !baselinePresent )
            {
                hasLeftNeedsBaseline.add( feature );
                baselineFoundByLeft = true;
            }

            if ( rightPresent && !leftPresent )
            {
                hasRightNeedsLeft.add( feature );
                leftFoundByRight = true;
            }

            if ( projectHasBaseline && rightPresent && !baselinePresent && !baselineFoundByLeft )
            {
                hasRightNeedsBaseline.add( feature );
            }

            if ( projectHasBaseline && baselinePresent && !leftPresent && !leftFoundByRight )
            {
                hasBaselineNeedsLeft.add( feature );
            }

            if ( projectHasBaseline && baselinePresent && !rightPresent && !rightFoundByLeft )
            {
                hasBaselineNeedsRight.add( feature );
            }
        }


        // Figure out if the same authority is used on multiple sides. If so,
        // consolidate lists.
        WrdsLocationAuthority
                leftAuthority = FeatureFinder.determineAuthority( projectConfig,
                                                                  projectConfig.getInputs()
                                                                               .getLeft() );
        WrdsLocationAuthority
                rightAuthority = FeatureFinder.determineAuthority( projectConfig,
                                                                   projectConfig.getInputs()
                                                                                .getRight() );
        WrdsLocationAuthority baselineAuthority = null;

        if ( projectHasBaseline )
        {
            baselineAuthority = FeatureFinder.determineAuthority( projectConfig,
                                                                  projectConfig.getInputs()
                                                                               .getBaseline() );
        }


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
            if ( leftAuthority.equals( rightAuthority ) )
            {
                // No need to ask service for hasLeftNeedsRight.
                // No need to ask service for hasRightNeedsLeft.
                withNewRight.put( feature, feature.getLeft() );
            }
            else
            {
                String foundName = FeatureFinder.findFeatureName( featureServiceBaseUri,
                                                                  leftAuthority,
                                                                  feature.getLeft(),
                                                                  rightAuthority );
                withNewRight.put( feature, foundName );
            }
        }

        for ( Feature feature : hasLeftNeedsBaseline )
        {
            if ( projectHasBaseline
                 && leftAuthority.equals( baselineAuthority ) )
            {
                // No need to ask service for hasLeftNeedsBaseline.
                // No need to ask service for hasBaselineNeedsLeft.
                withNewBaseline.put( feature, feature.getLeft() );
            }
            else
            {
                String foundName = FeatureFinder.findFeatureName( featureServiceBaseUri,
                                                                  leftAuthority,
                                                                  feature.getLeft(),
                                                                  baselineAuthority );
                withNewBaseline.put( feature, foundName );
            }
        }

        for ( Feature feature : hasRightNeedsLeft )
        {
            if ( leftAuthority.equals( rightAuthority ) )
            {
                // No need to ask service for hasLeftNeedsRight.
                // No need to ask service for hasRightNeedsLeft.
                withNewLeft.put( feature, feature.getRight() );
            }
            else
            {
                String foundName = FeatureFinder.findFeatureName( featureServiceBaseUri,
                                                                  rightAuthority,
                                                                  feature.getRight(),
                                                                  leftAuthority );
                withNewLeft.put( feature, foundName );
            }
        }

        for ( Feature feature : hasRightNeedsBaseline )
        {
            if ( projectHasBaseline
                 && rightAuthority.equals( baselineAuthority ) )
            {
                // No need to ask service for hasRightNeedsBaseline.
                // No need to ask service for hasBaselineNeedsRight.
                withNewBaseline.put( feature, feature.getRight() );
            }
            else
            {
                String foundName = FeatureFinder.findFeatureName( featureServiceBaseUri,
                                                                  rightAuthority,
                                                                  feature.getRight(),
                                                                  baselineAuthority );
                withNewBaseline.put( feature, foundName );
            }
        }

        for ( Feature feature : hasBaselineNeedsLeft )
        {
            if ( projectHasBaseline
                 && leftAuthority.equals( baselineAuthority ) )
            {
                // No need to ask service for hasLeftNeedsBaseline.
                // No need to ask service for hasBaselineNeedsLeft.
                withNewLeft.put( feature, feature.getBaseline() );
            }
            else
            {
                String foundName = FeatureFinder.findFeatureName( featureServiceBaseUri,
                                                                  baselineAuthority,
                                                                  feature.getBaseline(),
                                                                  leftAuthority );
                withNewLeft.put( feature, foundName );
            }
        }

        for ( Feature feature : hasBaselineNeedsRight )
        {
            if ( projectHasBaseline
                 && rightAuthority.equals( baselineAuthority ) )
            {
                // No need to ask service for hasRightNeedsBaseline.
                // No need to ask service for hasBaselineNeedsRight.
                withNewRight.put( feature, feature.getBaseline() );
            }
            else
            {
                String foundName = FeatureFinder.findFeatureName( featureServiceBaseUri,
                                                                  baselineAuthority,
                                                                  feature.getBaseline(),
                                                                  rightAuthority );
                withNewRight.put( feature, foundName );
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
     * @throws ProjectConfigException On inconsistent or incomplete declaration.
     * @throws NullPointerException When any argument is null.
     */

    private static WrdsLocationAuthority determineAuthority( ProjectConfig projectConfig,
                                                             DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSourceConfig );

        SortedSet<WrdsLocationAuthority> authorities = new TreeSet<>();

        for ( DataSourceConfig.Source source : dataSourceConfig.getSource() )
        {
            if ( Objects.isNull( source.getInterface() ) )
            {
                throw new ProjectConfigException( source,
                                                  "The interface on sources must be declared to determine the authority to ask for location correlations." );
            }
            if ( source.getInterface()
                       .equals( InterfaceShortHand.USGS_NWIS ) )
            {
                authorities.add( WrdsLocationAuthority.USGS_SITE_CODE );
            }
            else if ( source.getInterface()
                            .equals( InterfaceShortHand.WRDS_AHPS ) )
            {
                authorities.add( WrdsLocationAuthority.NWS_LID );
            }
            else if ( source.getInterface()
                            .equals( InterfaceShortHand.WRDS_NWM ) )
            {
                authorities.add( WrdsLocationAuthority.NWM_FEATURE_ID );
            }
            else if ( source.getInterface()
                            .toString()
                            .toLowerCase()
                            .startsWith( "nwm_" ) )
            {
                authorities.add( WrdsLocationAuthority.NWM_FEATURE_ID );
            }
            else
            {
                throw new UnsupportedOperationException( "Unable to determine what geographic identifier authority to use for source "
                                                         + source
                                                         + " having interface "
                                                         + source.getInterface() );
            }
        }

        if ( authorities.isEmpty() )
        {
            throw new ProjectConfigException( dataSourceConfig,
                                              "Unable to determine what geographic authority to use for source, cannot ask service without this data." );
        }
        else if ( authorities.size() > 1 )
        {
            throw new ProjectConfigException( dataSourceConfig,
                                              "Cannot have more than one authority for a given dataset (e.g. all sources on the left could use USGS site codes but not a mix of USGS site codes and NWS lids. Found these: "
                                              + authorities );
        }

        return authorities.first();
    }


    /**
     *
     * @param featureServiceBaseUri The base uri to use to get names.
     * @param from The authority of the locationNameFrom to use.
     * @param locationName The location name to look up.
     * @param to The authority to which to correlate the given locationName
     * @return The geographic feature name in the "to" authority.
     * @throws PreIngestException When correlation data not found.
     */

    private static String findFeatureName( URI featureServiceBaseUri,
                                           WrdsLocationAuthority from,
                                           String locationName,
                                           WrdsLocationAuthority to )
    {
        // Add to request set. (Request directly for now)
        String path = featureServiceBaseUri.getPath();
        String fullPath = path + "/" + from.toString()
                                           .toLowerCase()
                          + "/" + locationName;
        URI uri = featureServiceBaseUri.resolve( fullPath )
                                       .normalize();

        WrdsLocationRootDocument featureData = WrdsLocationReader.read( uri );
        int countOfLocations = featureData.getLocations()
                                          .size();

        if ( countOfLocations != 1 )
        {
            throw new PreIngestException( "Response from WRDS at " + uri
                                          + " did not include exactly "
                                          + "one location, had "
                                          + countOfLocations );
        }

        for ( WrdsLocation location : featureData.getLocations() )
        {
            if ( to.equals( WrdsLocationAuthority.NWS_LID ) )
            {
                String nwsLid = location.getNwsLid();

                if ( Objects.isNull( nwsLid ) || nwsLid.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + "NWS LID." );
                }

                return nwsLid;
            }
            else if ( to.equals( WrdsLocationAuthority.USGS_SITE_CODE ) )
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

                return usgsSiteCode;
            }
            else if ( to.equals( WrdsLocationAuthority.NWM_FEATURE_ID ) )
            {
                if ( !location.getNwmFeatures()
                              .containsKey( NWM_DEFAULT_VERSION ) )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + " had no "
                                                  + NWM_DEFAULT_VERSION
                                                  + " feature data." );
                }

                String nwmFeatureId = location.getNwmFeatures()
                                              .get( NWM_DEFAULT_VERSION )
                                              .getNwmFeatureId();

                if ( Objects.isNull( nwmFeatureId )
                     || nwmFeatureId.isBlank() )
                {
                    throw new PreIngestException( RESPONSE_FROM_WRDS_AT
                                                  + uri
                                                  + HAD_NULL_OR_BLANK
                                                  + " USGS Site Code." );
                }

                return nwmFeatureId;
            }
            else
            {
                throw new UnsupportedOperationException(
                        "Unknown feature location authority" );
            }
        }

        throw new NoDataException( "No geographic feature correlation data found at "
                                   + uri );
    }
}
