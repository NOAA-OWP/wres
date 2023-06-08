package wres.io.project;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.TimeScaleLenience;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.GeometryGroup;

/**
 * Utilities for working with {@link Project}.
 *
 * @author James Brown
 */

class ProjectUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectUtilities.class );

    private static final String FAILED_TO_MATCH_ANY_FEATURES_WITH_TIME_SERIES_DATA_FOR_DECLARED_FEATURE_WHEN =
            "Failed to match any features with time-series data for declared feature {} when ";

    private static final String DATA_SOURCES_TO_DISAMBIGUATE = " data sources to disambiguate.";

    private static final String NAME_FOR_THE = "name for the ";

    private static final String POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE =
            "possibilities. Please declare an explicit variable ";

    private static final String NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY =
            "name from the data, failed to identify any ";

    private static final String VARIABLE = " variable ";

    private static final String WHILE_ATTEMPTING_TO_DETECT_THE = "While attempting to detect the ";

    /**
     * Small value class to hold variable names.
     * @author James Brown
     */

    record VariableNames( String leftVariableName, String rightVariableName, String baselineVariableName ) {}

    /**
     * Creates feature groups from the inputs.
     * @param singletonsWithData the singleton features that have time-series data
     * @param groupedFeaturesWithData the features within multi-feature groups that have time-series data
     * @param declaration the declaration
     * @param projectId a project identifier to help with messaging
     * @return the feature groups
     */

    static Set<FeatureGroup> getFeatureGroups( Set<FeatureTuple> singletonsWithData,
                                               Set<FeatureTuple> groupedFeaturesWithData,
                                               EvaluationDeclaration declaration,
                                               long projectId )
    {
        LOGGER.debug( "Creating feature groups for project {}.", projectId );

        LOGGER.debug( "Discovered these singleton features with time-series data: {}.", singletonsWithData );
        LOGGER.debug( "Discovered these grouped features with time-series data: {}.", groupedFeaturesWithData );

        Set<FeatureGroup> innerGroups = new HashSet<>();

        // Add the singleton groups
        singletonsWithData.forEach( next -> innerGroups.add( FeatureGroup.of( MessageFactory.getGeometryGroup( next.toStringShort(),
                                                                                                               next ) ) ) );
        LOGGER.debug( "Added {} singleton feature groups to project {}.", innerGroups.size(), projectId );

        // Add the multi-feature groups
        Set<GeometryGroup> declaredGroups =
                ProjectUtilities.getAndValidateDeclaredFeatureGroups( groupedFeaturesWithData,
                                                                      declaration );

        AtomicInteger groupNumber = new AtomicInteger( 1 ); // For naming when no name is present        

        // Iterate the declared feature groups and correlate with features that have data
        for ( GeometryGroup nextGroup : declaredGroups )
        {
            Set<FeatureTuple> groupedTuples = new HashSet<>();
            Set<FeatureTuple> noDataTuples = new HashSet<>();

            for ( GeometryTuple nextFeature : nextGroup.getGeometryTuplesList() )
            {
                FeatureTuple foundTuple = ProjectUtilities.findFeature( nextFeature,
                                                                        groupedFeaturesWithData,
                                                                        nextGroup );

                // The next feature in a declared group was not matched by a feature with data
                if ( Objects.isNull( foundTuple ) )
                {
                    FeatureTuple noData = FeatureTuple.of( nextFeature );
                    noDataTuples.add( noData );
                }
                // The next feature in a declared group was matched by a feature with data
                else
                {
                    groupedTuples.add( foundTuple );
                }
            }

            String groupName = ProjectUtilities.getFeatureGroupNameFrom( nextGroup, groupNumber );

            if ( !groupedTuples.isEmpty() )
            {
                GeometryGroup geoGroup = MessageFactory.getGeometryGroup( groupName, groupedTuples );
                FeatureGroup newGroup = FeatureGroup.of( geoGroup );
                innerGroups.add( newGroup );
                LOGGER.debug( "Discovered a new feature group, {}.", newGroup );

                if ( !noDataTuples.isEmpty() && LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( "While processing feature group {}, discovered {} feature tuples without time-series "
                                 + "data. The statistics from this group will contain {} features instead of {} "
                                 + "features. The features without time-series data are: {}.",
                                 groupName,
                                 noDataTuples.size(),
                                 groupedTuples.size(),
                                 groupedTuples.size() + noDataTuples.size(),
                                 noDataTuples );
                }
            }
            else
            {
                LOGGER.warn( "Skipping feature group {} because there were no features with time-series data. The "
                             + "following features had no data: {}.",
                             groupName,
                             noDataTuples );
            }
        }

        LOGGER.info( "Discovered {} feature groups with data on both the left and right sides (statistics "
                     + "should be expected for this many feature groups at most).",
                     innerGroups.size() );


        return Collections.unmodifiableSet( innerGroups );
    }

    /**
     * Validates the variable names and emits a warning if assumptions have been made by the software.
     *
     * @param declaredLeftVariableName the declared left variable name
     * @param declaredRightVariableName the declared right variable name
     * @param declaredBaselineVariableName the declared baseline variable name
     * @param leftVariableName the left variable name
     * @param rightVariableName the right variable name
     * @param baselineVariableName the baseline variable name
     * @param hasBaseline is true if the project has a baseline, false otherwise
     */

    static void validateVariableNames( String declaredLeftVariableName,
                                       String declaredRightVariableName,
                                       String declaredBaselineVariableName,
                                       String leftVariableName,
                                       String rightVariableName,
                                       String baselineVariableName,
                                       boolean hasBaseline )
    {
        // Warn if the names were not declared and are different
        if ( ( Objects.isNull( declaredLeftVariableName )
               || Objects.isNull( declaredRightVariableName ) )
             && !Objects.equals( leftVariableName, rightVariableName )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The left and right variable names were auto-detected, but the detected variable names do not "
                         + "match. The left name is {} and the right name is {}. Proceeding to pair and evaluate these "
                         + "variables. If this is unexpected behavior, please add explicit variable declaration for "
                         + "both the left and right data and try again.",
                         leftVariableName,
                         rightVariableName );
        }

        if ( hasBaseline && ( Objects.isNull( declaredLeftVariableName )
                              || Objects.isNull( declaredBaselineVariableName ) )
             && !Objects.equals( leftVariableName, baselineVariableName )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The left and baseline variable names were auto-detected, but the detected variable names do "
                         + "not match. The left name is {} and the baseline name is {}. Proceeding to pair and "
                         + "evaluate these variables. If this is unexpected behavior, please add explicit variable "
                         + "declaration for both the left and baseline data and try again.",
                         leftVariableName,
                         rightVariableName );
        }
    }

    /**
     * Attempts to set the variable names from a set of left, right and baseline names.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @throws DeclarationException is the variable names could not be determined
     */

    static VariableNames getVariableNames( EvaluationDeclaration declaration,
                                           Set<String> left,
                                           Set<String> right,
                                           Set<String> baseline )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );
        Objects.requireNonNull( baseline );

        // Could not determine variable name
        if ( left.isEmpty() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE + DatasetOrientation.LEFT
                                            + VARIABLE
                                            + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                            + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                            + NAME_FOR_THE
                                            + DatasetOrientation.LEFT
                                            + DATA_SOURCES_TO_DISAMBIGUATE );
        }

        if ( right.isEmpty() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE + DatasetOrientation.RIGHT
                                            + VARIABLE
                                            + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                            + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                            + NAME_FOR_THE
                                            + DatasetOrientation.RIGHT
                                            + DATA_SOURCES_TO_DISAMBIGUATE );
        }

        if ( DeclarationUtilities.hasBaseline( declaration ) && baseline.isEmpty() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE + DatasetOrientation.BASELINE
                                            + VARIABLE
                                            + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                            + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                            + NAME_FOR_THE
                                            + DatasetOrientation.BASELINE
                                            + DATA_SOURCES_TO_DISAMBIGUATE );

        }

        // One variable name for all? Allow. 
        if ( left.size() == 1 && right.size() == 1 && ( baseline.isEmpty() || baseline.size() == 1 ) )
        {
            String leftVariableName = left.iterator()
                                          .next();
            String rightVariableName = right.iterator()
                                            .next();

            String baselineVariableName = null;

            if ( DeclarationUtilities.hasBaseline( declaration ) )
            {
                baselineVariableName = baseline.iterator()
                                               .next();
            }

            LOGGER.debug( "Discovered one variable name for all data sources. The variable name is {}.",
                          leftVariableName );

            return new VariableNames( leftVariableName, rightVariableName, baselineVariableName );
        }
        // More than one for some, need to intersect
        else
        {
            return ProjectUtilities.getVariableNamesFromIntersection( left,
                                                                      right,
                                                                      baseline,
                                                                      declaration );
        }
    }

    /**
     * Tests whether there is a desired time scale present that supports lenient upscaling for the specified side of 
     * data.
     * @param orientation the orientation of the data, required
     * @param desiredTimeScale the desired timescale, optional
     * @param lenience whether the timescale is lenient, required if the timescale is provided
     * @return true if the desiredTimeScale is not null and supports lenient upscaling for the specified orientation
     */

    static boolean isUpscalingLenient( DatasetOrientation orientation,
                                       TimeScale desiredTimeScale,
                                       TimeScaleLenience lenience )
    {
        Objects.requireNonNull( orientation );

        if ( Objects.isNull( desiredTimeScale )
             || Objects.isNull( desiredTimeScale.timeScale() ) )
        {
            return false;
        }

        return switch ( lenience )
                {
                    case ALL -> true;
                    case NONE -> false;
                    case LEFT -> orientation == DatasetOrientation.LEFT;
                    case RIGHT -> orientation == DatasetOrientation.RIGHT;
                    case BASELINE -> orientation == DatasetOrientation.BASELINE;
                    case LEFT_AND_RIGHT -> orientation == DatasetOrientation.LEFT
                                           || orientation == DatasetOrientation.RIGHT;
                    case LEFT_AND_BASELINE -> orientation == DatasetOrientation.LEFT
                                              || orientation == DatasetOrientation.BASELINE;
                    case RIGHT_AND_BASELINE -> orientation == DatasetOrientation.RIGHT
                                               || orientation == DatasetOrientation.BASELINE;
                };
    }

    /**
     * Filters the supplied features against a spatial mask, returning features that are contained within the mask
     * region.
     *
     * @param features the features to filter
     * @param spatialMask the spatial mask to use
     * @return the filtered features
     */

    static Set<FeatureTuple> filterFeatures( Set<FeatureTuple> features,
                                             SpatialMask spatialMask )
    {
        if ( Objects.isNull( spatialMask ) )
        {
            LOGGER.debug( "No spatial mask was found, returning the unfiltered features." );
            return features;
        }

        // Warn about any features that are not geospatial
        if ( LOGGER.isWarnEnabled() )
        {
            Set<FeatureTuple> notGeospatial
                    = features.stream()
                              .filter( a -> ProjectUtilities.isNotGeospatial( a.getGeometryTuple() ) )
                              .collect( Collectors.toSet() );
            if ( !notGeospatial.isEmpty() )
            {
                LOGGER.warn( "Discovered a spatial mask to filter geospatial features, but {} of the declared features "
                             + "did not have any geospatial information present and cannot be filtered. These features "
                             + "are: {}.",
                             notGeospatial.size(),
                             notGeospatial );
            }
        }

        Geometry maskGeometry = spatialMask.geometry();

        Set<FeatureTuple> inside = new TreeSet<>();
        Set<FeatureTuple> outside = new TreeSet<>();

        WKTReader reader = new WKTReader();

        for ( FeatureTuple tuple : features )
        {
            GeometryTuple nextGeom = tuple.getGeometryTuple();
            boolean include = ProjectUtilities.isContained( nextGeom, maskGeometry, reader );

            // Contained?
            if ( include )
            {
                inside.add( tuple );
            }
            else
            {
                outside.add( tuple );
            }
        }

        // Warn if features were removed
        if ( !outside.isEmpty()
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "When filtering features against the declared 'spatial_mask', encountered {} feature(s) "
                         + "outside the mask area, which will not be included in the evaluation: {}.",
                         outside.size(),
                         outside );
        }

        return Collections.unmodifiableSet( inside );
    }

    /**
     * Filters the features within the supplied feature groups against a spatial mask, removing any features that are
     * not contained within the mask region.
     *
     * @param featureGroups the feature groups to filter
     * @param spatialMask the spatial mask to use
     * @return the filtered feature groups
     */

    static Set<FeatureGroup> filterFeatureGroups( Set<FeatureGroup> featureGroups,
                                                  SpatialMask spatialMask )
    {
        if ( Objects.isNull( spatialMask ) )
        {
            LOGGER.debug( "No spatial mask was found, returning the unfiltered feature groups." );
            return featureGroups;
        }

        Geometry maskGeometry = spatialMask.geometry();

        // All groups after filtering
        Set<FeatureGroup> all = new TreeSet<>();
        // Groups that were adjusted but retained
        Set<FeatureGroup> adjusted = new TreeSet<>();
        // Groups that had no features left
        Set<FeatureGroup> removed = new TreeSet<>();

        WKTReader reader = new WKTReader();

        for ( FeatureGroup group : featureGroups )
        {
            GeometryGroup nextGroup = group.getGeometryGroup();
            GeometryGroup adjustedGroup = ProjectUtilities.adjustForContainment( nextGroup, maskGeometry, reader );

            // Removed
            if ( adjustedGroup.getGeometryTuplesList()
                              .isEmpty() )
            {
                removed.add( group );
            }
            // Adjusted, but retained
            else if ( !adjustedGroup.equals( nextGroup ) )
            {
                FeatureGroup adjustedFeatureGroup = FeatureGroup.of( adjustedGroup );
                adjusted.add( adjustedFeatureGroup );
                all.add( adjustedFeatureGroup );
            }
            // Unadjusted
            else
            {
                FeatureGroup adjustedFeatureGroup = FeatureGroup.of( adjustedGroup );
                all.add( adjustedFeatureGroup );
            }
        }

        // Warn if feature groups were adjusted
        if ( !adjusted.isEmpty()
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "When filtering feature groups against the declared 'spatial_mask', encountered {} feature "
                         + "group(s) with one or more features outside the mask area. These feature groups have been "
                         + "adjusted to remove the features outside the mask area. The adjusted feature groups are: "
                         + "{}.",
                         adjusted.size(),
                         adjusted );
        }

        // Warn if feature groups were removed
        if ( !removed.isEmpty()
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "When filtering feature groups against the declared 'spatial_mask', encountered {} feature "
                         + "group(s) whose features were all outside the mask area. These feature groups have been "
                         + "removed from the evaluation. The removed feature groups are: "
                         + "{}.",
                         removed.size(),
                         removed );
        }

        // Warn about any features that are not geospatial
        if ( LOGGER.isWarnEnabled() )
        {
            Set<FeatureTuple> notGeospatial
                    = featureGroups.stream()
                                   .flatMap( a -> a.getFeatures().stream() )
                                   .filter( a -> ProjectUtilities.isNotGeospatial( a.getGeometryTuple() ) )
                                   .collect( Collectors.toSet() );
            if ( !notGeospatial.isEmpty() )
            {
                LOGGER.warn( "Discovered a spatial mask to filter geospatial feature groups, but {} of the features "
                             + "contained within the declared feature groups did not have any geospatial information "
                             + "present and cannot be filtered. These features are: {}.",
                             notGeospatial.size(),
                             notGeospatial );
            }
        }

        return Collections.unmodifiableSet( all );
    }

    /**
     * Adjusted the supplied feature group, removing any features that are not contained within the mask area.
     *
     * @param group the feature group to adjust
     * @param spatialMask the spatial mask to use
     * @param reader the WKT string reader
     * @return the adjusted group, which may contain no features
     */

    private static GeometryGroup adjustForContainment( GeometryGroup group, Geometry spatialMask, WKTReader reader )
    {
        GeometryGroup.Builder builder = group.toBuilder()
                                             .clearGeometryTuples();

        List<GeometryTuple> features = group.getGeometryTuplesList()
                                            .stream()
                                            .filter( g -> ProjectUtilities.isContained( g, spatialMask, reader ) )
                                            .toList();

        return builder.addAllGeometryTuples( features )
                      .build();
    }

    /**
     * Checks the supplied geometry for containment within the mask.
     * @param nextGeom the geometry to check for containment
     * @param maskGeometry the mask geometry
     * @param reader the WKT string reader
     * @return whether the geometry is contained in the mask region
     */
    private static boolean isContained( GeometryTuple nextGeom, Geometry maskGeometry, WKTReader reader )
    {
        boolean include = true;
        String lastWkt = "";

        try
        {
            // Left present and inside?
            if ( nextGeom.hasLeft()
                 && ProjectUtilities.isGeospatial( nextGeom.getLeft() ) )
            {
                lastWkt = nextGeom.getLeft()
                                  .getWkt();
                Geometry geometry = reader.read( lastWkt );
                include = maskGeometry.covers( geometry );
            }

            // Right present and inside?
            if ( nextGeom.hasRight()
                 && ProjectUtilities.isGeospatial( nextGeom.getRight() ) )
            {
                lastWkt = nextGeom.getRight()
                                  .getWkt();
                Geometry geometry = reader.read( lastWkt );
                include = include && maskGeometry.covers( geometry );
            }

            // Baseline present and inside?
            if ( nextGeom.hasBaseline()
                 && ProjectUtilities.isGeospatial( nextGeom.getBaseline() ) )
            {
                lastWkt = nextGeom.getBaseline()
                                  .getWkt();
                Geometry geometry = reader.read( lastWkt );
                include = include && maskGeometry.covers( geometry );
            }
        }
        catch ( ParseException e )
        {
            throw new DataAccessException( "Failed to pass a WKT geometry string into a geometry: " + lastWkt );
        }

        return include;
    }

    /**
     * @param geometryTuple the geometry tuple to test
     * @return whether the geometry tuple has no geospatial information for any available side of data
     */

    private static boolean isNotGeospatial( GeometryTuple geometryTuple )
    {
        return ( !geometryTuple.hasLeft() || !ProjectUtilities.isGeospatial( geometryTuple.getLeft() ) )
               && ( !geometryTuple.hasRight() || !ProjectUtilities.isGeospatial( geometryTuple.getRight() ) )
               && ( !geometryTuple.hasBaseline() || !ProjectUtilities.isGeospatial( geometryTuple.getBaseline() ) );
    }

    /**
     * @param geometry the geometry to test
     * @return whether the geometry has geospatial information present
     */

    private static boolean isGeospatial( wres.statistics.generated.Geometry geometry )
    {
        return !geometry.getWkt()
                        .isBlank();
    }

    /**
     * Attempts to find a unique name by intersecting the left, right and baseline names.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @param declaration the declaration
     * @throws DeclarationException if a unique name could not be discovered
     */

    private static VariableNames getVariableNamesFromIntersection( Set<String> left,
                                                                   Set<String> right,
                                                                   Set<String> baseline,
                                                                   EvaluationDeclaration declaration )
    {
        LOGGER.debug( "Discovered several variable names for the data sources. Will attempt to intersect them and "
                      + "discover one. The LEFT variable names are {}, the RIGHT variable names are {} and the "
                      + "BASELINE variable names are {}.",
                      left,
                      right,
                      baseline );

        Set<String> intersection = new HashSet<>( left );
        intersection.retainAll( right );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            intersection.retainAll( baseline );
        }

        if ( intersection.size() == 1 )
        {
            String leftVariableName = intersection.iterator()
                                                  .next();
            String baselineVariableName = null;

            if ( DeclarationUtilities.hasBaseline( declaration ) )
            {
                baselineVariableName = leftVariableName;
            }

            LOGGER.debug( "After intersecting the variable names, discovered one variable name to evaluate, {}.",
                          leftVariableName );

            return new VariableNames( leftVariableName, leftVariableName, baselineVariableName );
        }
        else
        {
            throw new DeclarationException( "While attempting to auto-detect "
                                            + "the variable to evaluate, failed to identify a "
                                            + "single variable name that is common to all data "
                                            + "sources. Discovered LEFT variable names of "
                                            + left
                                            + ", RIGHT variable names of "
                                            + right
                                            + " and BASELINE variable names of "
                                            + baseline
                                            + ". Please declare an explicit variable name for "
                                            + "each required data source to disambiguate." );
        }
    }

    /**
     * Returns the declared feature groups.
     * @param featuresForGroups the fully qualified features available to correlate with the declared groups
     * @param declaration the declaration
     * @return the declared groups
     * @throws DeclarationException if some groups were declared but no features are available to correlate with them
     */

    private static Set<GeometryGroup> getAndValidateDeclaredFeatureGroups( Set<FeatureTuple> featuresForGroups,
                                                                           EvaluationDeclaration declaration )
    {
        FeatureGroups declaredGroups = declaration.featureGroups();

        // Some groups declared, but no fully qualified features available to correlate with the declared features
        if ( Objects.nonNull( declaredGroups )
             && !declaredGroups.geometryGroups()
                               .isEmpty()
             && featuresForGroups.isEmpty() )
        {
            throw new DeclarationException( "Discovered "
                                            + declaredGroups.geometryGroups()
                                                            .size()
                                            + " feature group in the project declaration, but could not "
                                            + "find any features with time-series data to correlate with "
                                            + "the declared features in these groups. If the feature "
                                            + "names are missing for some sides of data, it may help to "
                                            + "qualify them, else to use an explicit "
                                            + "'feature_authority' for all sides of data, in order to "
                                            + "aid interpolation of the feature names." );
        }

        Set<GeometryGroup> geometries = new HashSet<>();
        if ( Objects.nonNull( declaredGroups ) )
        {
            geometries.addAll( declaredGroups.geometryGroups() );
        }
        return Collections.unmodifiableSet( geometries );
    }

    /**
     * @param declaredGroup the declared group, not null
     * @param groupNumber the group number to increment when choosing a default group name
     * @return a group name
     */

    private static String getFeatureGroupNameFrom( GeometryGroup declaredGroup,
                                                   AtomicInteger groupNumber )
    {
        // Explicit name, use it
        if ( !declaredGroup.getRegionName()
                           .isBlank() )
        {
            return declaredGroup.getRegionName();
        }

        else
        {
            return "GROUP_" + groupNumber.getAndIncrement();
        }
    }

    /**
     * Searches for a matching feature tuple and throws an exception if more than one is found.
     * @param featureToFind the declared feature to find
     * @param featuresToSearch the fully elaborated feature tuples to search
     * @return a matching tuple or null if no tuple was found
     * @throws DeclarationException if more than one matching tuple was found
     */

    private static FeatureTuple findFeature( GeometryTuple featureToFind,
                                             Set<FeatureTuple> featuresToSearch,
                                             GeometryGroup nextGroup )
    {
        // Find the left-name matching features first.
        Set<FeatureTuple> leftMatched = featuresToSearch.stream()
                                                        .filter( next -> Objects.equals( featureToFind.getLeft()
                                                                                                      .getName(),
                                                                                         next.getLeft()
                                                                                             .getName() ) )
                                                        .collect( Collectors.toSet() );

        if ( leftMatched.isEmpty() )
        {
            LOGGER.debug( FAILED_TO_MATCH_ANY_FEATURES_WITH_TIME_SERIES_DATA_FOR_DECLARED_FEATURE_WHEN
                          + "considering the left name. The available features were: {}.",
                          featureToFind,
                          featuresToSearch );

            return null;
        }
        else if ( leftMatched.size() == 1 )
        {
            return leftMatched.iterator()
                              .next();
        }

        // Find the right-name matching features second.
        Set<FeatureTuple> rightMatched = leftMatched.stream()
                                                    .filter( next -> Objects.equals( featureToFind.getRight()
                                                                                                  .getName(),
                                                                                     next.getRight()
                                                                                         .getName() ) )
                                                    .collect( Collectors.toSet() );

        if ( rightMatched.isEmpty() )
        {
            LOGGER.debug( FAILED_TO_MATCH_ANY_FEATURES_WITH_TIME_SERIES_DATA_FOR_DECLARED_FEATURE_WHEN
                          + "considering both the left and right names. The available features were: {}.",
                          featureToFind,
                          featuresToSearch );

            return null;
        }
        else if ( rightMatched.size() == 1 )
        {
            return rightMatched.iterator()
                               .next();
        }

        // Find the baseline-name matching features last.
        Set<FeatureTuple> baselineMatched = rightMatched.stream()
                                                        .filter( next -> Objects.nonNull( next.getBaseline() ) )
                                                        .filter( next -> Objects.equals( featureToFind.getBaseline()
                                                                                                      .getName(),
                                                                                         next.getBaseline()
                                                                                             .getName() ) )
                                                        .collect( Collectors.toSet() );

        if ( baselineMatched.isEmpty() )
        {
            LOGGER.debug( FAILED_TO_MATCH_ANY_FEATURES_WITH_TIME_SERIES_DATA_FOR_DECLARED_FEATURE_WHEN
                          + "considering the left, right and baseline names. The available features were: {}.",
                          featureToFind,
                          featuresToSearch );

            return null;
        }
        else if ( baselineMatched.size() == 1 )
        {
            return baselineMatched.iterator()
                                  .next();
        }

        throw new DeclarationException( "Discovered a feature group called '" + nextGroup.getRegionName()
                                        + "', which has an ambiguous feature tuple. Please additionally "
                                        + "qualify the feature with left name "
                                        + featureToFind.getLeft()
                                        + ", right name "
                                        + featureToFind.getRight()
                                        + " and baseline name "
                                        + featureToFind.getBaseline()
                                        + ", which matches the feature tuples "
                                        + baselineMatched
                                        + "." );
    }

}
