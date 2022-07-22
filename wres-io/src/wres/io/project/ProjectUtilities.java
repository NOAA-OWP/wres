package wres.io.project;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.FeaturePool;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

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
     * Small value class to hold variable names. TODO: candidate for a "Record" in JDK 17+.
     * @author James Brown
     */

    static class VariableNames
    {
        private final String leftVariableName;
        private final String rightVariableName;
        private final String baselineVariableName;

        String getLeftVariableName()
        {
            return this.leftVariableName;
        }

        String getRightVariableName()
        {
            return this.rightVariableName;
        }

        String getBaselineVariableName()
        {
            return this.baselineVariableName;
        }

        VariableNames( String leftVariableName, String rightVariableName, String baselineVariableName )
        {
            this.leftVariableName = leftVariableName;
            this.rightVariableName = rightVariableName;
            this.baselineVariableName = baselineVariableName;
        }
    }

    /**
     * Creates feature groups from the inputs.
     * @param singletons the singleton features
     * @param featuresForGroups the features for multi-feature groups
     * @param pairConfig the pair configuration
     * @param projectId a project identifier to help with messaging
     * @return the feature groups
     * @throws ProjectConfigException if more than one matching tuple was found
     */

    static Set<FeatureGroup> getFeatureGroups( Set<FeatureTuple> singletons,
                                               Set<FeatureTuple> featuresForGroups,
                                               PairConfig pairConfig,
                                               long projectId )
    {
        LOGGER.debug( "Creating feature groups for project {}.", projectId );
        Set<FeatureGroup> innerGroups = new HashSet<>();

        // Add the singletons
        singletons.forEach( next -> innerGroups.add( FeatureGroup.of( MessageFactory.getGeometryGroup( next.toStringShort(),
                                                                                                       next ) ) ) );
        LOGGER.debug( "Added {} singleton feature groups to project {}.", innerGroups.size(), projectId );

        // Add the multi-feature groups
        List<FeaturePool> declaredGroups =
                ProjectUtilities.getAndValidateDeclaredFeatureGroups( featuresForGroups, pairConfig );

        AtomicInteger groupNumber = new AtomicInteger( 1 ); // For naming when no name is present        
        for ( FeaturePool nextGroup : declaredGroups )
        {
            Set<FeatureTuple> groupedTuples = new HashSet<>();
            Set<FeatureTuple> noDataTuples = new HashSet<>();

            for ( Feature nextFeature : nextGroup.getFeature() )
            {
                FeatureTuple foundTuple = ProjectUtilities.findFeature( nextFeature, featuresForGroups, nextGroup );

                if ( Objects.isNull( foundTuple ) )
                {
                    GeometryTuple geometryTuple = MessageFactory.parse( nextFeature );
                    FeatureTuple noData = FeatureTuple.of( geometryTuple );
                    noDataTuples.add( noData );
                }
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
                LOGGER.warn( "Skipping feature group {} because no features contained any time-series data.",
                             groupName );
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
            LOGGER.warn( "The LEFT and RIGHT variable names were auto-detected, but the detected variable names do not "
                         + "match. The LEFT name is {} and the RIGHT name is {}. Proceeding to pair and evaluate these "
                         + "variables. If this is unexpected behavior, please add explicit variable declaration for "
                         + "both the LEFT and RIGHT data and try again.",
                         leftVariableName,
                         rightVariableName );
        }

        if ( hasBaseline && ( Objects.isNull( declaredLeftVariableName )
                              || Objects.isNull( declaredBaselineVariableName ) )
             && !Objects.equals( leftVariableName, baselineVariableName )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The LEFT and BASELINE variable names were auto-detected, but the detected variable names do "
                         + "not match. The LEFT name is {} and the BASELINE name is {}. Proceeding to pair and "
                         + "evaluate these variables. If this is unexpected behavior, please add explicit variable "
                         + "declaration for both the LEFT and BASELINE data and try again.",
                         leftVariableName,
                         rightVariableName );
        }
    }

    /**
     * Attempts to set the variable names from a set of left, right and baseline names.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     */

    static VariableNames getVariableNames( ProjectConfig projectConfig,
                                           Set<String> left,
                                           Set<String> right,
                                           Set<String> baseline )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );
        Objects.requireNonNull( baseline );

        DataSourceConfig leftConfig = projectConfig.getInputs()
                                                   .getLeft();

        DataSourceConfig rightConfig = projectConfig.getInputs()
                                                    .getRight();

        DataSourceConfig baselineConfig = projectConfig.getInputs()
                                                       .getBaseline();

        // Could not determine variable name
        if ( left.isEmpty() )
        {
            throw new ProjectConfigException( leftConfig,
                                              WHILE_ATTEMPTING_TO_DETECT_THE + LeftOrRightOrBaseline.LEFT
                                                          + VARIABLE
                                                          + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                                          + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                                          + NAME_FOR_THE
                                                          + LeftOrRightOrBaseline.LEFT
                                                          + DATA_SOURCES_TO_DISAMBIGUATE );
        }

        if ( right.isEmpty() )
        {
            throw new ProjectConfigException( rightConfig,
                                              WHILE_ATTEMPTING_TO_DETECT_THE + LeftOrRightOrBaseline.RIGHT
                                                           + VARIABLE
                                                           + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                                           + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                                           + NAME_FOR_THE
                                                           + LeftOrRightOrBaseline.RIGHT
                                                           + DATA_SOURCES_TO_DISAMBIGUATE );
        }

        if ( Objects.nonNull( baselineConfig ) && baseline.isEmpty() )
        {
            throw new ProjectConfigException( baselineConfig,
                                              WHILE_ATTEMPTING_TO_DETECT_THE + LeftOrRightOrBaseline.BASELINE
                                                              + VARIABLE
                                                              + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                                              + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                                              + NAME_FOR_THE
                                                              + LeftOrRightOrBaseline.BASELINE
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

            if ( Objects.nonNull( baselineConfig ) )
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
                                                                      projectConfig.getInputs(),
                                                                      Objects.nonNull( baselineConfig ) );
        }
    }

    /**
     * Attempts to find a unique name by intersecting the left, right and baseline names.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @param inputs the inputs declaration
     * @param hasBaseline whether the project has a baseline defined
     * @throws ProjectConfigException if a unique name could not be discovered
     */

    private static VariableNames getVariableNamesFromIntersection( Set<String> left,
                                                                   Set<String> right,
                                                                   Set<String> baseline,
                                                                   Inputs inputs,
                                                                   boolean hasBaseline )
    {
        LOGGER.debug( "Discovered several variable names for the data sources. Will attempt to intersect them and "
                      + "discover one. The LEFT variable names are {}, the RIGHT variable names are {} and the "
                      + "BASELINE variable names are {}.",
                      left,
                      right,
                      baseline );

        Set<String> intersection = new HashSet<>();
        intersection.addAll( left );
        intersection.retainAll( right );

        if ( hasBaseline )
        {
            intersection.retainAll( baseline );
        }

        if ( intersection.size() == 1 )
        {
            String leftVariableName = intersection.iterator()
                                                  .next();
            String rightVariableName = leftVariableName;
            String baselineVariableName = null;

            if ( hasBaseline )
            {
                baselineVariableName = leftVariableName;
            }

            LOGGER.debug( "After intersecting the variable names, discovered one variable name to evaluate, {}.",
                          leftVariableName );

            return new VariableNames( leftVariableName, rightVariableName, baselineVariableName );
        }
        else
        {
            throw new ProjectConfigException( inputs,
                                              "While attempting to auto-detect "
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
     * @param pairConfig the pair configuration
     * @return the declared groups
     * @throws ProjectConfigException if some groups were declared but no features are available to correlate with them
     */

    private static List<FeaturePool> getAndValidateDeclaredFeatureGroups( Set<FeatureTuple> featuresForGroups,
                                                                          PairConfig pairConfig )
    {
        List<FeaturePool> declaredGroups = pairConfig.getFeatureGroup();

        // Some groups declared, but no fully qualified features available to correlate with the declared features
        if ( !declaredGroups.isEmpty() && featuresForGroups.isEmpty() )
        {
            throw new ProjectConfigException( pairConfig,
                                              "Discovered "
                                                          + declaredGroups.size()
                                                          + " feature group in the project declaration, but could not "
                                                          + "find any features with time-series data to correlate with "
                                                          + "the declared features in these groups. If the feature "
                                                          + "names are missing for some sides of data, it may help to "
                                                          + "qualify them, else to use an explicit "
                                                          + "\"featureDimension\" for all sides of data, in order to "
                                                          + "aid interpolation of the feature names." );
        }

        return declaredGroups;
    }

    /**
     * @param declaredGroup the declared group, not null
     * @param groupNumber the group number to increment when choosing a default group name
     * @return a group name
     */

    private static String getFeatureGroupNameFrom( FeaturePool declaredGroup,
                                                   AtomicInteger groupNumber )
    {
        // Explicit name, use it
        if ( Objects.nonNull( declaredGroup.getName() ) )
        {
            return declaredGroup.getName();
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
     * @throws ProjectConfigException if more than one matching tuple was found
     */

    private static FeatureTuple
            findFeature( Feature featureToFind, Set<FeatureTuple> featuresToSearch, FeaturePool nextGroup )
    {
        // Find the left-name matching features first.
        Set<FeatureTuple> leftMatched = featuresToSearch.stream()
                                                        .filter( next -> Objects.equals( featureToFind.getLeft(),
                                                                                         next.getLeft().getName() ) )
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
            return leftMatched.iterator().next();
        }

        // Find the right-name matching features second.
        Set<FeatureTuple> rightMatched = leftMatched.stream()
                                                    .filter( next -> Objects.equals( featureToFind.getRight(),
                                                                                     next.getRight().getName() ) )
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
            return rightMatched.iterator().next();
        }

        // Find the baseline-name matching features last.
        Set<FeatureTuple> baselineMatched = rightMatched.stream()
                                                        .filter( next -> Objects.equals( featureToFind.getBaseline(),
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
            return baselineMatched.iterator().next();
        }

        throw new ProjectConfigException( nextGroup,
                                          "Discovered a feature group called '" + nextGroup.getName()
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
