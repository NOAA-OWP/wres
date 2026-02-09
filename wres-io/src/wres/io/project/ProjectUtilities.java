package wres.io.project;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DataTypes;
import wres.config.DeclarationException;
import wres.config.DeclarationInterpolator;
import wres.config.DeclarationUtilities;
import wres.config.DeclarationValidator;
import wres.config.VariableNames;
import wres.config.components.CovariateDataset;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetOrientation;
import wres.config.components.EnsembleFilter;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.FeatureGroups;
import wres.config.components.FeatureGroupsBuilder;
import wres.config.components.Features;
import wres.config.components.Offset;
import wres.config.components.SpatialMask;
import wres.config.components.TimeScale;
import wres.config.components.TimeScaleLenience;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.io.NoProjectDataException;
import wres.io.ingesting.IngestResult;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.SummaryStatistic;

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
     * Small collection of geographic features to use in different contexts.
     * @param features the singleton features
     * @param featureGroups the feature groups
     * @param doNotPublish the feature groups whose raw statistics should not be published
     */
    record FeatureSets( Set<FeatureTuple> features,
                        Set<FeatureGroup> featureGroups,
                        Set<FeatureGroup> doNotPublish ) {}

    /**
     * Creates feature groups from the inputs.
     * @param singletonsWithData the singleton features that have time-series data
     * @param groupedFeaturesWithData the features within multi-feature groups that have time-series data
     * @param declaration the declaration
     * @param projectId a project identifier to help with messaging
     * @return the feature groups
     */

    static FeatureSets getFeatureGroups( Set<FeatureTuple> singletonsWithData,
                                         Set<FeatureTuple> groupedFeaturesWithData,
                                         EvaluationDeclaration declaration,
                                         long projectId )
    {
        LOGGER.debug( "Creating feature groups for project {}.", projectId );

        LOGGER.debug( "Discovered these singleton features with time-series data: {}.", singletonsWithData );
        LOGGER.debug( "Discovered these grouped features with time-series data: {}.", groupedFeaturesWithData );

        // Add the singleton groups
        Set<FeatureGroup> singletonGroups
                = singletonsWithData.stream()
                                    .map( f -> FeatureGroup.of( MessageFactory.getGeometryGroup( f.toStringShort(),
                                                                                                 f ) ) )
                                    .collect( Collectors.toUnmodifiableSet() );
        Set<FeatureGroup> innerGroups = new HashSet<>( singletonGroups );

        LOGGER.debug( "Added {} singleton feature groups to project {}.", innerGroups.size(), projectId );

        // Add the multi-feature groups
        Set<FeatureGroup> multiFeatureGroups = ProjectUtilities.getMultiFeatureGroups( declaration,
                                                                                       groupedFeaturesWithData );
        innerGroups.addAll( multiFeatureGroups );

        LOGGER.info( "Discovered {} feature groups with data on both the 'observed' and 'predicted' sides (statistics "
                     + "should be expected for this many feature groups at most).",
                     innerGroups.size() );

        // When summary statistics are required for feature groups, add the singleton members regardless of whether they
        // were declared, because the statistics are needed for every singleton feature
        // If summary statistics are required for feature groups, ensure that all singletons are present too
        Set<FeatureGroup> doNotPublish = new HashSet<>();
        if ( declaration.summaryStatistics()
                        .stream()
                        .anyMatch( s -> s.getDimensionList()
                                         .contains( SummaryStatistic.StatisticDimension.FEATURE_GROUP ) ) )
        {
            groupedFeaturesWithData.forEach( f -> doNotPublish.add( FeatureGroup.of( MessageFactory.getGeometryGroup( f.toStringShort(),
                                                                                                                      f ) ) ) );
            // Remove all the declared singletons
            doNotPublish.removeAll( singletonGroups );
            LOGGER.debug( "Added the following singleton feature groups to calculate summary statistics only: {}",
                          doNotPublish );
            innerGroups.addAll( doNotPublish );
        }

        FeatureSets featureSets = new FeatureSets( singletonsWithData,
                                                   Collections.unmodifiableSet( innerGroups ),
                                                   Collections.unmodifiableSet( doNotPublish ) );

        return ProjectUtilities.adjustForAliasedFeatures( featureSets );
    }

    /**
     * Insects the input for singleton feature tuples that are "aliased" and places the aliased tuples in a
     * multi-feature group, removing them as singleton groups. A feature tuple is "aliased" if more than one tuple
     * contains the same feature names across all dataset orientations or sides of data, implying that some metadata
     * other than the feature names (e.g., coordinates) differs between the aliased tuples. In this situation, a user
     * would generally expect that the aliased feature tuples are grouped together rather than, for example, evaluating
     * the time-series from each aliased feature tuple separately.
     *
     * @param featureSets the feature sets
     * @return the feature sets adjusted for aliases
     */

    private static FeatureSets adjustForAliasedFeatures( FeatureSets featureSets )
    {
        // Group the singletons by common feature tuple name, filtering for those with two or more per group
        Map<String, List<FeatureTuple>> grouped =
                featureSets.features()
                           .stream()
                           .collect( Collectors.groupingBy( FeatureTuple::toStringShort ) )
                           .entrySet()
                           .stream()
                           .filter( e -> e.getValue()
                                          .size() > 1 )
                           .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );

        if ( !grouped.isEmpty() )
        {
            LOGGER.warn( "Discovered several feature tuples with aliased feature names. This occurs when the feature "
                         + "names are shared across two or more feature tuples whose other geospatial metadata (e.g., "
                         + "coordinates) is different. Since these features have the same names and feature authorities, "
                         + "it is assumed that they are intended to be treated as the same features. As such, the "
                         + "following feature tuples will be grouped together under their corresponding feature "
                         + "names, which has the same effect as treating the aliased features as part of a multi-"
                         + "feature group whose time-series data will be pooled: {}.",
                         grouped );
        }

        // Adjust the feature set, removing the aliased singletons and replacing them with a multi-feature group
        Set<String> remove = grouped.keySet();
        Set<FeatureTuple> adjustedSingletons = featureSets.features()
                                                          .stream()
                                                          .filter( f -> !remove.contains( f.toStringShort() ) )
                                                          .collect( Collectors.toUnmodifiableSet() );
        // Singletons are also one-feature groups, so remove from the groups too
        Set<FeatureGroup> adjustedGroups = featureSets.featureGroups()
                                                      .stream()
                                                      .filter( f -> !remove.contains( f.getName() ) )
                                                      .collect( Collectors.toSet() );

        for ( Map.Entry<String, List<FeatureTuple>> nextGroup : grouped.entrySet() )
        {
            String groupName = nextGroup.getKey();
            List<FeatureTuple> groupFeatures = nextGroup.getValue();
            GeometryGroup geometryGroup = GeometryGroup.newBuilder()
                                                       .setRegionName( groupName )
                                                       .addAllGeometryTuples( groupFeatures.stream()
                                                                                           .map( FeatureTuple::getGeometryTuple )
                                                                                           .toList() )
                                                       .build();
            FeatureGroup group = FeatureGroup.of( geometryGroup );
            adjustedGroups.add( group );
        }

        return new FeatureSets( adjustedSingletons,
                                Collections.unmodifiableSet( adjustedGroups ),
                                featureSets.doNotPublish() );
    }

    /**
     * Validates the variable names and emits a warning if assumptions have been made by the software.
     *
     * @param declaration the declaration
     * @param variableNames the variable names
     */

    private static void checkAutoDetectedVariableNames( EvaluationDeclaration declaration,
                                                        VariableNames variableNames )
    {
        String declaredLeftVariableName = DeclarationUtilities.getVariableName( declaration.left() );
        String declaredRightVariableName = DeclarationUtilities.getVariableName( declaration.right() );
        String declaredBaselineVariableName = null;
        String leftVariableName = variableNames.leftVariableName();
        String rightVariableName = variableNames.rightVariableName();
        String baselineVariableName = variableNames.baselineVariableName();

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            declaredBaselineVariableName = DeclarationUtilities.getVariableName( declaration.baseline()
                                                                                            .dataset() );
        }

        // Warn if the names were not declared and are different
        if ( ( Objects.isNull( declaredLeftVariableName )
               || Objects.isNull( declaredRightVariableName ) )
             && !Objects.equals( leftVariableName, rightVariableName )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The 'observed' and 'predicted' variable names were auto-detected, but the detected variable "
                         + "names do not match. The 'observed' name is {} and the 'predicted' name is {}. Proceeding "
                         + "to pair and evaluate these variables. If this is unexpected behavior, please explicitly "
                         + "declare the 'name' of the 'variable' for both the 'observed' and 'predicted' datasets and "
                         + "try again.",
                         leftVariableName,
                         rightVariableName );
        }

        if ( DeclarationUtilities.hasBaseline( declaration )
             && ( Objects.isNull( declaredLeftVariableName )
                  || Objects.isNull( declaredBaselineVariableName ) )
             && !Objects.equals( leftVariableName, baselineVariableName )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The 'observed' and 'baseline' variable names were auto-detected, but the detected variable "
                         + "names do not match. The 'observed' name is {} and the 'baseline' name is {}. Proceeding to "
                         + "pair and evaluate these variables. If this is unexpected behavior, please explicitly "
                         + "declare the 'name' of the 'variable' for both the 'observed' and 'baseline' datasets and "
                         + "try again.",
                         leftVariableName,
                         rightVariableName );
        }
    }

    /**
     * Checks that the declared and ingested variable names are consistent hen both are available.
     *
     * @param declaration the declaration
     * @param left the possible names for left variables, according to ingest and where available
     * @param right the possible names for right variables, according to ingest and where available
     * @param baseline the possible names for baseline variables, according to ingest and where available
     * @param covariates the possible names for covariate variables, according to ingest and where available
     * @throws DeclarationException if the declared and ingested variable names are inconsistent
     */

    private static void checkDeclaredAndIngestedVariablesAgree( EvaluationDeclaration declaration,
                                                                Set<String> left,
                                                                Set<String> right,
                                                                Set<String> baseline,
                                                                Set<String> covariates )
    {
        Set<String> declaredLeft = ProjectUtilities.getVariableNames( declaration.left() );
        Set<String> declaredRight = ProjectUtilities.getVariableNames( declaration.right() );
        Set<String> declaredBaseline = Collections.emptySet();
        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            declaredBaseline = ProjectUtilities.getVariableNames( declaration.baseline()
                                                                             .dataset() );
        }

        // Where a name has been declared and the ingested names are available, the declared name must be among them
        String leftError = "";
        String rightError = "";
        String baselineError = "";
        StringBuilder covariateError = new StringBuilder();

        String start = "    - ";
        if ( !declaredLeft.isEmpty()
             && !left.isEmpty()
             && left.stream()
                    .noneMatch( declaredLeft::contains ) )
        {
            leftError += System.lineSeparator() + start;
            leftError += ProjectUtilities.getVariableErrorMessage( declaredLeft,
                                                                   left,
                                                                   DatasetOrientation.LEFT );
        }
        if ( !declaredRight.isEmpty()
             && !right.isEmpty()
             && right.stream()
                     .noneMatch( declaredRight::contains ) )
        {
            rightError += System.lineSeparator() + start;
            rightError += ProjectUtilities.getVariableErrorMessage( declaredRight,
                                                                    right,
                                                                    DatasetOrientation.RIGHT );
        }
        if ( !declaredBaseline.isEmpty()
             && !baseline.isEmpty()
             && baseline.stream()
                        .noneMatch( declaredBaseline::contains ) )
        {
            baselineError += System.lineSeparator() + start;
            baselineError += ProjectUtilities.getVariableErrorMessage( declaredBaseline,
                                                                       baseline,
                                                                       DatasetOrientation.BASELINE );
        }

        // Iterate through the covariates
        for ( CovariateDataset covariate : declaration.covariates() )
        {
            Set<String> declaredNames = ProjectUtilities.getVariableNames( covariate.dataset() );
            if ( !declaredNames.isEmpty()
                 && covariates.stream()
                              .noneMatch( declaredNames::contains ) )
            {
                covariateError.append( System.lineSeparator() )
                              .append( start );
                covariateError.append( ProjectUtilities.getVariableErrorMessage( declaredNames,
                                                                                 covariates,
                                                                                 DatasetOrientation.COVARIATE ) );
            }
        }

        if ( !leftError.isBlank()
             || !rightError.isBlank()
             || !baselineError.isBlank()
             || !covariateError.toString()
                               .isBlank() )
        {
            throw new DeclarationException( "The declared variable names are inconsistent with the ingested data "
                                            + "sources in one or more contexts: "
                                            + leftError
                                            + rightError
                                            + baselineError
                                            + covariateError );
        }
    }

    /**
     * Looks for a variable name.
     * @param dataset the dataset
     * @return the declared variable name or null if no variable was declared
     * @throws NullPointerException if the dataset is null
     */

    private static Set<String> getVariableNames( Dataset dataset )
    {
        Objects.requireNonNull( dataset );

        Set<String> names = new TreeSet<>();

        if ( Objects.nonNull( dataset.variable() ) )
        {
            if ( Objects.nonNull( dataset.variable()
                                         .name() ) )
            {
                String variableName = dataset.variable()
                                             .name();
                names.add( variableName );
            }

            names.addAll( dataset.variable()
                                 .aliases() );
        }

        return Collections.unmodifiableSet( names );
    }

    /**
     * Generates an error message when a declared variable or one of its aliases are not among the ingested options.
     * @param variableNames the declared variable names
     * @param nameOptions the name options
     * @param orientation the dataset orientation
     * @return the error message
     */
    private static String getVariableErrorMessage( Set<String> variableNames,
                                                   Set<String> nameOptions,
                                                   DatasetOrientation orientation )
    {
        String article = "the";
        if ( orientation == DatasetOrientation.COVARIATE )
        {
            article = "a";
        }

        return "The declared 'name' and 'aliases' of the 'variable' for "
               + article
               + " '"
               + orientation
               + "' dataset included: "
               + variableNames
               + ". However, none of these names were found among the variable names of the ingested data sources, "
               + "whose names included: "
               + nameOptions
               + ". Please declare the 'name' or 'aliases' of a 'variable' that selects data for at least one of the "
               + "geographic features contained in this evaluation and try again.";
    }

    /**
     * Attempts to get the variable names from a set of left, right and baseline names.
     * @param leftVariablesFromData the possible left variable names from ingested data
     * @param rightVariablesFromData the possible right variable names from ingested data
     * @param baselineVariablesFromData the possible baseline variable names from ingested data
     * @param covariateVariablesFromData the covariate variable names from ingested data
     * @return the variable names
     * @throws DeclarationException is the variable names could not be determined or are otherwise invalid
     */

    static VariableNames getVariableNames( EvaluationDeclaration declaration,
                                           Set<String> leftVariablesFromData,
                                           Set<String> rightVariablesFromData,
                                           Set<String> baselineVariablesFromData,
                                           Set<String> covariateVariablesFromData )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( leftVariablesFromData );
        Objects.requireNonNull( rightVariablesFromData );
        Objects.requireNonNull( baselineVariablesFromData );
        Objects.requireNonNull( covariateVariablesFromData );

        // Could not determine variable name
        if ( leftVariablesFromData.isEmpty() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE + DatasetOrientation.LEFT
                                            + VARIABLE
                                            + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                            + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                            + NAME_FOR_THE
                                            + DatasetOrientation.LEFT
                                            + DATA_SOURCES_TO_DISAMBIGUATE );
        }

        if ( rightVariablesFromData.isEmpty() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE + DatasetOrientation.RIGHT
                                            + VARIABLE
                                            + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                            + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                            + NAME_FOR_THE
                                            + DatasetOrientation.RIGHT
                                            + DATA_SOURCES_TO_DISAMBIGUATE );
        }

        if ( DeclarationUtilities.hasBaseline( declaration )
             && baselineVariablesFromData.isEmpty() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE + DatasetOrientation.BASELINE
                                            + VARIABLE
                                            + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                            + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                            + NAME_FOR_THE
                                            + DatasetOrientation.BASELINE
                                            + DATA_SOURCES_TO_DISAMBIGUATE );

        }

        // Check that covariate names are present when required
        ProjectUtilities.covariateVariableNamesArePresentWhenRequired( declaration, covariateVariablesFromData );

        // Further determine the variable names based on ingested sources
        VariableNames variableNames;

        // Use declared names when available, otherwise ingested names when there is one of them. If there is more than
        // one, then intersect
        String leftVariableName = null;
        String rightVariableName = null;
        String baselineVariableName = null;

        if ( ProjectUtilities.hasVariableName( declaration.left() ) )
        {
            leftVariableName = declaration.left()
                                          .variable()
                                          .name();
        }
        else if ( leftVariablesFromData.size() == 1 )
        {
            leftVariableName = leftVariablesFromData.iterator()
                                                    .next();
        }

        if ( ProjectUtilities.hasVariableName( declaration.right() ) )
        {
            rightVariableName = declaration.right()
                                           .variable()
                                           .name();
        }
        else if ( rightVariablesFromData.size() == 1 )
        {
            rightVariableName = rightVariablesFromData.iterator()
                                                      .next();
        }

        if ( DeclarationUtilities.hasBaseline( declaration )
             && ProjectUtilities.hasVariableName( declaration.baseline()
                                                             .dataset() ) )
        {
            baselineVariableName = declaration.baseline()
                                              .dataset()
                                              .variable()
                                              .name();
        }
        else if ( baselineVariablesFromData.size() == 1 )
        {
            baselineVariableName = baselineVariablesFromData.iterator()
                                                            .next();
        }

        // One variable name for all? Allow.
        if ( Objects.nonNull( leftVariableName )
             && Objects.nonNull( rightVariableName )
             && ( !DeclarationUtilities.hasBaseline( declaration )
                  || Objects.nonNull( baselineVariableName ) ) )
        {
            LOGGER.debug( "Discovered one variable name for each data source." );

            variableNames = new VariableNames( leftVariableName,
                                               rightVariableName,
                                               baselineVariableName,
                                               covariateVariablesFromData );
        }
        // More than one for some, need to intersect
        else
        {
            variableNames = ProjectUtilities.getUniqueVariableNames( leftVariablesFromData,
                                                                     rightVariablesFromData,
                                                                     baselineVariablesFromData,
                                                                     covariateVariablesFromData,
                                                                     declaration );
        }

        // Perform additional validation checks
        ProjectUtilities.checkAutoDetectedVariableNames( declaration,
                                                         variableNames );

        ProjectUtilities.checkDeclaredAndIngestedVariablesAgree( declaration,
                                                                 leftVariablesFromData,
                                                                 rightVariablesFromData,
                                                                 baselineVariablesFromData,
                                                                 covariateVariablesFromData );

        return variableNames;
    }

    /**
     * Tests whether there is a desired timescale present that supports lenient upscaling for the specified side of
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
     * Filters from the input any ensemble member labels that are declared to be filtered.
     * @param labels the ensemble labels to filter
     * @param declaration the declaration
     * @param orientation the dataset orientation
     * @return the filtered members
     */

    static SortedSet<String> filter( SortedSet<String> labels,
                                     EvaluationDeclaration declaration,
                                     DatasetOrientation orientation )
    {
        switch ( orientation )
        {
            case LEFT ->
            {
                return ProjectUtilities.filter( labels, declaration.left()
                                                                   .ensembleFilter() );
            }
            case RIGHT ->
            {
                return ProjectUtilities.filter( labels, declaration.right()
                                                                   .ensembleFilter() );
            }
            case BASELINE ->
            {
                if ( !DeclarationUtilities.hasBaseline( declaration ) )
                {
                    throw new IllegalArgumentException( "The project does not contain a 'baseline' dataset." );
                }

                return ProjectUtilities.filter( labels, declaration.baseline()
                                                                   .dataset()
                                                                   .ensembleFilter() );
            }
            default ->
            {
                return labels;
            }
        }
    }

    /**
     * @param max the maximum value, inclusive
     * @return a series of integer labels between 1 and the maximum value, inclusive
     */

    static SortedSet<String> getSeries( int max )
    {
        SortedSet<String> labels = IntStream.range( 1, max + 1 )
                                            .boxed()
                                            .map( Object::toString )
                                            .collect( Collectors.toCollection( TreeSet::new ) );
        return Collections.unmodifiableSortedSet( labels );
    }

    /**
     * Interpolates missing declaration post-ingest.
     *
     * @param declaration the declaration
     * @param ingestResults the ingest results
     * @param variableNames the analyzed variable names
     * @param measurementUnit the analyzed measurement unit
     * @param timeScale the analyzed evaluation timescale, possibly null
     * @param features the features
     * @param featureGroups the feature groups
     * @return the augmented declaration
     * @throws NullPointerException if any required input is null
     */
    static EvaluationDeclaration interpolate( EvaluationDeclaration declaration,
                                              List<IngestResult> ingestResults,
                                              VariableNames variableNames,
                                              String measurementUnit,
                                              TimeScaleOuter timeScale,
                                              Set<FeatureTuple> features,
                                              Set<FeatureGroup> featureGroups )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( ingestResults );
        Objects.requireNonNull( variableNames );
        Objects.requireNonNull( measurementUnit );
        Objects.requireNonNull( features );
        Objects.requireNonNull( featureGroups );

        // Organize the data types by dataset orientation, including linked types
        Map<DatasetOrientation, Set<DataType>> dataTypes = new EnumMap<>( DatasetOrientation.class );
        for ( IngestResult result : ingestResults )
        {
            Set<DatasetOrientation> orientations = result.getDatasetOrientations();
            for ( DatasetOrientation next : orientations )
            {
                DataType type = result.getDataType();
                if ( Objects.nonNull( type ) )
                {
                    if ( dataTypes.containsKey( next ) )
                    {
                        dataTypes.get( next )
                                 .add( type );
                    }
                    else
                    {
                        Set<DataType> types = new HashSet<>();
                        types.add( type );
                        dataTypes.put( next, types );
                    }
                }
            }
        }

        Map<DatasetOrientation, Set<DataType>> filtered =
                dataTypes.entrySet()
                         .stream()
                         .filter( e -> e.getValue()
                                        .size() > 1 )
                         .collect( Collectors.toMap( Map.Entry::getKey,
                                                     Map.Entry::getValue ) );

        if ( !filtered.isEmpty() )
        {
            throw new DeclarationException( "Following data ingest, discovered more than one data 'type' in "
                                            + filtered.size()
                                            + " datasets, which is not allowed. The following datasets had "
                                            + "mixed data types: "
                                            + filtered
                                            + ". Please remove any mixed data types for each dataset and try "
                                            + "again." );
        }

        Map<DatasetOrientation, DataType> singletons =
                dataTypes.entrySet()
                         .stream()
                         .filter( e -> !e.getValue()
                                         .isEmpty() )
                         .collect( Collectors.toMap( Map.Entry::getKey, e -> e.getValue()
                                                                              .iterator()
                                                                              .next() ) );

        DataType leftType = singletons.get( DatasetOrientation.LEFT );
        DataType rightType = singletons.get( DatasetOrientation.RIGHT );
        DataType baselineType = singletons.get( DatasetOrientation.BASELINE );
        DataType covariateType = singletons.get( DatasetOrientation.COVARIATE );

        DataTypes types = new DataTypes( leftType,
                                         rightType,
                                         baselineType,
                                         covariateType );

        wres.statistics.generated.TimeScale innerTimeScale = null;
        if ( Objects.nonNull( timeScale ) )
        {
            innerTimeScale = timeScale.getTimeScale();
        }

        // If the ingested types differ from any existing types of there are conflicting types for a single
        // orientation, this will throw an exception
        declaration = DeclarationInterpolator.interpolate( declaration,
                                                           types,
                                                           variableNames,
                                                           measurementUnit,
                                                           innerTimeScale,
                                                           true );

        // Adjust the declaration to include the fully described features based on the ingested data
        Set<GeometryTuple> unwrappedFeatures = features.stream()
                                                       .map( FeatureTuple::getGeometryTuple )
                                                       .collect( Collectors.toUnmodifiableSet() );

        // Match and gather any offsets for the declared features using the ingest-augmented features
        boolean hasBaseline = DeclarationUtilities.hasBaseline( declaration );
        Map<GeometryTuple, Offset> declaredOffsets = ProjectUtilities.getOffsets( declaration );
        Map<GeometryTuple, Offset> offsets =
                ProjectUtilities.getOffsetsForMatchingFeatures( unwrappedFeatures,
                                                                declaredOffsets,
                                                                hasBaseline );

        Features dataFeatures = new Features( unwrappedFeatures, offsets );
        FeatureGroups dataFeatureGroups
                = FeatureGroupsBuilder.builder()
                                      .geometryGroups( featureGroups.stream()
                                                                    .map( FeatureGroup::getGeometryGroup )
                                                                    // Non-singletons only
                                                                    .filter( g -> g.getGeometryTuplesList()
                                                                                   .size()
                                                                                  > 1 )
                                                                    .collect( Collectors.toSet() ) )
                                      .build();
        declaration = EvaluationDeclarationBuilder.builder( declaration )
                                                  .features( dataFeatures )
                                                  .featureGroups( dataFeatureGroups )
                                                  .build();

        return declaration;
    }

    /**
     * Performs post-ingest validation that does not depend on the precise implementation details of a {@link Project}.
     * Validation that depends on the implementation details of a {@link Project} should be performed within that
     * implementation, leveraging this method for implementation-invariant validation.
     *
     * @param project the project
     * @throws DeclarationException if the declaration and ingest are inconsistent
     */
    static void validate( Project project )
    {
        Objects.requireNonNull( project );

        // Validate the declaration in relation to the interpolated data types and other analyzed information
        LOGGER.debug( "Performing post-ingest validation of the declaration" );
        DeclarationValidator.validatePostDataIngest( project.getDeclaration() );

        ProjectUtilities.validateThresholdsForFeatureGroups( project.getFeatureGroups(), project.getFeatures() );
    }

    /**
     * Returns the named covariate dataset from the supplied declaration.
     * @param declaration the declaration
     * @param variableName the variable name
     * @return the covariate dataset
     * @throws IllegalArgumentException if the covariate dataset does not exist
     * @throws NullPointerException if any input is null
     */
    static Dataset getCovariateDatset( EvaluationDeclaration declaration, String variableName )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( variableName );

        return declaration.covariates()
                          .stream()
                          .map( CovariateDataset::dataset )
                          .filter( c -> Objects.nonNull( c.variable() )
                                        && Objects.nonNull( c.variable()
                                                             .name() )
                                        && c.variable()
                                            .name()
                                            .equals( variableName ) )
                          .findFirst()
                          .orElseThrow( () -> new IllegalArgumentException( "Could not find a covariate dataset with "
                                                                            + "a variable name of '"
                                                                            + variableName
                                                                            + "'." ) );
    }

    /**
     * Checks that some time-series data is present for the features associated with the covariate feature
     * authority when inspecting the corresponding features for the dataset whose feature authority matches that
     * of the covariate. If no data is discovered when using the covariate features and the assumed authority, an
     * exception is thrown, indicating that the authority may not be correct.
     *
     * @param covariate the covariate dataset
     * @param projectFeatures the project features, post-ingest
     * @param ingestedCovariateFeatures the ingested covariate feature names
     * @return the covariate features with time-series data for the dataset that has the same feature authority
     * @throws NoProjectDataException if the covariate feature names do not select any time-series data
     */
    static Set<Feature> covariateFeaturesSelectSomeData( CovariateDataset covariate,
                                                         Set<FeatureTuple> projectFeatures,
                                                         Set<Feature> ingestedCovariateFeatures )
    {
        DatasetOrientation orientation = covariate.featureNameOrientation();

        Objects.requireNonNull( orientation,
                                "Failed to identify the feature name orientation for the covariate dataset: "
                                + covariate
                                + "." );
        Objects.requireNonNull( covariate.dataset(), "Expected a covariate dataset." );
        Objects.requireNonNull( covariate.dataset()
                                         .variable(), "Expected a covariate variable." );
        Objects.requireNonNull( covariate.dataset()
                                         .variable()
                                         .name(), "Expected a covariate variable name." );

        String covariateName = covariate.dataset()
                                        .variable()
                                        .name();

        Objects.requireNonNull( covariate.featureNameOrientation(), "Could not find the orientation of the "
                                                                    + "feature names associated with the covariate "
                                                                    + "dataset whose variable name is '"
                                                                    + covariateName
                                                                    + "'." );

        Set<String> ingestedNames = ingestedCovariateFeatures.stream()
                                                             .map( Feature::getName )
                                                             .collect( Collectors.toSet() );
        Set<Feature> matchingFeatures;

        switch ( covariate.featureNameOrientation() )
        {
            case LEFT -> matchingFeatures = ProjectUtilities.getMatchingFeatures( ingestedCovariateFeatures,
                                                                                  projectFeatures,
                                                                                  FeatureTuple::getLeft );
            case RIGHT -> matchingFeatures = ProjectUtilities.getMatchingFeatures( ingestedCovariateFeatures,
                                                                                   projectFeatures,
                                                                                   FeatureTuple::getRight );
            case BASELINE -> matchingFeatures = ProjectUtilities.getMatchingFeatures( ingestedCovariateFeatures,
                                                                                      projectFeatures,
                                                                                      FeatureTuple::getBaseline );
            default -> throw new IllegalStateException( "Unrecognized dataset orientation, '"
                                                        + covariate.featureNameOrientation()
                                                        + "'." );
        }

        // No covariate feature names with corresponding feature names for time-series data?
        if ( matchingFeatures.isEmpty() )
        {
            throw new NoProjectDataException( "Could not find non-covariate time-series data for any of the feature "
                                              + "names associated with covariate '"
                                              + covariateName
                                              + "'. These feature names were interpreted with the feature "
                                              + "authority of the '"
                                              + covariate.featureNameOrientation()
                                              + "' data. If this is incorrect, please explicitly and accurately "
                                              + "declare the 'feature_authority' for this covariate, as well as "
                                              + "the 'feature_authority' of the corresponding 'observed', "
                                              + "'predicted' or 'baseline' dataset (each covariate must have the "
                                              + "same feature authority as one of these datasets). Otherwise, "
                                              + "ensure that one or more of the features to evaluate has some "
                                              + "covariate data or remove the covariate entirely. The feature "
                                              + "names with data (and whose feature authority should be declared) "
                                              + "for this covariate are: "
                                              + ingestedNames
                                              + "." );
        }

        return matchingFeatures;
    }

    /**
     * Inspects the declaration for feature-specific offsets in all contexts and returns them.
     * @param declaration the declaration, not null
     * @return the offsets
     * @throws NullPointerException if the declaration is null
     */
    static Map<GeometryTuple, Offset> getOffsets( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        Map<GeometryTuple, Offset> offsets = new HashMap<>();


        if ( Objects.nonNull( declaration.features() ) )
        {
            offsets.putAll( declaration.features()
                                       .offsets() );
        }

        if ( Objects.nonNull( declaration.featureGroups() ) )
        {
            offsets.putAll( declaration.featureGroups()
                                       .offsets() );
        }

        return Collections.unmodifiableMap( offsets );
    }

    /**
     * Finds the offsets for features with matching feature names.
     *
     * @param toMatch the feature names for which offsets should be matched
     * @param toSearch the offsets to search
     * @param hasBaseline whether the evaluation contains a baseline dataset
     * @return the offsets for matching features
     */
    private static Map<GeometryTuple, Offset> getOffsetsForMatchingFeatures( Set<GeometryTuple> toMatch,
                                                                             Map<GeometryTuple, Offset> toSearch,
                                                                             boolean hasBaseline )
    {
        Map<GeometryTuple, Offset> offsets = new HashMap<>();

        if ( Objects.nonNull( toSearch ) )
        {
            for ( GeometryTuple next : toMatch )
            {
                Optional<Map.Entry<GeometryTuple, Offset>> matched =
                        toSearch.entrySet()
                                .stream()
                                .filter( f -> Objects.equals( next.getLeft()
                                                                  .getName(), f.getKey()
                                                                               .getLeft()
                                                                               .getName() )
                                              && Objects.equals( next.getRight()
                                                                     .getName(),
                                                                 f.getKey()
                                                                  .getRight()
                                                                  .getName() )
                                              && ( !hasBaseline
                                                   || Objects.equals( next.getBaseline()
                                                                          .getName(),
                                                                      f.getKey()
                                                                       .getBaseline()
                                                                       .getName() ) ) )
                                .findFirst();

                matched.ifPresent( geometryTupleOffsetEntry
                                           -> offsets.put( next, geometryTupleOffsetEntry.getValue() ) );
            }
        }

        return Collections.unmodifiableMap( offsets );
    }

    /**
     * Retrieves the matching features using the inputs.
     * @param ingestedCovariateFeatures the ingested covariate features to filter
     * @param projectFeatures the project features to match with the ingested covariate features
     * @param getter the getter to retrieve a feature from a tuple
     * @return the matching features
     */
    private static Set<Feature> getMatchingFeatures( Set<Feature> ingestedCovariateFeatures,
                                                     Set<FeatureTuple> projectFeatures,
                                                     Function<FeatureTuple, Feature> getter )
    {
        return ingestedCovariateFeatures.stream()
                                        .filter( f -> projectFeatures.stream()
                                                                     .anyMatch( g -> Objects.equals( getter.apply( g )
                                                                                                           .getName(),
                                                                                                     f.getName() ) ) )
                                        .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Filters from the input any ensemble member labels that are declared to be filtered.
     * @param labels the ensemble labels to filter
     * @param ensembleFilter the ensemble filter, possibkly null
     * @return the filtered members
     */

    private static SortedSet<String> filter( SortedSet<String> labels,
                                             EnsembleFilter ensembleFilter )
    {
        Objects.requireNonNull( labels );

        if ( Objects.isNull( ensembleFilter ) )
        {
            return labels;
        }

        SortedSet<String> labelsToFilter = new TreeSet<>( labels );
        if ( ensembleFilter.exclude() )
        {
            labelsToFilter.removeAll( ensembleFilter.members() );
        }
        else
        {
            labelsToFilter.retainAll( ensembleFilter.members() );
        }

        return Collections.unmodifiableSortedSet( labelsToFilter );
    }

    /**
     * Validates the covariate variable names as they relate to the ingested data.
     * @param declaration the declaration
     * @param covariates the covariates
     * @throws DeclarationException if the declaration is invalid
     */
    private static void covariateVariableNamesArePresentWhenRequired( EvaluationDeclaration declaration,
                                                                      Set<String> covariates )
    {
        if ( !declaration.covariates()
                         .isEmpty()
             && covariates.isEmpty() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE
                                            + "name(s) of the "
                                            + DatasetOrientation.COVARIATE
                                            + " variables from the data, failed to identify any "
                                            + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                            + "name for each "
                                            + DatasetOrientation.COVARIATE
                                            + " dataset to disambiguate." );

        }

        // Multiple cpvariates declared, but some variable names not declared. This should be validated at declaration
        // time also, but is reinforced here
        if ( declaration.covariates()
                        .size() > 1
             && declaration.covariates()
                           .stream()
                           .anyMatch( c -> Objects.isNull( c.dataset()
                                                            .variable() )
                                           || Objects.isNull( c.dataset()
                                                               .variable()
                                                               .name() ) ) )
        {
            throw new DeclarationException( "When declaring two or more 'covariates', the 'name' of each 'variable' "
                                            + "must be declared explicitly, but one or more of the 'covariates' had no "
                                            + "declared 'variable' and 'name'. Please clarify the 'name' of the "
                                            + "'variable' for each covariate and try again." );
        }

        // Different number of covariate names than datasets
        if ( declaration.covariates()
                        .size() != covariates.size() )
        {
            throw new DeclarationException( WHILE_ATTEMPTING_TO_DETECT_THE
                                            + "name(s) of the "
                                            + DatasetOrientation.COVARIATE
                                            + " variables from the data, discovered a different number of names than "
                                            + "datasets, which is not allowed. Please declare each "
                                            + DatasetOrientation.COVARIATE
                                            + " variable name explicitly to disambiguate. The number of "
                                            + DatasetOrientation.COVARIATE
                                            + " datasets is: "
                                            + declaration.covariates()
                                                         .size()
                                            + ". The possible variable names are: "
                                            + covariates
                                            + "." );
        }
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
     * Attempts to find a unique variable name across the datasets that have several possibilities.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @param covariates the covariate variable names
     * @param declaration the declaration
     * @throws DeclarationException if a unique name could not be discovered
     */

    private static VariableNames getUniqueVariableNames( Set<String> left,
                                                         Set<String> right,
                                                         Set<String> baseline,
                                                         Set<String> covariates,
                                                         EvaluationDeclaration declaration )
    {
        LOGGER.debug( "Discovered several variable names for the data sources. Will attempt to intersect them and "
                      + "discover one. The LEFT variable names are {}, the RIGHT variable names are {}, the "
                      + "BASELINE variable names are {} and the COVARIATE variable names are {}.",
                      left,
                      right,
                      baseline,
                      covariates );

        // No intersection needed, short-circuit
        if ( left.size() == 1
             && right.size() == 1
             && ( !DeclarationUtilities.hasBaseline( declaration ) || baseline.size() == 1 ) )
        {
            String leftVariableName = left.iterator()
                                          .next();
            String rightVariableName = right.iterator()
                                            .next();
            String baselineVariableName = null;

            if ( DeclarationUtilities.hasBaseline( declaration ) )
            {
                baselineVariableName = declaration.baseline()
                                                  .dataset()
                                                  .variable()
                                                  .name();
            }

            return new VariableNames( leftVariableName, rightVariableName, baselineVariableName, covariates );
        }

        return ProjectUtilities.getVariableNamesFromIntersection( left, right, baseline, covariates, declaration );
    }

    /**
     * Attempts to find a unique name across the datasets that have several possibilities by intersecting these
     * possibilities and identifying the unique name among them.
     *
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @param covariates the covariate variable names
     * @param declaration the declaration
     * @throws DeclarationException if a unique name could not be discovered
     */

    private static VariableNames getVariableNamesFromIntersection( Set<String> left,
                                                                   Set<String> right,
                                                                   Set<String> baseline,
                                                                   Set<String> covariates,
                                                                   EvaluationDeclaration declaration )
    {
        Set<String> intersection = new HashSet<>();

        if ( !ProjectUtilities.hasVariableName( declaration.left() ) )
        {
            intersection.addAll( left );
        }

        if ( !ProjectUtilities.hasVariableName( declaration.right() ) )
        {
            if ( intersection.isEmpty() )
            {
                intersection.addAll( right );
            }
            else
            {
                intersection.retainAll( right );
            }
        }

        if ( DeclarationUtilities.hasBaseline( declaration )
             && Objects.isNull( declaration.baseline()
                                           .dataset()
                                           .variable()
                                           .name() ) )
        {
            if ( intersection.isEmpty() )
            {
                intersection.addAll( baseline );
            }
            else
            {
                intersection.retainAll( baseline );
            }
        }

        return ProjectUtilities.getVariableNamesFromIntersection( left,
                                                                  right,
                                                                  baseline,
                                                                  covariates,
                                                                  intersection,
                                                                  declaration );
    }

    /**
     * Attempts to find a unique name across the datasets that have several possibilities by intersecting these
     * possibilities and identifying the unique name among them.
     *
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @param covariates the covariate variable names
     * @param intersection the intersection
     * @param declaration the declaration
     * @throws DeclarationException if a unique name could not be discovered
     */

    private static VariableNames getVariableNamesFromIntersection( Set<String> left,
                                                                   Set<String> right,
                                                                   Set<String> baseline,
                                                                   Set<String> covariates,
                                                                   Set<String> intersection,
                                                                   EvaluationDeclaration declaration )
    {
        if ( intersection.size() == 1 )
        {
            String uniqueVariableName = intersection.iterator()
                                                    .next();

            String leftVariableName = uniqueVariableName;
            String rightVariableName = uniqueVariableName;
            String baselineVariableName = null;

            // If the name is declared, use that instead
            if ( DeclarationUtilities.hasBaseline( declaration ) )
            {
                if ( Objects.nonNull( declaration.baseline()
                                                 .dataset()
                                                 .variable()
                                                 .name() ) )
                {
                    baselineVariableName = declaration.baseline()
                                                      .dataset()
                                                      .variable()
                                                      .name();
                }
                else
                {
                    baselineVariableName = uniqueVariableName;
                }
            }

            if ( Objects.nonNull( declaration.left()
                                             .variable()
                                             .name() ) )
            {
                leftVariableName = declaration.left()
                                              .variable()
                                              .name();
            }

            if ( Objects.nonNull( declaration.right()
                                             .variable()
                                             .name() ) )
            {
                rightVariableName = declaration.right()
                                               .variable()
                                               .name();
            }

            LOGGER.debug( "After intersecting the variable names, discovered one variable name to evaluate, {}.",
                          uniqueVariableName );

            return new VariableNames( leftVariableName,
                                      rightVariableName,
                                      baselineVariableName,
                                      covariates );
        }

        // No unique name
        throw new DeclarationException( "While attempting to auto-detect "
                                        + "the variable to evaluate, failed to identify a "
                                        + "single variable name that is common to all data "
                                        + "sources. Discovered 'observed' variable names of "
                                        + left
                                        + ", 'predicted' variable names of "
                                        + right
                                        + " and 'baseline' variable names of "
                                        + baseline
                                        + ". Please declare an explicit variable name for "
                                        + "each required data source to disambiguate." );
    }

    /**
     * Generates the multi-feature groups for evaluation.
     * @param declaration the declaration
     * @param groupedFeaturesWithData the groped features with data
     * @return the multi-feature-groups
     */
    private static Set<FeatureGroup> getMultiFeatureGroups( EvaluationDeclaration declaration,
                                                            Set<FeatureTuple> groupedFeaturesWithData )
    {
        Set<GeometryGroup> declaredGroups =
                ProjectUtilities.getAndValidateDeclaredFeatureGroups( groupedFeaturesWithData,
                                                                      declaration );

        AtomicInteger groupNumber = new AtomicInteger( 1 ); // For naming when no name is present

        Set<FeatureGroup> grouped = new HashSet<>();

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
                grouped.add( newGroup );
                LOGGER.debug( "Discovered a new feature group, {}.", newGroup );

                if ( !noDataTuples.isEmpty()
                     && LOGGER.isWarnEnabled() )
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

        return Collections.unmodifiableSet( grouped );
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
                                            + " feature group(s) in the project declaration, but could not "
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
     * @param nextGroup the next feature group
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


    /**
     * @param featureGroups the feature groups
     * @param featuresWithExplicitThresholds features with explicit thresholds (not the implicit "all data" threshold)
     */

    private static void validateThresholdsForFeatureGroups( Set<FeatureGroup> featureGroups,
                                                            Set<FeatureTuple> featuresWithExplicitThresholds )
    {
        Objects.requireNonNull( featureGroups );
        Objects.requireNonNull( featuresWithExplicitThresholds );

        // Log a warning about any discrepancies between features with thresholds and features to evaluate
        if ( LOGGER.isWarnEnabled() )
        {
            Map<String, Set<String>> missing = new HashMap<>();

            // Check that every group has one or more thresholds for every tuple, else warn
            for ( FeatureGroup nextGroup : featureGroups )
            {
                if ( nextGroup.getFeatures()
                              .size() > 1
                     && !featuresWithExplicitThresholds.containsAll( nextGroup.getFeatures() ) )
                {
                    Set<FeatureTuple> missingFeatures = new HashSet<>( nextGroup.getFeatures() );
                    missingFeatures.removeAll( featuresWithExplicitThresholds );

                    // Show abbreviated information only
                    missing.put( nextGroup.getName(),
                                 missingFeatures.stream()
                                                .map( FeatureTuple::toStringShort )
                                                .collect( Collectors.toSet() ) );
                }
            }

            // Warn about groups without thresholds, which will be skipped
            if ( !missing.isEmpty() )
            {
                LOGGER.warn( "While correlating thresholds with the features contained in feature groups, "
                             + "discovered {} feature groups that did not have thresholds for every feature within the "
                             + "group. These groups will be evaluated, but the grouped statistics will not include the "
                             + "pairs associated with the features that have missing thresholds (for the thresholds "
                             + "that are missing). By default, the \"all data\" threshold is added to every feature "
                             + "and the statistics for this threshold will not be impacted. The features with missing "
                             + "thresholds and their associated feature groups are: {}.",
                             missing.size(),
                             missing );
            }
        }
    }

    /**
     * @param dataset the dataset
     * @return whether the dataset has a variable name
     */
    private static boolean hasVariableName( Dataset dataset )
    {
        return Objects.nonNull( dataset )
               && Objects.nonNull( dataset.variable() )
               && Objects.nonNull( dataset.variable()
                                          .name() );
    }
}