package wres.io.project;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DataTypes;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.DeclarationValidator;
import wres.config.yaml.VariableNames;
import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.TimeScaleLenience;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
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
     * Small value class to hold feature group information.
     * @param featureGroups the feature groups
     * @param doNotPublish a set of feature groups for which statistics should not be published
     * @author James Brown
     */

    record FeatureGroupsPlus( Set<FeatureGroup> featureGroups, Set<FeatureGroup> doNotPublish ) {}

    /**
     * Creates feature groups from the inputs.
     * @param singletonsWithData the singleton features that have time-series data
     * @param groupedFeaturesWithData the features within multi-feature groups that have time-series data
     * @param declaration the declaration
     * @param projectId a project identifier to help with messaging
     * @return the feature groups
     */

    static FeatureGroupsPlus getFeatureGroups( Set<FeatureTuple> singletonsWithData,
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

        LOGGER.info( "Discovered {} feature groups with data on both the left and right sides (statistics "
                     + "should be expected for this many feature groups at most).",
                     innerGroups.size() );

        // When summary statistics are required for feature groups, add the singleton members regardless of whether they
        // were declared, because the statistics are needed for every singleton feature
        // If summary statistics are required for feature groups, ensure that all singletons are present too
        Set<FeatureGroup> doNotPublish = new HashSet<>();
        if ( declaration.summaryStatistics()
                        .stream()
                        .anyMatch( s -> s.getDimension() == SummaryStatistic.StatisticDimension.FEATURE_GROUP ) )
        {
            groupedFeaturesWithData.forEach( f -> doNotPublish.add( FeatureGroup.of( MessageFactory.getGeometryGroup( f.toStringShort(),
                                                                                                                      f ) ) ) );
            // Remove all the declared singletons
            doNotPublish.removeAll( singletonGroups );
            LOGGER.debug( "Added the following singleton feature groups to calculate summary statistics only: {}",
                          doNotPublish );
            innerGroups.addAll( doNotPublish );
        }

        return new FeatureGroupsPlus( Collections.unmodifiableSet( innerGroups ),
                                      Collections.unmodifiableSet( doNotPublish ) );
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
            LOGGER.warn( "The left and right variable names were auto-detected, but the detected variable names do not "
                         + "match. The left name is {} and the right name is {}. Proceeding to pair and evaluate these "
                         + "variables. If this is unexpected behavior, please add explicit variable declaration for "
                         + "both the left and right data and try again.",
                         leftVariableName,
                         rightVariableName );
        }

        if ( DeclarationUtilities.hasBaseline( declaration )
             && ( Objects.isNull( declaredLeftVariableName )
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
        String declaredLeftVariableName = DeclarationUtilities.getVariableName( declaration.left() );
        String declaredRightVariableName = DeclarationUtilities.getVariableName( declaration.right() );
        String declaredBaselineVariableName = null;

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            declaredBaselineVariableName = DeclarationUtilities.getVariableName( declaration.baseline()
                                                                                            .dataset() );
        }

        // Where a name has been declared and the ingested names are available, the declared name must be among them
        String leftError = "";
        String rightError = "";
        String baselineError = "";
        StringBuilder covariateError = new StringBuilder();

        String start = "    - ";
        if ( Objects.nonNull( declaredLeftVariableName )
             && !left.isEmpty()
             && !left.contains( declaredLeftVariableName ) )
        {
            leftError += System.lineSeparator() + start;
            leftError += ProjectUtilities.getVariableErrorMessage( declaredLeftVariableName,
                                                                   left,
                                                                   DatasetOrientation.LEFT );
        }
        if ( Objects.nonNull( declaredRightVariableName )
             && !right.isEmpty()
             && !right.contains( declaredRightVariableName ) )
        {
            rightError += System.lineSeparator() + start;
            rightError += ProjectUtilities.getVariableErrorMessage( declaredRightVariableName,
                                                                    right,
                                                                    DatasetOrientation.RIGHT );
        }
        if ( Objects.nonNull( declaredBaselineVariableName )
             && !baseline.isEmpty()
             && !baseline.contains( declaredBaselineVariableName ) )
        {
            baselineError += System.lineSeparator() + start;
            baselineError += ProjectUtilities.getVariableErrorMessage( declaredRightVariableName,
                                                                       baseline,
                                                                       DatasetOrientation.BASELINE );
        }

        // Iterate through the covariates
        for ( CovariateDataset covariate : declaration.covariates() )
        {
            if ( Objects.nonNull( covariate.dataset()
                                           .variable() )
                 && Objects.nonNull( covariate.dataset()
                                              .variable()
                                              .name() )
                 && !covariates.contains( covariate.dataset()
                                                   .variable()
                                                   .name() ) )
            {
                covariateError.append( System.lineSeparator() )
                              .append( start );
                covariateError.append( ProjectUtilities.getVariableErrorMessage( covariate.dataset()
                                                                                          .variable()
                                                                                          .name(),
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
     * Generates an error message when a declared variable is not among the ingested options.
     * @param variableName the variable name
     * @param nameOptions the name options
     * @param orientation the dataset orientation
     * @return the error message
     */
    private static String getVariableErrorMessage( String variableName,
                                                   Set<String> nameOptions,
                                                   DatasetOrientation orientation )
    {
        String article = "the";
        if ( orientation == DatasetOrientation.COVARIATE )
        {
            article = "a";
        }

        return "The declared 'name' of the 'variable' for "
               + article
               + " '"
               + orientation
               + "' dataset was '"
               + variableName
               + "', but this could not be found among the variable names of the ingested data sources, which "
               + "were: "
               + nameOptions
               + ".";
    }

    /**
     * Attempts to get the variable names from a set of left, right and baseline names.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @param covariates the covariate variable names
     * @return the variable names
     * @throws DeclarationException is the variable names could not be determined or are otherwise invalid
     */

    static VariableNames getVariableNames( EvaluationDeclaration declaration,
                                           Set<String> left,
                                           Set<String> right,
                                           Set<String> baseline,
                                           Set<String> covariates )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );
        Objects.requireNonNull( baseline );
        Objects.requireNonNull( covariates );

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

        if ( DeclarationUtilities.hasBaseline( declaration )
             && baseline.isEmpty() )
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
        ProjectUtilities.covariateVariableNamesArePresentWhenRequired( declaration, covariates );

        // Further determine the variable names based on ingested sources
        VariableNames variableNames;

        // One variable name for all? Allow. 
        if ( left.size() == 1
             && right.size() == 1
             && ( baseline.isEmpty()
                  || baseline.size() == 1 ) )
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

            LOGGER.debug( "Discovered one variable name for each data source." );

            variableNames = new VariableNames( leftVariableName,
                                               rightVariableName,
                                               baselineVariableName,
                                               covariates );
        }
        // More than one for some, need to intersect
        else
        {
            variableNames = ProjectUtilities.getVariableNamesFromIntersection( left,
                                                                               right,
                                                                               baseline,
                                                                               covariates,
                                                                               declaration );
        }

        // Perform additional validation checks
        ProjectUtilities.checkAutoDetectedVariableNames( declaration,
                                                         variableNames );

        ProjectUtilities.checkDeclaredAndIngestedVariablesAgree( declaration,
                                                                 left,
                                                                 right,
                                                                 baseline,
                                                                 covariates );

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
     * @return the augmented declaration
     * @throws NullPointerException if any required input is null
     */
    static EvaluationDeclaration interpolate( EvaluationDeclaration declaration,
                                              List<IngestResult> ingestResults,
                                              VariableNames variableNames,
                                              String measurementUnit,
                                              TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( ingestResults );
        Objects.requireNonNull( variableNames );
        Objects.requireNonNull( measurementUnit );

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
        if( Objects.nonNull( timeScale ) )
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

        return declaration;
    }

    /**
     * Validates the declaration, post-ingest.
     * @param declaration the declaration
     * @throws DeclarationException if the declaration and ingest are inconsistent
     */
    static void validate( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        // Validate the declaration in relation to the interpolated data types and other analyzed information
        LOGGER.debug( "Performing post-ingest validation of the declaration" );
        DeclarationValidator.validatePostDataIngest( declaration );
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
                                            + "dataset to disambiguate." );

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
     * Attempts to find a unique name by intersecting the left, right and baseline names.
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
        LOGGER.debug( "Discovered several variable names for the data sources. Will attempt to intersect them and "
                      + "discover one. The LEFT variable names are {}, the RIGHT variable names are {}, the "
                      + "BASELINE variable names are {} and the COVARIATE variable names are {}.",
                      left,
                      right,
                      baseline,
                      covariates );

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

            return new VariableNames( leftVariableName,
                                      leftVariableName,
                                      baselineVariableName,
                                      covariates );
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
