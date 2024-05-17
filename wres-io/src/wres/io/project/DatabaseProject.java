package wres.io.project;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import net.jcip.annotations.Immutable;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.TimeScale;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.io.NoProjectDataException;
import wres.datamodel.DataProvider;
import wres.io.database.DatabaseOperations;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.Features;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.config.yaml.VariableNames;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * An implementation of the {@link Project} for an evaluation performed using a database.
 */
@Immutable
public class DatabaseProject implements Project
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseProject.class );
    private static final String SELECT_1 = "SELECT 1";
    private static final String PROJECT_ID = "project_id";
    private final EvaluationDeclaration declaration;
    private final Database database;

    /** The overall hash for the data sources used in the project. */
    private final String hash;

    private final long projectId;

    /** The measurement unit, which is the declared unit, if available, else the most commonly occurring unit among the
     * project sources, with a preference for the mostly commonly occurring right-sided source unit. See 
     * {@link ProjectScriptGenerator#createUnitScript(Database, long)}}. */

    private final String measurementUnit;

    /** The set of all features pertaining to the project. */
    private final Set<FeatureTuple> features;

    /** The covariate features by variable name. **/
    private final Map<String, Set<Feature>> covariateFeatures;

    /** The feature groups related to the project. */
    private final Set<FeatureGroup> featureGroups;

    /** The singleton feature groups for which statistics should not be published, if any. */
    private final Set<FeatureGroup> doNotPublish;

    /** The left-ish variable to evaluate. */
    private final String leftVariable;

    /** The right-ish variable to evaluate. */
    private final String rightVariable;

    /** The baseline-ish variable to evaluate. */
    private final String baselineVariable;

    /** The desired timescale. */
    private final TimeScaleOuter desiredTimeScale;

    private final boolean leftUsesGriddedData;
    private final boolean rightUsesGriddedData;
    private final boolean baselineUsesGriddedData;
    private final boolean covariatesUseGriddedData;

    /**
     * Creates an instance.
     * @param database the database
     * @param caches the database ORMs/caches
     * @param griddedFeatures the gridded features cache, if required
     * @param declaration the project declaration
     * @param ingestResults the ingest results
     * @throws SQLException if the project could not be created
     */

    public DatabaseProject( Database database,
                            DatabaseCaches caches,
                            GriddedFeatures griddedFeatures,
                            EvaluationDeclaration declaration,
                            List<IngestResult> ingestResults ) throws SQLException
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( ingestResults );

        this.database = database;
        IngestIds ingestIds = DatabaseProject.getIngestIds( ingestResults );

        this.hash = DatabaseProject.getTopHashOfIngestedSources( ingestIds, this.database );

        // Finally, save the project in the database and prepare the remainder of the internal state
        this.projectId = this.save( ingestIds, this.hash, declaration.label() );

        LOGGER.info( "Validating the project and loading preliminary metadata..." );

        // Set the gridded data usage status.
        this.leftUsesGriddedData = this.getUsesGriddedData( DatasetOrientation.LEFT );
        this.rightUsesGriddedData = this.getUsesGriddedData( DatasetOrientation.RIGHT );
        this.baselineUsesGriddedData = this.getUsesGriddedData( DatasetOrientation.BASELINE );
        this.covariatesUseGriddedData = this.getUsesGriddedData( DatasetOrientation.COVARIATE );

        // Set the measurement unit
        this.measurementUnit = this.getAnalyzedMeasurementUnit( declaration, this.projectId );

        ProjectUtilities.FeatureSets featureSets = this.getFeaturesAndFeatureGroups( this.projectId,
                                                                                     declaration,
                                                                                     caches,
                                                                                     this.leftUsesGriddedData
                                                                                     || this.rightUsesGriddedData,
                                                                                     griddedFeatures );
        this.features = featureSets.features();
        this.featureGroups = featureSets.featureGroups();
        this.doNotPublish = featureSets.doNotPublish();

        // Determine and set the variables to evaluate
        VariableNames variableNames = this.getVariablesToEvaluate( declaration, this.projectId );
        this.leftVariable = variableNames.leftVariableName();
        this.rightVariable = variableNames.rightVariableName();
        this.baselineVariable = variableNames.baselineVariableName();

        // Set the desired timescale
        this.desiredTimeScale = this.getDesiredTimeScale( declaration, this.projectId );

        // Interpolate the declaration and set it
        this.declaration = ProjectUtilities.interpolate( declaration,
                                                         ingestResults,
                                                         variableNames,
                                                         this.measurementUnit,
                                                         this.desiredTimeScale,
                                                         this.features,
                                                         this.featureGroups );

        // Set the covariate features
        this.covariateFeatures = this.getCovariateFeatures( this.declaration.covariates(),
                                                            this.projectId,
                                                            this.features,
                                                            caches.getFeaturesCache() );

        // Validate any ensemble conditions in the declaration
        this.validateEnsembleConditions( this.declaration, this.projectId );

        // Perform validation that requires the API only, no implementation details
        ProjectUtilities.validate( this );

        LOGGER.info( "Project validation and metadata loading is complete." );
    }

    @Override
    public EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
    }

    /**
     * @return the measurement unit, which is either the declared unit or the analyzed unit, but possibly null
     * @throws DataAccessException if the measurement unit could not be determined
     * @throws IllegalArgumentException if the project identity is required and undefined
     */

    @Override
    public String getMeasurementUnit()
    {
        return this.measurementUnit;
    }

    /**
     * Returns the desired timescale. In order of availability, this is:
     *
     * <ol>
     * <li>The desired time scale provided on construction;</li>
     * <li>The Least Common Scale (LCS) computed from the input data; or</li>
     * <li>The LCS computed from the <code>existingTimeScale</code> provided in the input declaration.</li>
     * </ol>
     *
     * The LCS is the smallest common multiple of the time scales associated with every ingested dataset for a given 
     * project, variable and feature. The LCS is computed from all sides of a pairing (left, right and baseline) 
     * collectively. 
     *
     * @return the desired timescale or null if unknown
     * @throws DataAccessException if the existing time scales could not be obtained from the database
     */

    @Override
    public TimeScaleOuter getDesiredTimeScale()
    {
        return this.desiredTimeScale;
    }

    @Override
    public boolean isUpscalingLenient( DatasetOrientation orientation )
    {
        return ProjectUtilities.isUpscalingLenient( orientation,
                                                    this.getDeclaration()
                                                        .timeScale(),
                                                    this.getDeclaration()
                                                        .rescaleLenience() );
    }

    @Override
    public Set<FeatureTuple> getFeatures()
    {
        if ( Objects.isNull( this.features ) )
        {
            throw new IllegalStateException( "The features have not been set." );
        }

        return this.features;
    }

    @Override
    public Set<Feature> getCovariateFeatures( String variableName )
    {
        Objects.requireNonNull( variableName );

        return this.covariateFeatures.get( variableName );
    }

    @Override
    public Set<FeatureGroup> getFeatureGroups()
    {
        if ( Objects.isNull( this.featureGroups ) )
        {
            throw new IllegalStateException( "The feature groups have not been set." );
        }

        return this.featureGroups;
    }

    @Override
    public Set<FeatureGroup> getFeatureGroupsForWhichStatisticsShouldNotBePublished()
    {
        if ( Objects.isNull( this.doNotPublish ) )
        {
            throw new IllegalStateException( "The feature groups used only for summary statistics have not been set." );
        }

        return this.doNotPublish;
    }

    @Override
    public SortedSet<String> getEnsembleLabels( DatasetOrientation orientation )
    {
        Objects.requireNonNull( orientation );

        DataScripter script = ProjectScriptGenerator.createEnsembleLabelScript( this.getDatabase(),
                                                                                this.getId(),
                                                                                orientation );
        try ( DataProvider provider = script.getData() )
        {
            // Labels are always present in the database, even if only defaults (i.e., a zero-indexed series), as
            // defined in the relevant ingest class
            SortedSet<String> labels = new TreeSet<>();
            while ( provider.next() )
            {
                String label = provider.getString( "ensemble_name" );
                labels.add( label );
            }

            SortedSet<String> unmodifiable = Collections.unmodifiableSortedSet( labels );


            return ProjectUtilities.filter( unmodifiable, this.getDeclaration(), orientation );
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While attempting to determine whether gridded data were ingested.", e );
        }
    }

    @Override
    public Duration getEarliestAnalysisDuration()
    {
        return DeclarationUtilities.getEarliestAnalysisDuration( this.getDeclaration() );
    }

    @Override
    public Duration getLatestAnalysisDuration()
    {
        return DeclarationUtilities.getLatestAnalysisDuration( this.getDeclaration() );
    }

    @Override
    public MonthDay getStartOfSeason()
    {
        return DeclarationUtilities.getStartOfSeason( this.getDeclaration() );
    }

    @Override
    public MonthDay getEndOfSeason()
    {
        return DeclarationUtilities.getEndOfSeason( this.getDeclaration() );
    }

    @Override
    public boolean usesGriddedData( DatasetOrientation orientation )
    {
        return switch ( orientation )
        {
            case LEFT -> this.leftUsesGriddedData;
            case RIGHT -> this.rightUsesGriddedData;
            case BASELINE -> this.baselineUsesGriddedData;
            case COVARIATE -> this.covariatesUseGriddedData;
        };
    }

    @Override
    public String getHash()
    {
        return this.hash;
    }

    @Override
    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    @Override
    public boolean hasGeneratedBaseline()
    {
        return DeclarationUtilities.hasGeneratedBaseline( this.getBaseline() );
    }

    @Override
    public long getId()
    {
        return this.projectId;
    }

    @Override
    public boolean hasProbabilityThresholds()
    {
        return DeclarationUtilities.hasProbabilityThresholds( this.getDeclaration() );
    }

    @Override
    public String getLeftVariableName()
    {
        if ( Objects.isNull( this.leftVariable ) )
        {
            return this.getDeclaredLeftVariableName();
        }

        return this.leftVariable;
    }

    @Override
    public String getRightVariableName()
    {
        if ( Objects.isNull( this.rightVariable ) )
        {
            return this.getDeclaredRightVariableName();
        }

        return this.rightVariable;
    }

    @Override
    public String getBaselineVariableName()
    {
        if ( Objects.isNull( this.baselineVariable ) )
        {
            return this.getDeclaredBaselineVariableName();
        }

        return this.baselineVariable;
    }

    @Override
    public Dataset getCovariateDataset( String variableName )
    {
        return ProjectUtilities.getCovariateDatset( this.declaration, variableName );
    }

    @Override
    public String toString()
    {
        return "Project { Name: " + this.getProjectName()
               +
               ", Code: "
               + this.getHash()
               + " }";
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof DatabaseProject && this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getHash() );
    }

    /**
     * Gets the features and feature groups.
     * @param projectId the project identifier
     * @param declaration the project declaration
     * @param caches the database caches
     * @param gridded whether the evaluation uses gridded data
     * @param griddedFeatures the gridded features cache
     * @throws DataAccessException if the features and/or feature groups could not be set
     */

    private ProjectUtilities.FeatureSets getFeaturesAndFeatureGroups( long projectId,
                                                                      EvaluationDeclaration declaration,
                                                                      DatabaseCaches caches,
                                                                      boolean gridded,
                                                                      GriddedFeatures griddedFeatures )
    {
        LOGGER.debug( "Setting the features and feature groups for project {}.", projectId );

        Set<FeatureTuple> singletons = new HashSet<>(); // Singleton feature tuples
        Set<FeatureTuple> grouped = new HashSet<>(); // Multi-tuple groups
        boolean hasBaseline = DeclarationUtilities.hasBaseline( declaration );

        // Gridded features? #74266
        // Yes
        if ( gridded )
        {
            Set<FeatureTuple> griddedTuples = this.getGriddedFeatureTuples( griddedFeatures, hasBaseline );
            singletons.addAll( griddedTuples );
        }
        // No
        else
        {
            Features fCache = caches.getFeaturesCache();

            // At this point, features should already have been correlated by
            // the declaration or by a location service. In the latter case, the
            // WRES will have generated the List<Feature> and replaced them in
            // a new ProjectConfig, so this code cannot tell the difference.


            // Deal with the special case of singletons first
            Set<GeometryTuple> singletonFeatures = this.getDeclaredFeatures( declaration );

            // If there are no declared singletons, allow features to be discovered, but only if there are no declared
            // multi-feature groups.
            Set<GeometryGroup> declaredGroups = this.getDeclaredFeatureGroups( declaration );

            if ( !singletonFeatures.isEmpty() || declaredGroups.isEmpty() )
            {
                DataScripter script =
                        ProjectScriptGenerator.createIntersectingFeaturesScript( this.getDatabase(),
                                                                                 projectId,
                                                                                 singletonFeatures,
                                                                                 hasBaseline,
                                                                                 false );

                LOGGER.debug( "getIntersectingFeatures will run for singleton features: {}", script );
                Set<FeatureTuple> innerSingletons = this.readFeaturesFromScript( script, fCache, hasBaseline );

                singletons.addAll( innerSingletons );
                LOGGER.debug( "getIntersectingFeatures completed for singleton features, which identified "
                              + "{} features.",
                              innerSingletons.size() );
            }

            // Now deal with feature groups that contain one or more
            Set<GeometryTuple> groupedFeatures = declaredGroups.stream()
                                                               .flatMap( next -> next.getGeometryTuplesList()
                                                                                     .stream() )
                                                               .collect( Collectors.toSet() );

            if ( !groupedFeatures.isEmpty() )
            {
                DataScripter scriptForGroups =
                        ProjectScriptGenerator.createIntersectingFeaturesScript( this.getDatabase(),
                                                                                 projectId,
                                                                                 groupedFeatures,
                                                                                 hasBaseline,
                                                                                 true );

                LOGGER.debug( "getIntersectingFeatures will run for grouped features: {}", scriptForGroups );
                Set<FeatureTuple> innerGroups = this.readFeaturesFromScript( scriptForGroups, fCache, hasBaseline );
                grouped.addAll( innerGroups );
                LOGGER.debug( "getIntersectingFeatures completed for grouped features, which identified {} features",
                              innerGroups.size() );
            }
        }

        // Filter the singleton features against any spatial mask, unless there is gridded data, which is masked upfront
        // Do this before forming the groups, which include singleton groups
        if ( !gridded )
        {
            singletons = ProjectUtilities.filterFeatures( singletons, declaration.spatialMask() );
        }

        // Combine the singletons and feature groups into groups that contain one or more tuples
        ProjectUtilities.FeatureSets
                groups = ProjectUtilities.getFeatureGroups( Collections.unmodifiableSet( singletons ),
                                                            Collections.unmodifiableSet( grouped ),
                                                            declaration,
                                                            projectId );

        Set<FeatureGroup> finalGroups = groups.featureGroups();

        // Filter the multi-group features against any spatial mask, unless there is gridded data, which is masked
        // upfront
        if ( !gridded )
        {
            finalGroups = ProjectUtilities.filterFeatureGroups( finalGroups, declaration.spatialMask() );
        }

        LOGGER.debug( "Finished setting the feature groups for project {}. Discovered {} feature groups: {}.",
                      projectId,
                      finalGroups.size(),
                      finalGroups );

        // Features are the union of the singletons and grouped features
        finalGroups.stream()
                   .flatMap( next -> next.getFeatures()
                                         .stream() )
                   .forEach( singletons::add );

        LOGGER.debug( "Finished setting the features for project {}. Discovered {} features: {}.",
                      projectId,
                      singletons.size(),
                      singletons );

        if ( singletons.isEmpty()
             && finalGroups.isEmpty() )
        {
            throw new NoProjectDataException( "Failed to identify any geographic features with data on all required "
                                              + "sides (left, right and, when declared, baseline) for the variables "
                                              + "and other declaration supplied. Please check that the declaration is "
                                              + "expected to produce some features with time-series data on both sides "
                                              + "of the pairing." );
        }

        return new ProjectUtilities.FeatureSets( Collections.unmodifiableSet( singletons ),
                                                 finalGroups,
                                                 groups.doNotPublish() );
    }

    /**
     * Generates ingest identifiers from the ingest results.
     * @param ingestResults the ingest results
     * @return the ingest identifiers
     */
    private static IngestIds getIngestIds( List<IngestResult> ingestResults )
    {
        long[] leftIds = DatabaseProject.getIds( ingestResults, IngestResult::getLeftCount );
        long[] rightIds = DatabaseProject.getIds( ingestResults, IngestResult::getRightCount );
        long[] baselineIds = DatabaseProject.getIds( ingestResults, IngestResult::getBaselineCount );
        long[] covariateIds = DatabaseProject.getIds( ingestResults, IngestResult::getCovariateCount );

        IngestIds ingestIds = new IngestIds( leftIds, rightIds, baselineIds, covariateIds );

        // Check assumption that at least one left and one right source have
        // been created.
        int leftCount = leftIds.length;
        int rightCount = rightIds.length;

        if ( leftCount < 1 || rightCount < 1 )
        {
            throw new NoProjectDataException( "When examining the ingested data, discovered insufficient data sources "
                                              + "to proceed. At least one data source is required for the left side of "
                                              + "the evaluation and one data source for the right side, but the left "
                                              + "side had "
                                              + leftCount
                                              + " sources and the right side had "
                                              + rightCount
                                              + " sources. There were "
                                              + baselineIds.length
                                              + " baseline sources. Please check that all intended data sources were "
                                              + "declared and that all declared data sources were ingested correctly. "
                                              + "For example, were some data sources skipped because the format was "
                                              + "unrecognized?" );
        }

        return ingestIds;
    }

    /**
     * <p>Get the list of surrogate keys from given ingest results.
     *
     * @param ingestResults The ingest results.
     * @param count a function that returns the count of ingest results
     * @return The ids for the baseline dataset
     */

    private static long[] getIds( List<IngestResult> ingestResults,
                                  ToIntFunction<IngestResult> count )
    {
        // How big to make the array? We don't want to guess because then we
        // would need to resize, which requires more heap again. Better to get
        // it correct at the outset.
        int sizeNeeded = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            sizeNeeded += count.applyAsInt( ingestResult );
        }

        long[] ids = new long[sizeNeeded];
        int i = 0;

        for ( IngestResult ingestResult : ingestResults )
        {
            for ( short j = 0; j < count.applyAsInt( ingestResult ); j++ )
            {
                ids[i] = ingestResult.getSurrogateKey();
                i++;
            }
        }

        return ids;
    }

    /**
     * Saves the project in the database
     * @param ingestIds the ingest identifiers
     * @param hash the project hash
     * @param projectName the project name
     * @return the project identifier
     * @throws SQLException if the save fails
     */
    private long save( IngestIds ingestIds,
                       String hash,
                       String projectName ) throws SQLException
    {
        long innerProjectId;
        boolean performedInsert;

        LOGGER.trace( "Attempting to save project." );

        DataScripter saveScript = this.getInsertSelectStatement( hash, projectName );

        try
        {
            performedInsert = saveScript.execute() > 0;
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While attempting to save the project.", e );
        }

        if ( performedInsert )
        {
            innerProjectId = saveScript.getInsertedIds()
                                       .get( 0 );
        }
        else
        {
            Database db = this.getDatabase();
            DataScripter scriptWithId = new DataScripter( db );
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.addLine( "SELECT project_id" );
            scriptWithId.addLine( "FROM wres.Project P" );
            scriptWithId.addLine( "WHERE P.hash = ?" );
            scriptWithId.addArgument( this.getHash() );
            scriptWithId.setMaxRows( 1 );

            try ( DataProvider data = scriptWithId.getData() )
            {
                innerProjectId = data.getLong( PROJECT_ID );
            }
            catch ( SQLException e )
            {
                throw new DataAccessException( "While attempting to save the project.", e );
            }
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Did I create Project ID {}? {}",
                          innerProjectId,
                          performedInsert );
        }

        LOGGER.debug( "Did the Project created by this Thread insert into the database first? {}",
                      performedInsert );

        // Insert the wres.ProjectSource rows connecting ingest results to this project
        DatabaseProject.insertProjectSources( this.getDatabase(), innerProjectId, performedInsert, ingestIds );

        LOGGER.info( "The identity of the database project is '{}'.", innerProjectId );

        return innerProjectId;
    }

    /**
     * Continue ingest.
     *
     * @param database the database
     * @param projectId the database project identifier
     * @param inserted whether the project caused an insert
     * @param ingestIds the ingest identifiers
     * @throws SQLException if the project could not be ingested
     */
    private static void insertProjectSources( Database database,
                                              long projectId,
                                              boolean inserted,
                                              IngestIds ingestIds )
            throws SQLException
    {
        if ( Boolean.TRUE.equals( inserted ) )
        {
            String projectIdString = Long.toString( projectId );
            LOGGER.debug( "Found that this Thread is responsible for "
                          + "wres.ProjectSource rows for project {}",
                          projectId );

            // If we just created the Project, we are responsible for relating
            // project to source. Otherwise, we trust it is present.
            String tableName = "wres.ProjectSource";
            List<String> columnNames = List.of( PROJECT_ID, "source_id", "member" );

            List<String[]> values = DatabaseProject.getSourceRowsFromIds( ingestIds, projectIdString );

            // The first two columns are numbers, last one is char.
            boolean[] charColumns = { false, false, true };
            DatabaseOperations.insertIntoDatabase( database, tableName, columnNames, values, charColumns );
        }
        else
        {
            LOGGER.debug( "Found that this Thread is NOT responsible for "
                          + "wres.ProjectSource rows for project {}",
                          projectId );
            DataScripter scripter = new DataScripter( database );
            scripter.addLine( "SELECT COUNT( source_id ) AS count" );
            scripter.addLine( "FROM wres.ProjectSource" );
            scripter.addLine( "WHERE project_id = ?" );
            scripter.addArgument( projectId );

            // Need to wait here until the data is available. How long to wait?
            // Start with 30ish seconds, error out after that. We might actually
            // wait longer than 30 seconds.
            long startMillis = System.currentTimeMillis();
            long endMillis = startMillis + Duration.ofSeconds( 30 )
                                                   .toMillis();
            long currentMillis = startMillis;
            long sleepMillis = Duration.ofSeconds( 1 )
                                       .toMillis();
            boolean projectSourceRowsFound = false;

            while ( currentMillis < endMillis )
            {
                try ( DataProvider dataProvider = scripter.getData() )
                {
                    long count = dataProvider.getLong( "count" );

                    if ( count > 1 )
                    {
                        // We assume that the projectsource rows are made
                        // in a single transaction here. We further assume that
                        // each project will have at least a left and right
                        // member, therefore 2 or more rows (greater than 1).
                        LOGGER.debug( "wres.ProjectSource rows present for {}",
                                      projectId );
                        projectSourceRowsFound = true;
                        break;
                    }
                    else
                    {
                        LOGGER.debug( "wres.ProjectSource rows missing for {}",
                                      projectId );
                        Thread.sleep( sleepMillis );
                    }
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.warn( "Interrupted while waiting for wres.ProjectSource rows.", ie );
                    Thread.currentThread().interrupt();
                    // No need to rethrow, the evaluation will fail.
                }

                currentMillis = System.currentTimeMillis();
            }

            if ( !projectSourceRowsFound )
            {
                throw new IngestException( "Another WRES instance failed to "
                                           + "complete ingest that this "
                                           + "evaluation depends on." );
            }
        }
    }

    /**
     * Returns the top hash of the project sources from the ingest identifiers.
     * @param ingestIds the ingest identifiers
     * @param database the database
     * @return the top hash of project sources
     * @throws SQLException if the top hash could not be determined
     */
    private static String getTopHashOfIngestedSources( IngestIds ingestIds,
                                                       Database database ) throws SQLException
    {
        // Permit the List<IngestResult> to be garbage collected here, which
        // should leave space on heap for creating collections in the following.

        // We don't yet know how many unique timeseries there are. For example,
        // a baseline forecast could be the same as a right forecast. So we
        // can't as easily drop to primitive arrays because we would want to
        // know how to size them up front. The countOfIngestResults is a
        // maximum, though.
        Set<Long> uniqueSourcesUsed = new HashSet<>();

        // Assemble the IDs
        Arrays.stream( ingestIds.leftIds() )
              .forEach( uniqueSourcesUsed::add );
        Arrays.stream( ingestIds.rightIds() )
              .forEach( uniqueSourcesUsed::add );
        Arrays.stream( ingestIds.baselineIds() )
              .forEach( uniqueSourcesUsed::add );
        Arrays.stream( ingestIds.covariateIds() )
              .forEach( uniqueSourcesUsed::add );

        int countOfUniqueHashes = uniqueSourcesUsed.size();
        final int MAX_PARAMETER_COUNT = 999;
        Map<Long, String> idsToHashes = new HashMap<>( countOfUniqueHashes );
        Set<Long> batchOfIds = new HashSet<>( MAX_PARAMETER_COUNT );

        for ( Long rawId : uniqueSourcesUsed )
        {
            // If appending this id is <= max, add it.
            if ( batchOfIds.size() + 1 > MAX_PARAMETER_COUNT )
            {
                LOGGER.debug( "Query would exceed {} params, running it now and building a new one.",
                              MAX_PARAMETER_COUNT );
                DatabaseProject.selectIdsAndHashes( database, batchOfIds, idsToHashes );
                batchOfIds.clear();
            }

            batchOfIds.add( rawId );
        }

        // The last query with the remainder of ids.
        DatabaseProject.selectIdsAndHashes( database, batchOfIds, idsToHashes );

        // "select hash from wres.Source S inner join ( select ... ) I on S.source_id = I.source_id"
        String[] leftHashes = DatabaseProject.getHashes( ingestIds.leftIds(), idsToHashes );
        String[] rightHashes = DatabaseProject.getHashes( ingestIds.rightIds(), idsToHashes );
        String[] baselineHashes = DatabaseProject.getHashes( ingestIds.baselineIds(), idsToHashes );
        String[] covariateHashes = DatabaseProject.getHashes( ingestIds.covariateIds(), idsToHashes );

        IngestHashes hashes = new IngestHashes( leftHashes, rightHashes, baselineHashes, covariateHashes );

        return DatabaseProject.getTopHashOfSources( hashes );
    }

    /**
     * <p>Creates a hash for the indicated project configuration based on its
     * data ingested.
     *
     * @param hashes the ingest hashes
     * @return a unique hash code for the project's circumstances
     */
    private static String getTopHashOfSources( IngestHashes hashes )
    {
        MessageDigest md5Digest;

        try
        {
            md5Digest = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IngestException( "Couldn't use MD5 algorithm.",
                                       nsae );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.stream( hashes.leftHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );
        Arrays.stream( hashes.rightHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );
        Arrays.stream( hashes.baselineHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );
        Arrays.stream( hashes.covariateHashes() )
              .sorted()
              .forEach( n -> DigestUtils.updateDigest( md5Digest, n ) );

        byte[] digestAsHex = md5Digest.digest();
        return Hex.encodeHexString( digestAsHex );
    }

    /**
     * Converts the supplied IDs to source hashes using the translation map.
     * @param ids the ids to hash
     * @param idsToHashes the map of IDs to hashes
     * @return the hashes
     */
    private static String[] getHashes( long[] ids, Map<Long, String> idsToHashes )
    {
        String[] hashes = new String[ids.length];

        for ( int i = 0; i < ids.length; i++ )
        {
            long id = ids[i];
            String hash = idsToHashes.get( id );

            if ( Objects.nonNull( hash ) )
            {
                hashes[i] = hash;
            }
            else
            {
                throw new IngestException( "Unexpected null left hash value for id="
                                           + id );
            }
        }

        return hashes;
    }

    /**
     * Select source ids and hashes and put them into the given Map.
     * @param database The database to use.
     * @param ids The ids to use for selection of hashes.
     * @param idsToHashes MUTATED by this method, results go into this Map.
     * @throws SQLException When something goes wrong related to database.
     * @throws IngestException When a null value is found in result set.
     */

    private static void selectIdsAndHashes( Database database,
                                            Set<Long> ids,
                                            Map<Long, String> idsToHashes )
            throws SQLException
    {
        String queryStart = "SELECT source_id, hash "
                            + "FROM wres.Source "
                            + "WHERE source_id in ";
        StringJoiner idJoiner = new StringJoiner( ",", "(", ")" );

        for ( int i = 0; i < ids.size(); i++ )
        {
            idJoiner.add( "?" );
        }

        String query = queryStart + idJoiner;
        DataScripter script = new DataScripter( database, query );

        for ( Long id : ids )
        {
            script.addArgument( id );
        }

        script.setMaxRows( ids.size() );

        try ( DataProvider dataProvider = script.getData() )
        {
            while ( dataProvider.next() )
            {
                Long id = dataProvider.getLong( "source_id" );
                String hash = dataProvider.getString( "hash" );

                if ( Objects.nonNull( id )
                     && Objects.nonNull( hash ) )
                {
                    idsToHashes.put( id, hash );
                }
                else
                {
                    boolean idNull = Objects.isNull( id );
                    boolean hashNull = Objects.isNull( hash );
                    throw new IngestException( "Found a null value in db when expecting a value. idNull="
                                               + idNull
                                               + " hashNull="
                                               + hashNull );
                }
            }
        }
    }

    /**
     * Converts source IDs into source rows for insertion.
     * @param ingestIds the ingest ids
     * @param projectId the project ID
     * @return the source rows to insert
     */
    private static List<String[]> getSourceRowsFromIds( IngestIds ingestIds,
                                                        String projectId )
    {
        List<String[]> values = new ArrayList<>();

        for ( long sourceID : ingestIds.leftIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "left";
            values.add( row );
        }

        for ( long sourceID : ingestIds.rightIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "right";
            values.add( row );
        }

        for ( long sourceID : ingestIds.baselineIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "baseline";
            values.add( row );
        }

        for ( long sourceID : ingestIds.covariateIds() )
        {
            String[] row = new String[3];
            row[0] = projectId;
            row[1] = Long.toString( sourceID );
            row[2] = "covariate";
            values.add( row );
        }

        return Collections.unmodifiableList( values );
    }

    /**
     * Checks that the union of ensemble conditions will select some data, otherwise throws an exception.
     *
     * @param declaration the project declaration
     * @param projectId the project identifier
     * @throws NoProjectDataException if the conditions select no data
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private void validateEnsembleConditions( EvaluationDeclaration declaration,
                                             long projectId )
    {
        // Show all errors at once rather than drip-feeding
        Dataset left = declaration.left();
        List<String> failedLeft = this.getInvalidEnsembleConditions( DatasetOrientation.LEFT, left, projectId );
        List<String> failed = new ArrayList<>( failedLeft );
        Dataset right = declaration.right();
        List<String> failedRight = this.getInvalidEnsembleConditions( DatasetOrientation.RIGHT, right, projectId );
        failed.addAll( failedRight );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            Dataset baseline = declaration.baseline()
                                          .dataset();
            List<String> failedBaseline = this.getInvalidEnsembleConditions( DatasetOrientation.BASELINE,
                                                                             baseline,
                                                                             projectId );
            failed.addAll( failedBaseline );
        }

        if ( !failed.isEmpty() )
        {
            throw new NoProjectDataException( "Of the filters that were defined for ensemble names, "
                                              + failed.size()
                                              + " of those filters did not select any data. Fix the declared filters to "
                                              + "ensure that each filter selects some data. The invalid filters are: "
                                              + failed
                                              + "." );
        }
    }

    /**
     * Checks that each covariate has some data for the feature names associated with it. Otherwise, the feature
     * authority of the covariate may need to be declared explicitly.
     *
     * @param covariates the covariates
     * @param projectId the project identifier
     * @param projectFeatures the project features
     * @param featureCache the feature cache
     * @return the covariate features by variable name
     * @throws DataAccessException if the data could not be accessed
     * @throws NoProjectDataException if no features could be correlated with time-series data
     */

    private Map<String, Set<Feature>> getCovariateFeatures( List<CovariateDataset> covariates,
                                                            long projectId,
                                                            Set<FeatureTuple> projectFeatures,
                                                            Features featureCache )
    {
        Map<String, Set<Feature>> covariateFeaturesInner = new HashMap<>();

        for ( CovariateDataset covariate : covariates )
        {
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

            // Get the set of features available for this covariate
            Database db = this.getDatabase();
            DataScripter script = new DataScripter( db );
            script.addLine( "SELECT DISTINCT F.feature_id" );
            script.addLine( "FROM wres.Feature F" );
            script.addLine( "INNER JOIN wres.Source S" );
            script.addTab().addLine( "ON F.feature_id = S.feature_id" );
            script.addLine( "INNER JOIN wres.ProjectSource PS" );
            script.addTab().addLine( "ON S.source_id = PS.source_id" );
            script.addLine( "WHERE PS.project_id = ?" );
            script.addArgument( projectId );
            script.addTab().addLine( "AND PS.member = ?" );
            script.addArgument( DatasetOrientation.COVARIATE.toString() );
            script.addTab().addLine( "AND S.variable_name = ?" );
            script.addArgument( covariateName );
            Set<Feature> ingestedFeatures = new HashSet<>();
            try ( DataProvider provider = script.getData() )
            {
                while ( provider.next() )
                {
                    long nextId = provider.getLong( "feature_id" );
                    Feature nextFeature = featureCache.getFeatureKey( nextId );
                    ingestedFeatures.add( nextFeature );
                }
            }
            catch ( SQLException e )
            {
                throw new DataAccessException( "While attempting to determine whether the covariate feature names "
                                               + "select some data.", e );
            }

            Set<Feature> matchingFeatures = ProjectUtilities.covariateFeaturesSelectSomeData( covariate,
                                                                                              projectFeatures,
                                                                                              ingestedFeatures );

            covariateFeaturesInner.put( covariateName, matchingFeatures );
        }

        return Collections.unmodifiableMap( covariateFeaturesInner );
    }

    /**
     * Sets the variables to evaluate. Begins by looking at the declaration. If it cannot find a declared variable for 
     * any particular left/right/baseline context, it looks at the data instead. If there is more than one possible 
     * name and that name does not exactly match the name identified for the other side of the pairing, then an
     * exception is thrown because declaration is required to disambiguate. Otherwise, it chooses the single variable
     * name and warns about the assumption made when using the data to disambiguate.
     *
     * @param declaration the project declaration
     * @param projectId the project identifier
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private VariableNames getVariablesToEvaluate( EvaluationDeclaration declaration,
                                                  long projectId )
    {
        // The set of possibilities to validate
        boolean hasBaseline = DeclarationUtilities.hasBaseline( declaration );
        Set<String> leftNames = this.getVariableNameByInspectingData( DatasetOrientation.LEFT, projectId, hasBaseline );
        Set<String> rightNames = this.getVariableNameByInspectingData( DatasetOrientation.RIGHT,
                                                                       projectId,
                                                                       hasBaseline );
        Set<String> baselineNames = this.getVariableNameByInspectingData( DatasetOrientation.BASELINE,
                                                                          projectId,
                                                                          hasBaseline );
        Set<String> covariateNames = this.getVariableNameByInspectingData( DatasetOrientation.COVARIATE,
                                                                           projectId,
                                                                           hasBaseline );

        LOGGER.debug( "While looking for variable names to evaluate, discovered {} on the LEFT side, {} on the RIGHT "
                      + "side, {} on the BASELINE side and {} on the COVARIATE side.",
                      leftNames,
                      rightNames,
                      baselineNames,
                      covariateNames );

        return ProjectUtilities.getVariableNames( declaration,
                                                  leftNames,
                                                  rightNames,
                                                  baselineNames,
                                                  covariateNames );
    }

    /**
     * Determines the possible variable names by inspecting the data.
     *
     * @param orientation the context
     * @param projectId the project identifier
     * @param hasBaseline whether the evaluation contains a baseline dataset
     * @return the possible variable names
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private Set<String> getVariableNameByInspectingData( DatasetOrientation orientation,
                                                         long projectId,
                                                         boolean hasBaseline )
    {
        if ( orientation == DatasetOrientation.BASELINE
             && !hasBaseline )
        {
            LOGGER.debug( "No variable names to inspect for the {} orientation.", DatasetOrientation.BASELINE );
            return Set.of();
        }

        DataScripter script = ProjectScriptGenerator.createVariablesScript( this.getDatabase(),
                                                                            projectId,
                                                                            orientation );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "The script for auto-detecting variables on the {} side will run with parameters {}:{}{}",
                          orientation,
                          script.getParameterStrings(),
                          System.lineSeparator(),
                          script );
        }

        Set<String> names = new HashSet<>();
        try ( DataProvider provider = script.getData() )
        {
            while ( provider.next() )
            {
                String nextName = provider.getString( "variable_name" );
                names.add( nextName );
            }
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While attempting to determine the variable name for "
                                           + orientation
                                           + " data.", e );
        }

        return Collections.unmodifiableSet( names );
    }

    /**
     * Checks for any invalid ensemble conditions and returns a string representation of the invalid conditions.
     *
     * @param orientation the orientation of the source
     * @param dataset the source configuration whose ensemble conditions should be validated
     * @param projectId the project identifier
     * @return a string representation of the invalid conditions 
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private List<String> getInvalidEnsembleConditions( DatasetOrientation orientation,
                                                       Dataset dataset,
                                                       long projectId )
    {
        List<String> failed = new ArrayList<>();

        EnsembleFilter filter = dataset.ensembleFilter();
        if ( Objects.nonNull( filter ) )
        {
            for ( String name : filter.members() )
            {
                DataScripter script = ProjectScriptGenerator.getIsValidEnsembleCondition( this.getDatabase(),
                                                                                          name,
                                                                                          projectId,
                                                                                          filter.exclude() );

                LOGGER.debug( "getIsValidEnsembleCondition will run: {}", script );

                try ( Connection connection = this.getDatabase()
                                                  .getConnection();
                      DataProvider dataProvider = script.buffer( connection ) )
                {
                    while ( dataProvider.next() )
                    {
                        boolean dataExists = dataProvider.getBoolean( "data_exists" );

                        if ( !dataExists )
                        {
                            ToStringBuilder builder =
                                    new ToStringBuilder( ToStringStyle.SHORT_PREFIX_STYLE ).append( "orientation",
                                                                                                    orientation )
                                                                                           .append( "name",
                                                                                                    name )
                                                                                           .append( "exclude",
                                                                                                    filter.exclude() );

                            failed.add( builder.toString() );
                        }
                    }
                }
                catch ( SQLException e )
                {
                    throw new DataAccessException( "While attempting validate ensemble conditions.", e );
                }

                LOGGER.debug( "getIsValidEnsembleCondition finished run: {}", script );
            }
        }

        return Collections.unmodifiableList( failed );
    }

    /**
     * @param declaration the declaration
     * @return the declared features
     */
    private Set<GeometryTuple> getDeclaredFeatures( EvaluationDeclaration declaration )
    {
        if ( Objects.isNull( declaration.features() ) )
        {
            return Set.of();
        }

        return declaration.features()
                          .geometries();
    }

    /**
     * @param declaration the declaration
     * @return the declared feature groups
     */
    private Set<GeometryGroup> getDeclaredFeatureGroups( EvaluationDeclaration declaration )
    {
        if ( Objects.isNull( declaration.featureGroups() ) )
        {
            return Set.of();
        }

        return declaration.featureGroups()
                          .geometryGroups();
    }

    /**
     * Builds a set of gridded feature tuples. Assumes that all dimensions have the same tuple (i.e., cannot currently
     * pair grids with different features. Feature groupings are also not supported.
     *
     * @param griddedFeatures the gridded features cache
     * @return a set of gridded feature tuples
     */

    private Set<FeatureTuple> getGriddedFeatureTuples( GriddedFeatures griddedFeatures,
                                                       boolean hasBaseline )
    {
        Objects.requireNonNull( griddedFeatures,
                                "Cannot query gridded feature availability without a gridded features cache." );

        LOGGER.debug( "Getting details of intersecting features for gridded data." );
        Set<Feature> innerGriddedFeatures = griddedFeatures.get();
        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Feature nextFeature : innerGriddedFeatures )
        {
            Geometry geometry = MessageFactory.parse( nextFeature );
            GeometryTuple geoTuple;
            if ( hasBaseline )
            {
                geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, geometry );
            }
            else
            {
                geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, null );
            }

            FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
            featureTuples.add( featureTuple );
        }

        return Collections.unmodifiableSet( featureTuples );
    }

    /**
     * Reads a set of feature tuples from a feature selection script.
     * @param script the script to read
     * @param fCache the features cache
     * @param hasBaseline whether the evaluation has a baseline dataset
     * @return the feature tuples
     * @throws DataAccessException if the features could not be read
     */

    private Set<FeatureTuple> readFeaturesFromScript( DataScripter script, Features fCache, boolean hasBaseline )
    {
        Set<FeatureTuple> featureTuples = new HashSet<>();

        try ( Connection connection = this.getDatabase()
                                          .getConnection();
              DataProvider dataProvider = script.buffer( connection ) )
        {
            while ( dataProvider.next() )
            {
                int leftId = dataProvider.getInt( "left_id" );
                Feature leftKey = fCache.getFeatureKey( leftId );
                int rightId = dataProvider.getInt( "right_id" );
                Feature rightKey = fCache.getFeatureKey( rightId );
                Feature baselineKey = null;

                // Baseline column will only be there when baseline exists.
                if ( hasBaseline )
                {
                    int baselineId = dataProvider.getInt( "baseline_id" );

                    // JDBC getInt returns 0 when not found. All primary key
                    // columns should start at 1.
                    if ( baselineId > 0 )
                    {
                        baselineKey = fCache.getFeatureKey( baselineId );
                    }
                }

                GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( leftKey, rightKey, baselineKey );
                FeatureTuple featureTuple = FeatureTuple.of( geometryTuple );

                featureTuples.add( featureTuple );
            }
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While attempting to read features.", e );
        }

        return Collections.unmodifiableSet( featureTuples );
    }

    /**
     * Generates an insert select statementfrom the input.
     * @param hash the project hash
     * @param projectName the project name
     * @return a data scripter
     */
    private DataScripter getInsertSelectStatement( String hash,
                                                   String projectName )
    {
        Database db = this.getDatabase();
        DataScripter script = new DataScripter( db );
        script.setUseTransaction( true );

        script.retryOnSerializationFailure();
        script.retryOnUniqueViolation();

        script.setHighPriority( true );

        script.addLine( "INSERT INTO wres.Project ( hash, project_name )" );
        script.addTab().addLine( "SELECT ?, ?" );

        script.addArgument( hash );
        script.addArgument( projectName );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( SELECT_1 );
        script.addTab( 2 ).addLine( "FROM wres.Project P" );
        script.addTab( 2 ).addLine( "WHERE P.hash = ?" );

        script.addArgument( hash );

        script.addTab().addLine( ")" );
        return script;
    }

    /**
     * @return the project name
     */
    private String getProjectName()
    {
        return this.getDeclaration()
                   .label();
    }

    /**
     * @return the database
     */
    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * @see #getLeftVariableName()
     * @return The declared left variable name or null if undeclared
     */
    private String getDeclaredLeftVariableName()
    {
        return DeclarationUtilities.getVariableName( this.getLeft() );
    }

    /**
     * @see #getRightVariableName()
     * @return The declared right variable name or null if undeclared
     */
    private String getDeclaredRightVariableName()
    {
        return DeclarationUtilities.getVariableName( this.getRight() );
    }

    /**
     * @see #getBaselineVariableName()
     * @return The declared baseline variable name or null if undeclared
     */
    private String getDeclaredBaselineVariableName()
    {
        String variableName = null;

        if ( this.hasBaseline() )
        {
            variableName = DeclarationUtilities.getVariableName( this.getBaseline()
                                                                     .dataset() );
        }

        return variableName;
    }

    /**
     * @return The left hand data source configuration
     */
    private Dataset getLeft()
    {
        return this.getDeclaration()
                   .left();
    }

    /**
     * @return The right hand data source configuration
     */
    private Dataset getRight()
    {
        return this.getDeclaration()
                   .right();
    }

    /**
     * @return The baseline data source configuration
     */
    private BaselineDataset getBaseline()
    {
        return this.getDeclaration()
                   .baseline();
    }

    /**
     * @param declaration the project declaration
     * @param projectId the project identifier
     * @return the measurement unit, which is either the declared unit or the analyzed unit, but possibly null
     * @throws DataAccessException if the measurement unit could not be determined
     * @throws IllegalArgumentException if the project identity is required and undefined
     */

    private String getAnalyzedMeasurementUnit( EvaluationDeclaration declaration, long projectId )
    {
        String measurementUnitInner = null;

        String declaredUnit = declaration.unit();

        // Declared unit available?
        if ( Objects.nonNull( declaredUnit )
             && !declaredUnit.isBlank() )
        {
            measurementUnitInner = declaredUnit;

            LOGGER.debug( "Determined the measurement unit from the project declaration as {}.",
                          measurementUnitInner );
        }

        // Still not available? Then analyze the unit.
        if ( Objects.isNull( measurementUnitInner ) )
        {
            DataScripter scripter = ProjectScriptGenerator.createUnitScript( this.getDatabase(), projectId );

            try ( Connection connection = this.getDatabase()
                                              .getConnection();
                  DataProvider dataProvider = scripter.buffer( connection ) )
            {
                if ( dataProvider.next() )
                {
                    measurementUnitInner = dataProvider.getString( "unit_name" );

                    String member = dataProvider.getString( "member" );

                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "Determined the measurement unit by analyzing the project sources. The analyzed "
                                      + "measurement unit is {} and corresponds to the most commonly occurring unit "
                                      + "among time-series from {} sources. The script used to discover the "
                                      + "measurement unit for the project with identifier {} was: {}{}",
                                      measurementUnitInner,
                                      member,
                                      projectId,
                                      System.lineSeparator(),
                                      scripter );
                    }
                }
            }
            catch ( SQLException e )
            {
                throw new DataAccessException( "While attempting to acquire a measurement unit.", e );
            }
        }

        return measurementUnitInner;
    }

    /**
     * Returns the desired timescale. In order of availability, this is:
     *
     * <ol>
     * <li>The desired time scale provided on construction;</li>
     * <li>The Least Common Scale (LCS) computed from the input data; or</li>
     * <li>The LCS computed from the <code>existingTimeScale</code> provided in the input declaration.</li>
     * </ol>
     *
     * The LCS is the smallest common multiple of the time scales associated with every ingested dataset for a given
     * project, variable and feature. The LCS is computed from all sides of a pairing (left, right and baseline)
     * collectively.
     *
     * @return the desired timescale or null if unknown
     * @param declaration the project declaration
     * @param projectId the project identifier
     * @throws DataAccessException if the existing time scales could not be obtained from the database
     */

    private TimeScaleOuter getDesiredTimeScale( EvaluationDeclaration declaration,
                                                long projectId )
    {
        TimeScaleOuter desiredTimeScaleInner;

        // Use the declared timescale
        TimeScale declaredScale = declaration.timeScale();
        if ( Objects.nonNull( declaredScale ) )
        {
            desiredTimeScaleInner = TimeScaleOuter.of( declaredScale.timeScale() );

            LOGGER.trace( "Discovered that the desired time scale was declared explicitly as {}.",
                          desiredTimeScaleInner );

            return desiredTimeScaleInner;
        }

        // Find the Least Common Scale
        Set<TimeScaleOuter> existingTimeScales = new HashSet<>();
        DataScripter script = ProjectScriptGenerator.createTimeScalesScript( this.getDatabase(),
                                                                             projectId );

        try ( Connection connection = this.getDatabase()
                                          .getConnection();
              DataProvider dataProvider = script.buffer( connection ) )
        {
            while ( dataProvider.next() )
            {
                long durationMillis = dataProvider.getLong( "duration_ms" );
                String functionName = dataProvider.getString( "function_name" );

                Duration duration = Duration.ofMillis( durationMillis );
                TimeScaleFunction function = TimeScaleFunction.valueOf( functionName );
                TimeScaleOuter scale = TimeScaleOuter.of( duration, function );
                existingTimeScales.add( scale );
            }
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Unable to obtain the existing time scales of ingested time-series.", e );
        }

        // Look for the LCS among the ingested sources
        if ( !existingTimeScales.isEmpty() )
        {
            TimeScaleOuter leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( existingTimeScales );

            desiredTimeScaleInner = leastCommonScale;

            LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project. "
                          + "Instead, determined the desired time scale from the Least Common Scale of the ingested "
                          + "inputs, which was {}.",
                          leastCommonScale );

            return desiredTimeScaleInner;
        }

        // Look for the LCS among the declared inputs
        Set<TimeScaleOuter> declaredExistingTimeScales = DeclarationUtilities.getSourceTimeScales( declaration )
                                                                             .stream()
                                                                             .map( TimeScaleOuter::of )
                                                                             .collect( Collectors.toUnmodifiableSet() );

        if ( !declaredExistingTimeScales.isEmpty() )
        {
            TimeScaleOuter leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( declaredExistingTimeScales );

            desiredTimeScaleInner = leastCommonScale;

            LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project."
                          + " Instead, determined the desired time scale from the Least Common Scale of the "
                          + "declared inputs, which  was {}.",
                          leastCommonScale );

            return desiredTimeScaleInner;
        }

        LOGGER.debug( "The desired timescale is missing from project {}.", projectId );
        return null;
    }

    /**
     * @param orientation the dataset orientation
     * @return whether the evaluation uses gridded data
     */
    private boolean getUsesGriddedData( DatasetOrientation orientation )
    {
        boolean usesGriddedData = false;

        Database db = this.getDatabase();
        DataScripter script = new DataScripter( db );
        script.addLine( "SELECT DISTINCT S.is_point_data" );
        script.addLine( "FROM wres.ProjectSource PS" );
        script.addLine( "INNER JOIN wres.Source S" );
        script.addTab().addLine( "ON PS.source_id = S.source_id" );
        script.addLine( "WHERE PS.project_id = ?" );
        script.addArgument( this.getId() );
        script.addTab().addLine( "AND PS.member = ?" );
        script.addArgument( orientation.name()
                                       .toLowerCase() );
        script.setMaxRows( 2 );

        try ( DataProvider provider = script.getData() )
        {
            if ( provider.next() )
            {
                usesGriddedData = !provider.getBoolean( "is_point_data" );

                // If there are multiple rows, that is disallowed
                if ( provider.next() )
                {
                    throw new IllegalStateException( "Discovered multiple datasets with a "
                                                     + orientation
                                                     + " orientation of which some were gridded and "
                                                     + "others not, which is not supported. " );
                }
            }
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While attempting to determine whether gridded data were ingested.", e );
        }

        return usesGriddedData;
    }

    /**
     * Record of ingest identifiers.
     * @param leftIds the left identifiers
     * @param rightIds the right identifiers
     * @param baselineIds the baseline identifiers
     * @param covariateIds the covariate identifiers
     */
    private record IngestIds( long[] leftIds, long[] rightIds, long[] baselineIds, long[] covariateIds ) {} // NOSONAR

    /**
     * Record of ingest hashes.
     * @param leftHashes the left hashes
     * @param rightHashes the right hashes
     * @param baselineHashes the baseline hashes
     * @param covariateHashes the covariate hashes
     */
    private record IngestHashes( String[] leftHashes, // NOSONAR
                                 String[] rightHashes,
                                 String[] baselineHashes,
                                 String[] covariateHashes ) {}
}