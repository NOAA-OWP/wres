package wres.io.project;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.BaselineDataset;
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
import wres.io.BadProjectException;
import wres.io.data.DataProvider;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.Features;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.io.database.caching.Variables;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.project.ProjectUtilities.VariableNames;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Provides helpers related to the project declaration in combination with the ingested time-series data.
 */
public class DatabaseProject implements Project
{
    private static final String SELECT_1 = "SELECT 1";
    private static final String PROJECT_ID = "project_id";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseProject.class );

    /** Protects access and generation of the feature collection. */
    private final Object featureLock = new Object();

    private final EvaluationDeclaration declaration;
    private final Database database;
    private final DatabaseCaches caches;
    private final GriddedFeatures griddedFeatures;

    /** The overall hash for the data sources used in the project. */
    private final String hash;

    private long projectId;

    /** The measurement unit, which is the declared unit, if available, else the most commonly occurring unit among the
     * project sources, with a preference for the mostly commonly occurring right-sided source unit. See 
     * {@link ProjectScriptGenerator#createUnitScript(Database, long)}}. */

    private String measurementUnit = null;

    /** The set of all features pertaining to the project. */
    private Set<FeatureTuple> features;

    /** The feature groups related to the project. */
    private Set<FeatureGroup> featureGroups;

    /** The singleton feature groups for which statistics should not be published, if any. */
    private Set<FeatureGroup> doNotPublish;

    /** Indicates whether this project was inserted on upon this execution of the project. */
    private boolean performedInsert;

    private Boolean leftUsesGriddedData = null;
    private Boolean rightUsesGriddedData = null;
    private Boolean baselineUsesGriddedData = null;

    /** The left-ish variable to evaluate. */
    private String leftVariable;

    /** The right-ish variable to evaluate. */
    private String rightVariable;

    /** The baseline-ish variable to evaluate. */
    private String baselineVariable;

    /** The desired timescale. */
    private TimeScaleOuter desiredTimeScale;

    /**
     * Creates an instance.
     * @param database the database
     * @param caches the database ORMs/caches
     * @param griddedFeatures the gridded features cache, if required
     * @param declaration the project declaration
     * @param hash the hash of the project data
     */

    public DatabaseProject( Database database,
                            DatabaseCaches caches,
                            GriddedFeatures griddedFeatures,
                            EvaluationDeclaration declaration,
                            String hash )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( hash );

        this.database = database;
        this.declaration = declaration;
        this.hash = hash;
        this.caches = caches;
        this.griddedFeatures = griddedFeatures;

        // Read only from now on, post ingest
        this.caches.setReadOnly();
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
        // Declared unit available?
        String declaredUnit = this.getDeclaration()
                                  .unit();
        if ( Objects.isNull( this.measurementUnit ) && Objects.nonNull( declaredUnit ) && !declaredUnit.isBlank() )
        {
            this.measurementUnit = declaredUnit;

            LOGGER.debug( "Determined the measurement unit from the project declaration as {}.",
                          this.measurementUnit );
        }

        // Still not available? Then analyze the unit.
        if ( Objects.isNull( this.measurementUnit ) )
        {
            DataScripter scripter = ProjectScriptGenerator.createUnitScript( this.getDatabase(), this.getId() );

            try ( Connection connection = this.getDatabase()
                                              .getConnection();
                  DataProvider dataProvider = scripter.buffer( connection ) )
            {
                if ( dataProvider.next() )
                {
                    this.measurementUnit = dataProvider.getString( "unit_name" );

                    String member = dataProvider.getString( "member" );

                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "Determined the measurement unit by analyzing the project sources. The analyzed "
                                      + "measurement unit is {} and corresponds to the most commonly occurring unit "
                                      + "among time-series from {} sources. The script used to discover the "
                                      + "measurement unit for the project with identifier {} was: {}{}",
                                      this.measurementUnit,
                                      member,
                                      this.getId(),
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
        if ( Objects.nonNull( this.desiredTimeScale ) )
        {
            LOGGER.trace( "Discovered a desired time scale of {}.",
                          this.desiredTimeScale );

            return this.desiredTimeScale;
        }

        // Use the declared timescale
        TimeScale declaredScale = this.getDeclaration()
                                      .timeScale();
        if ( Objects.nonNull( declaredScale ) )
        {
            this.desiredTimeScale = TimeScaleOuter.of( declaredScale.timeScale() );

            LOGGER.trace( "Discovered that the desired time scale was declared explicitly as {}.",
                          this.desiredTimeScale );

            return this.desiredTimeScale;
        }

        // Find the Least Common Scale
        Set<TimeScaleOuter> existingTimeScales = new HashSet<>();
        DataScripter script = ProjectScriptGenerator.createTimeScalesScript( this.getDatabase(),
                                                                             this.getProjectId() );

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

            this.desiredTimeScale = leastCommonScale;

            LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project. "
                          + "Instead, determined the desired time scale from the Least Common Scale of the ingested "
                          + "inputs, which was {}.",
                          leastCommonScale );

            return this.desiredTimeScale;
        }

        // Look for the LCS among the declared inputs
        Set<TimeScaleOuter> declaredExistingTimeScales = DeclarationUtilities.getSourceTimeScales( declaration )
                                                                             .stream()
                                                                             .map( TimeScaleOuter::of )
                                                                             .collect( Collectors.toUnmodifiableSet() );

        if ( !declaredExistingTimeScales.isEmpty() )
        {
            TimeScaleOuter leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( declaredExistingTimeScales );

            this.desiredTimeScale = leastCommonScale;

            LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project."
                          + " Instead, determined the desired time scale from the Least Common Scale of the "
                          + "declared inputs, which  was {}.",
                          leastCommonScale );

            return this.desiredTimeScale;
        }

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

        return Collections.unmodifiableSet( this.features );
    }

    @Override
    public Set<FeatureGroup> getFeatureGroups()
    {
        if ( Objects.isNull( this.featureGroups ) )
        {
            throw new IllegalStateException( "The feature groups have not been set." );
        }

        return Collections.unmodifiableSet( this.featureGroups );
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
    public Dataset getDeclaredDataset( DatasetOrientation orientation )
    {
        Objects.requireNonNull( orientation );

        if ( orientation == DatasetOrientation.BASELINE && !this.hasBaseline() )
        {
            LOGGER.debug( "Requested a baseline dataset, but not baseline dataset was defined." );
            return null;
        }

        return switch ( orientation )
        {
            case LEFT -> this.getDeclaration()
                             .left();
            case RIGHT -> this.getDeclaration()
                              .right();
            case BASELINE -> this.getDeclaration()
                                 .baseline()
                                 .dataset();
        };
    }

    @Override
    public String getVariableName( DatasetOrientation orientation )
    {
        Objects.requireNonNull( orientation );

        return switch ( orientation )
        {
            case LEFT -> this.getLeftVariableName();
            case RIGHT -> this.getRightVariableName();
            case BASELINE -> this.getBaselineVariableName();
        };
    }

    @Override
    public SortedSet<String> getEnsembleLabels( DatasetOrientation orientation )
    {
        Objects.requireNonNull( orientation );

        DataScripter script = ProjectScriptGenerator.createEnsembleLabelScript( this.getDatabase(),
                                                                                this.getProjectId(),
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

            return ProjectUtilities.filter( unmodifiable, this.getDeclaredDataset( orientation )
                                                              .ensembleFilter() );
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
        Boolean usesGriddedData;

        usesGriddedData = switch ( orientation )
        {
            case LEFT -> this.leftUsesGriddedData;
            case RIGHT -> this.rightUsesGriddedData;
            case BASELINE -> this.baselineUsesGriddedData;
        };

        if ( usesGriddedData == null )
        {
            Database db = this.getDatabase();
            DataScripter script = new DataScripter( db );
            script.addLine( SELECT_1 );
            script.addLine( "FROM wres.ProjectSource PS" );
            script.addLine( "INNER JOIN wres.Source S" );
            script.addTab().addLine( "ON PS.source_id = S.source_id" );
            script.addLine( "WHERE PS.project_id = ?" );
            script.addArgument( this.getId() );
            script.addTab().addLine( "AND PS.member = ?" );
            script.addArgument( orientation.name()
                                           .toLowerCase() );
            script.addTab().addLine( "AND S.is_point_data = FALSE" );
            script.setMaxRows( 1 );

            try ( DataProvider provider = script.getData() )
            {
                // If there is a row, then gridded data is used.
                usesGriddedData = provider.next();
            }
            catch ( SQLException e )
            {
                throw new DataAccessException( "While attempting to determine whether gridded data were ingested.", e );
            }
            switch ( orientation )
            {
                case LEFT -> this.leftUsesGriddedData = usesGriddedData;
                case RIGHT -> this.rightUsesGriddedData = usesGriddedData;
                case BASELINE -> this.baselineUsesGriddedData = usesGriddedData;
            }
        }

        return usesGriddedData;
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
    public boolean save()
    {
        // Not already saved?
        if ( !this.performedInsert )
        {
            LOGGER.trace( "Attempting to save project." );

            DataScripter saveScript = this.getInsertSelectStatement();

            try
            {
                this.performedInsert = saveScript.execute() > 0;
            }
            catch ( SQLException e )
            {
                throw new DataAccessException( "While attempting to save the project.", e );
            }

            if ( this.performedInsert )
            {
                this.projectId = saveScript.getInsertedIds()
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
                    this.projectId = data.getLong( PROJECT_ID );
                }
                catch ( SQLException e )
                {
                    throw new DataAccessException( "While attempting to save the project.", e );
                }
            }
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Did I create Project ID {}? {}",
                          this.getId(),
                          this.performedInsert );
        }

        LOGGER.info( "The identity of the database project is '{}'.", this.getProjectId() );

        return this.performedInsert;
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
     * Performs operations that are needed for the project to run between ingest and evaluation.
     *
     * @throws DataAccessException if retrieval of data fails
     * @throws BadProjectException if zero features have intersecting data
     */

    void prepareAndValidate()
    {
        LOGGER.info( "Validating the project and loading preliminary metadata..." );

        LOGGER.trace( "prepareForExecution() entered" );

        // Validates that the required variables are present
        this.validateVariables();

        // Check for features that potentially have intersecting values.
        // The query in getIntersectingFeatures checks that there is some
        // data for each feature on each side, but does not guarantee pairs.
        synchronized ( this.featureLock )
        {
            this.setFeaturesAndFeatureGroups();
        }

        if ( this.features.isEmpty()
             && this.featureGroups.isEmpty() )
        {
            throw new BadProjectException( "Failed to identify any features with data on all required sides (left, right "
                                           + "and, when declared, baseline) for the variables and other declaration "
                                           + "supplied. Please check that the declaration is expected to produce some "
                                           + "features with time-series data on both sides of the pairing." );
        }

        // Validate any ensemble conditions
        this.validateEnsembleConditions();

        // Determine and set the variables to evaluate
        this.setVariablesToEvaluate();

        LOGGER.info( "Project validation and metadata loading is complete." );
    }

    /**
     * Sets the features and feature groups.
     * @throws DataAccessException if the features and/or feature groups could not be set
     */

    private void setFeaturesAndFeatureGroups()
    {
        LOGGER.debug( "Setting the features and feature groups for project {}.", this.getId() );

        Set<FeatureTuple> singletons = new HashSet<>(); // Singleton feature tuples
        Set<FeatureTuple> grouped = new HashSet<>(); // Multi-tuple groups

        // Gridded features? #74266
        // Yes
        if ( this.usesGriddedData( DatasetOrientation.RIGHT ) )
        {
            Set<FeatureTuple> griddedTuples = this.getGriddedFeatureTuples();
            singletons.addAll( griddedTuples );
        }
        // No
        else
        {
            Features fCache = this.getCaches()
                                  .getFeaturesCache();

            // At this point, features should already have been correlated by
            // the declaration or by a location service. In the latter case, the
            // WRES will have generated the List<Feature> and replaced them in
            // a new ProjectConfig, so this code cannot tell the difference.


            // Deal with the special case of singletons first
            Set<GeometryTuple> singletonFeatures = this.getDeclaredFeatures();

            // If there are no declared singletons, allow features to be discovered, but only if there are no declared
            // multi-feature groups.
            Set<GeometryGroup> declaredGroups = this.getDeclaredFeatureGroups();
            if ( !singletonFeatures.isEmpty() || declaredGroups.isEmpty() )
            {
                DataScripter script =
                        ProjectScriptGenerator.createIntersectingFeaturesScript( database,
                                                                                 this.getId(),
                                                                                 singletonFeatures,
                                                                                 this.hasBaseline(),
                                                                                 false );

                LOGGER.debug( "getIntersectingFeatures will run for singleton features: {}", script );
                Set<FeatureTuple> innerSingletons = this.readFeaturesFromScript( script, fCache );

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
                        ProjectScriptGenerator.createIntersectingFeaturesScript( database,
                                                                                 this.getId(),
                                                                                 groupedFeatures,
                                                                                 this.hasBaseline(),
                                                                                 true );

                LOGGER.debug( "getIntersectingFeatures will run for grouped features: {}", scriptForGroups );
                Set<FeatureTuple> innerGroups = this.readFeaturesFromScript( scriptForGroups, fCache );
                grouped.addAll( innerGroups );
                LOGGER.debug( "getIntersectingFeatures completed for grouped features, which identified {} features",
                              innerGroups.size() );
            }
        }

        // Filter the singleton features against any spatial mask, unless there is gridded data, which is masked upfront
        // Do this before forming the groups, which include singleton groups
        if ( !this.usesGriddedData( DatasetOrientation.RIGHT ) )
        {
            singletons = ProjectUtilities.filterFeatures( singletons, this.getDeclaration()
                                                                          .spatialMask() );
        }

        // Combine the singletons and feature groups into groups that contain one or more tuples
        ProjectUtilities.FeatureGroupsPlus
                groups = ProjectUtilities.getFeatureGroups( Collections.unmodifiableSet( singletons ),
                                                            Collections.unmodifiableSet( grouped ),
                                                            this.getDeclaration(),
                                                            this.getId() );

        Set<FeatureGroup> finalGroups = groups.featureGroups();

        // Filter the multi-group features against any spatial mask, unless there is gridded data, which is masked
        // upfront
        if ( !this.usesGriddedData( DatasetOrientation.RIGHT ) )
        {
            finalGroups = ProjectUtilities.filterFeatureGroups( finalGroups, this.getDeclaration()
                                                                                 .spatialMask() );
        }

        // Immutable on construction
        this.featureGroups = finalGroups;
        this.doNotPublish = groups.doNotPublish();

        LOGGER.debug( "Finished setting the feature groups for project {}. Discovered {} feature groups: {}.",
                      this.getId(),
                      this.featureGroups.size(),
                      this.featureGroups );

        // Features are the union of the singletons and grouped features
        this.featureGroups.stream()
                          .flatMap( next -> next.getFeatures()
                                                .stream() )
                          .forEach( singletons::add );
        this.features = Collections.unmodifiableSet( singletons );

        LOGGER.debug( "Finished setting the features for project {}. Discovered {} features: {}.",
                      this.getId(),
                      this.features.size(),
                      this.features );
    }

    /**
     * Checks that the union of ensemble conditions will select some data, otherwise throws an exception.
     *
     * @throws BadProjectException if the conditions select no data
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private void validateEnsembleConditions()
    {
        // Show all errors at once rather than drip-feeding
        Dataset left = this.getLeft();
        List<String> failedLeft = this.getInvalidEnsembleConditions( DatasetOrientation.LEFT, left );
        List<String> failed = new ArrayList<>( failedLeft );
        Dataset right = this.getRight();
        List<String> failedRight = this.getInvalidEnsembleConditions( DatasetOrientation.RIGHT, right );
        failed.addAll( failedRight );

        if ( this.hasBaseline() )
        {
            Dataset baseline = this.getBaseline()
                                   .dataset();
            List<String> failedBaseline = this.getInvalidEnsembleConditions( DatasetOrientation.BASELINE, baseline );
            failed.addAll( failedBaseline );
        }

        if ( !failed.isEmpty() )
        {
            throw new BadProjectException( "Of the filters that were defined for ensemble names, "
                                           + failed.size()
                                           + " of those filters did not select any data. Fix the declared filters to "
                                           + "ensure that each filter selects some data. The invalid filters are: "
                                           + failed
                                           + "." );
        }
    }

    /**
     * Validates the variables.
     */

    private void validateVariables()
    {
        boolean isVector;
        Variables variables = this.getCaches()
                                  .getVariablesCache();

        boolean leftTimeSeriesValid = true;
        boolean rightTimeSeriesValid = true;
        boolean baselineTimeSeriesValid = true;

        try
        {
            isVector = !( this.usesGriddedData( DatasetOrientation.LEFT ) ||
                          this.usesGriddedData( DatasetOrientation.RIGHT ) );

            // Validate the variable declaration against the data, when the declaration is present
            if ( isVector )
            {
                String name = this.getDeclaredVariableName( DatasetOrientation.LEFT );
                leftTimeSeriesValid = Objects.isNull( name )
                                      || variables.isValid( this.getId(),
                                                            DatasetOrientation.LEFT.name()
                                                                                   .toLowerCase(),
                                                            name );
            }

            if ( isVector )
            {
                String name = this.getDeclaredVariableName( DatasetOrientation.RIGHT );
                rightTimeSeriesValid = Objects.isNull( name )
                                       || variables.isValid( this.getId(),
                                                             DatasetOrientation.RIGHT.name()
                                                                                     .toLowerCase(),
                                                             name );
            }

            if ( isVector && this.hasBaseline() )
            {
                String name = this.getDeclaredVariableName( DatasetOrientation.BASELINE );
                baselineTimeSeriesValid = Objects.isNull( name )
                                          || variables.isValid( this.getId(),
                                                                DatasetOrientation.BASELINE.name()
                                                                                           .toLowerCase(),
                                                                name );
            }
        }
        catch ( SQLException | DataAccessException e )
        {
            throw new DataAccessException( "Could not determine whether the variables are valid.", e );
        }


        // If we're performing gridded evaluation, we can't check if our
        // variables are valid via normal means, so just return
        if ( !isVector )
        {
            LOGGER.info( "Preliminary metadata loading is complete." );
            return;
        }

        // Get the details of the invalid variables
        boolean valid = true;
        String message = "";
        if ( !leftTimeSeriesValid )
        {
            valid = false;
            message += System.lineSeparator();
            message += this.getInvalidVariablesMessage( variables, DatasetOrientation.LEFT );
        }
        if ( !rightTimeSeriesValid )
        {
            valid = false;
            message += System.lineSeparator();
            message += this.getInvalidVariablesMessage( variables, DatasetOrientation.RIGHT );
        }
        if ( !baselineTimeSeriesValid )
        {
            valid = false;
            message += System.lineSeparator();
            message += this.getInvalidVariablesMessage( variables, DatasetOrientation.BASELINE );
        }

        if ( !valid )
        {
            throw new BadProjectException( message );
        }
    }

    /**
     * Get the message about invalid variables.
     * @param variables the variables cache
     * @param orientation the orientation
     * @return the message
     */

    private String getInvalidVariablesMessage( Variables variables, DatasetOrientation orientation )
    {
        try
        {
            List<String> availableVariables = variables.getAvailableVariables( this.getId(),
                                                                               orientation.name()
                                                                                          .toLowerCase() );
            StringBuilder message = new StringBuilder();
            message.append( "    - There is no '" )
                   .append( this.getVariableName( orientation ) )
                   .append( "' data available for the " )
                   .append( orientation )
                   .append( " evaluation dataset." );

            if ( !availableVariables.isEmpty() )
            {
                message.append( " Available variable(s): " )
                       .append( availableVariables );
            }
            else
            {
                message.append( " There are no other available variables for use." );
            }

            return message.toString();
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "'"
                                           + this.getVariableName( orientation )
                                           + "' is not a valid "
                                           + orientation
                                           + "variable for evaluation. Possible alternatives could "
                                           + "not be found.",
                                           e );
        }
    }

    /**
     * Sets the variables to evaluate. Begins by looking at the declaration. If it cannot find a declared variable for 
     * any particular left/right/baseline context, it looks at the data instead. If there is more than one possible 
     * name and it does not exactly match the name identified for the other side of the pairing, then an exception is 
     * thrown because declaration is require to disambiguate. Otherwise, it chooses the single variable name and warns 
     * about the assumption made when using the data to disambiguate.
     *
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private void setVariablesToEvaluate()
    {
        // The set of possibilities to validate
        Set<String> leftNames = new HashSet<>();
        Set<String> rightNames = new HashSet<>();
        Set<String> baselineNames = new HashSet<>();

        boolean leftAuto = false;
        boolean rightAuto = false;
        boolean baselineAuto = false;

        // Left declared?
        if ( Objects.nonNull( this.getLeft()
                                  .variable() ) )
        {
            String name = this.getLeft()
                              .variable()
                              .name();
            leftNames.add( name );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( DatasetOrientation.LEFT );
            leftNames.addAll( names );
            leftAuto = true;
        }

        // Right declared?
        if ( Objects.nonNull( this.getRight()
                                  .variable() ) )
        {
            String name = this.getRight()
                              .variable()
                              .name();
            rightNames.add( name );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( DatasetOrientation.RIGHT );
            rightNames.addAll( names );
            rightAuto = true;
        }

        // Baseline declared?
        if ( this.hasBaseline() )
        {
            if ( Objects.nonNull( this.getBaseline()
                                      .dataset()
                                      .variable() ) )
            {
                String name = this.getBaseline()
                                  .dataset()
                                  .variable()
                                  .name();
                baselineNames.add( name );
            }
            // No, look at data
            else
            {
                Set<String> names = this.getVariableNameByInspectingData( DatasetOrientation.BASELINE );
                baselineNames.addAll( names );
                baselineAuto = true;
            }
        }

        LOGGER.debug( "While looking for variable names to evaluate, discovered {} on the LEFT side, {} on the RIGHT "
                      + "side and {} on the BASELINE side. LEFT autodetected: {}, RIGHT autodetected: {}, BASELINE "
                      + "auto-detected: {}.",
                      leftNames,
                      rightNames,
                      baselineNames,
                      leftAuto,
                      rightAuto,
                      baselineAuto );

        VariableNames variableNames = ProjectUtilities.getVariableNames( this.getDeclaration(),
                                                                         Collections.unmodifiableSet( leftNames ),
                                                                         Collections.unmodifiableSet( rightNames ),
                                                                         Collections.unmodifiableSet( baselineNames ) );

        this.leftVariable = variableNames.leftVariableName();
        this.rightVariable = variableNames.rightVariableName();
        this.baselineVariable = variableNames.baselineVariableName();

        ProjectUtilities.validateVariableNames( this.getDeclaredLeftVariableName(),
                                                this.getDeclaredRightVariableName(),
                                                this.getDeclaredBaselineVariableName(),
                                                this.getLeftVariableName(),
                                                this.getRightVariableName(),
                                                this.getBaselineVariableName(),
                                                this.hasBaseline() );
    }

    /**
     * Determines the possible variable names by inspecting the data.
     *
     * @param orientation the context
     * @return the possible variable names
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private Set<String> getVariableNameByInspectingData( DatasetOrientation orientation )
    {
        DataScripter script = ProjectScriptGenerator.createVariablesScript( this.getDatabase(),
                                                                            this.getId(),
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
            throw new DataAccessException(
                    "While attempting to determine the variable name for " + orientation + " data.", e );
        }

        return Collections.unmodifiableSet( names );
    }

    /**
     * @return the project identifier
     */

    private long getProjectId()
    {
        return this.projectId;
    }

    /**
     * Checks for any invalid ensemble conditions and returns a string representation of the invalid conditions.
     *
     * @param orientation the orientation of the source
     * @param dataset the source configuration whose ensemble conditions should be validated
     * @return a string representation of the invalid conditions 
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private List<String> getInvalidEnsembleConditions( DatasetOrientation orientation,
                                                       Dataset dataset )
    {
        List<String> failed = new ArrayList<>();

        EnsembleFilter filter = dataset.ensembleFilter();
        if ( Objects.nonNull( filter ) )
        {
            for ( String name : filter.members() )
            {
                DataScripter script = ProjectScriptGenerator.getIsValidEnsembleCondition( this.getDatabase(),
                                                                                          name,
                                                                                          this.getId(),
                                                                                          filter.exclude() );

                LOGGER.debug( "getIsValidEnsembleCondition will run: {}", script );

                try ( Connection connection = database.getConnection();
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
     * @return the declared features
     */
    private Set<GeometryTuple> getDeclaredFeatures()
    {
        if ( Objects.isNull( this.getDeclaration()
                                 .features() ) )
        {
            return Set.of();
        }

        return this.getDeclaration()
                   .features()
                   .geometries();
    }

    /**
     * @return the declared feature groups
     */
    private Set<GeometryGroup> getDeclaredFeatureGroups()
    {
        if ( Objects.isNull( this.getDeclaration()
                                 .featureGroups() ) )
        {
            return Set.of();
        }

        return this.getDeclaration()
                   .featureGroups()
                   .geometryGroups();
    }

    /**
     * Builds a set of gridded feature tuples. Assumes that all dimensions have the same tuple (i.e., cannot currently
     * pair grids with different features. Feature groupings are also not supported.
     *
     * @return a set of gridded feature tuples
     */

    private Set<FeatureTuple> getGriddedFeatureTuples()
    {
        LOGGER.debug( "Getting details of intersecting features for gridded data." );
        Set<Feature> innerGriddedFeatures = this.getGriddedFeatures()
                                                .get();
        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Feature nextFeature : innerGriddedFeatures )
        {
            Geometry geometry = MessageFactory.parse( nextFeature );
            GeometryTuple geoTuple;
            if ( this.hasBaseline() )
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
     * @return the feature tuples
     * @throws DataAccessException if the features could not be read
     */

    private Set<FeatureTuple> readFeaturesFromScript( DataScripter script, Features fCache )
    {
        Set<FeatureTuple> featureTuples = new HashSet<>();

        try ( Connection connection = this.database.getConnection();
              DataProvider dataProvider = script.buffer( connection ) )
        {
            while ( dataProvider.next() )
            {
                int leftId = dataProvider.getInt( "left_id" );
                Feature leftKey =
                        fCache.getFeatureKey( leftId );
                int rightId = dataProvider.getInt( "right_id" );
                Feature rightKey =
                        fCache.getFeatureKey( rightId );
                Feature baselineKey = null;

                // Baseline column will only be there when baseline exists.
                if ( hasBaseline() )
                {
                    int baselineId =
                            dataProvider.getInt( "baseline_id" );

                    // JDBC getInt returns 0 when not found. All primary key
                    // columns should start at 1.
                    if ( baselineId > 0 )
                    {
                        baselineKey =
                                fCache.getFeatureKey( baselineId );
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

    private DataScripter getInsertSelectStatement()
    {
        Database db = this.getDatabase();
        DataScripter script = new DataScripter( db );
        script.setUseTransaction( true );

        script.retryOnSerializationFailure();
        script.retryOnUniqueViolation();

        script.setHighPriority( true );

        script.addLine( "INSERT INTO wres.Project ( hash, project_name )" );
        script.addTab().addLine( "SELECT ?, ?" );

        script.addArgument( this.getHash() );
        script.addArgument( this.getProjectName() );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( SELECT_1 );
        script.addTab( 2 ).addLine( "FROM wres.Project P" );
        script.addTab( 2 ).addLine( "WHERE P.hash = ?" );

        script.addArgument( this.getHash() );

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
     * @return the caches
     */
    private DatabaseCaches getCaches()
    {
        return this.caches;
    }

    /**
     * @return the gridded features cache
     *
     */
    private GriddedFeatures getGriddedFeatures()
    {
        Objects.requireNonNull( this.griddedFeatures,
                                "Cannot query gridded feature availablity without a gridded features cache." );

        return this.griddedFeatures;
    }

    /**
     * @see #getDeclaredLeftVariableName()
     * @return the name of the left variable or null if determined from the data and the data has yet to be inspected
     */
    private String getLeftVariableName()
    {
        if ( Objects.isNull( this.leftVariable ) )
        {
            return this.getDeclaredLeftVariableName();
        }

        return this.leftVariable;
    }

    /**
     * @return the name of the right variable or null if determined from the data and the data has yet to be inspected
     */
    private String getRightVariableName()
    {
        if ( Objects.isNull( this.rightVariable ) )
        {
            return this.getDeclaredRightVariableName();
        }

        return this.rightVariable;
    }

    /**
     * @return the name of the baseline variable or null if determined from the data and the data has yet to be 
     *            inspected
     */
    private String getBaselineVariableName()
    {
        if ( Objects.isNull( this.baselineVariable ) )
        {
            return this.getDeclaredBaselineVariableName();
        }

        return this.baselineVariable;
    }

    /**
     * @param orientation the orientation
     * @return the declared variable name
     */
    private String getDeclaredVariableName( DatasetOrientation orientation )
    {
        Objects.requireNonNull( orientation );

        return switch ( orientation )
        {
            case LEFT -> this.getDeclaredLeftVariableName();
            case RIGHT -> this.getDeclaredRightVariableName();
            case BASELINE -> this.getDeclaredBaselineVariableName();
        };
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
}

