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
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.FeaturePool;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.project.ProjectUtilities.VariableNames;
import wres.io.retrieval.DataAccessException;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Provides helpers related to the project declaration in combination with the ingested time-series data.
 */
public class DatabaseProject implements Project
{
    private static final String EXPECTED_LEFT_OR_RIGHT_OR_BASELINE = "': expected LEFT or RIGHT or BASELINE.";

    private static final String UNEXPECTED_CONTEXT = "Unexpected context '";

    private static final String SELECT_1 = "SELECT 1";

    private static final String PROJECT_ID = "project_id";

    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseProject.class );

    /**
     * Protects access and generation of the feature collection
     */
    private final Object featureLock = new Object();

    private final ProjectConfig projectConfig;
    private final Database database;
    private final DatabaseCaches caches;

    private long projectId;

    /**
     * The measurement unit, which is the declared unit, if available, else the most commonly occurring unit among the 
     * project sources, with a preference for the mostly commonly occurring right-sided source unit. See 
     * {@link ProjectScriptGenerator#createUnitScript(Database, int)}.
     */

    private String measurementUnit = null;

    /**
     * The set of all features pertaining to the project
     */
    private Set<FeatureTuple> features;

    /**
     * The feature groups related to the project.
     */

    private Set<FeatureGroup> featureGroups;

    /**
     * Indicates whether or not this project was inserted on upon this
     * execution of the project
     */
    private boolean performedInsert;

    /**
     * The overall hash for the data sources used in the project
     */
    private final String hash;

    private Boolean leftUsesGriddedData = null;
    private Boolean rightUsesGriddedData = null;
    private Boolean baselineUsesGriddedData = null;

    /** The left-ish variable to evaluate. */
    private String leftVariable;

    /** The right-ish variable to evaluate. */
    private String rightVariable;

    /** The baseline-ish variable to evaluate. */
    private String baselineVariable;

    /** The desired time scale. */
    private TimeScaleOuter desiredTimeScale;

    public DatabaseProject( Database database,
                            DatabaseCaches caches,
                            ProjectConfig projectConfig,
                            String hash )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( hash );
        this.database = database;
        this.projectConfig = projectConfig;
        this.hash = hash;
        this.caches = caches;
        // Read only from now on, post ingest
        this.caches.setReadOnly();
    }

    @Override
    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
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
        String declaredUnit = this.getProjectConfig()
                                  .getPair()
                                  .getUnit();
        if ( Objects.isNull( this.measurementUnit ) && Objects.nonNull( declaredUnit ) && !declaredUnit.isBlank() )
        {
            this.measurementUnit = declaredUnit;

            LOGGER.debug( "Determined the measurement unit from the project declaration as {}.",
                          this.measurementUnit );
        }

        // Still not available? Then analyze the unit.
        if ( Objects.isNull( this.measurementUnit ) )
        {
            if ( Objects.isNull( this.getId() ) )
            {
                throw new IllegalArgumentException( "Cannot analyze the measurement unit for the project until the "
                                                    + "project identity is known." );
            }

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
     * Returns the desired time scale. In order of availability, this is:
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
     * @return the desired time scale or null if unknown
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

        // Use the declared time scale
        TimeScaleOuter declaredScale = ConfigHelper.getDesiredTimeScale( this.getProjectConfig()
                                                                             .getPair() );
        if ( Objects.nonNull( declaredScale ) )
        {
            LOGGER.trace( "Discovered that the desired time scale was declared explicitly as {}.",
                          this.desiredTimeScale );

            this.desiredTimeScale = declaredScale;

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

        Inputs inputDeclaration = this.getProjectConfig()
                                      .getInputs();

        // Look for the LCS among the declared inputs
        if ( Objects.nonNull( inputDeclaration ) )
        {
            Set<TimeScaleOuter> declaredExistingTimeScales = new HashSet<>();
            TimeScaleConfig leftScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();
            TimeScaleConfig rightScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();

            if ( Objects.nonNull( leftScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( leftScaleConfig ) );
            }
            if ( Objects.nonNull( rightScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( rightScaleConfig ) );
            }
            if ( Objects.nonNull( inputDeclaration.getBaseline() )
                 && Objects.nonNull( inputDeclaration.getBaseline().getExistingTimeScale() ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( inputDeclaration.getBaseline()
                                                                                   .getExistingTimeScale() ) );
            }

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
        }

        return this.desiredTimeScale;
    }

    /**
     * Performs operations that are needed for the project to run between ingest and evaluation.
     * 
     * @throws DataAccessException if retrieval of data fails
     * @throws NoDataException if zero features have intersecting data
     */
    
    void prepareAndValidate()
    {
        LOGGER.info( "Validating the project and loading preliminary metadata..." );
        
        LOGGER.trace( "prepareForExecution() entered" );
        Database db = this.getDatabase();

        // Validates that the required variables are present
        this.validateVariables();

        // Check for features that potentially have intersecting values.
        // The query in getIntersectingFeatures checks that there is some
        // data for each feature on each side, but does not guarantee pairs.
        synchronized ( this.featureLock )
        {
            this.setFeaturesAndFeatureGroups( db );
        }

        if ( this.features.isEmpty() && this.featureGroups.isEmpty() )
        {
            throw new NoDataException( "Failed to identify any features with data on both the left and right sides for "
                                       + "the variables and other declaration supplied. Please check that the declaration is expected to "
                                       + "produce some features with time-series data on both sides of the pairing." );
        }

        // Validate any ensemble conditions
        this.validateEnsembleConditions();

        // Determine and set the variables to evaluate
        this.setVariablesToEvaluate();

        LOGGER.info( "Project validation and metadata loading is complete." );
    }

    /**
     * Returns the set of {@link FeatureTuple} for the project. If none have been
     * created yet, then it is evaluated. If there is no specification within
     * the configuration, all locations that have been ingested are retrieved
     * @return A set of all feature tuples involved in the project
     * cannot be retrieved from the database
     * @throws IllegalStateException if the features have not been set. Call {@link #prepareAndValidate()} first.
     */
    @Override
    public Set<FeatureTuple> getFeatures()
    {
        if ( Objects.isNull( this.features ) )
        {
            throw new IllegalStateException( "The features have not been set." );
        }

        return Collections.unmodifiableSet( this.features );
    }

    /**
     * Returns the set of {@link FeatureGroup} for the project.
     * @return A set of all feature groups involved in the project
     * @throws IllegalStateException if the features have not been set. Call {@link #prepareAndValidate()} first.
     */
    @Override
    public Set<FeatureGroup> getFeatureGroups()
    {
        if ( Objects.isNull( this.featureGroups ) )
        {
            throw new IllegalStateException( "The feature groups have not been set." );
        }

        return Collections.unmodifiableSet( this.featureGroups );
    }

    /**
     * @param lrb The side of data for which the variable is required
     * @return The declared data source for the specified orientation
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    @Override
    public DataSourceConfig getDeclaredDataSource( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case LEFT:
                return this.getLeft();
            case RIGHT:
                return this.getRight();
            case BASELINE:
                return this.getBaseline();
            default:
                throw new IllegalArgumentException( UNEXPECTED_CONTEXT + lrb
                                                    + EXPECTED_LEFT_OR_RIGHT_OR_BASELINE );
        }
    }

    /**
     * @param lrb The side of data for which the variable is required
     * @return The name of the variable for the specified side of data
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    @Override
    public String getVariableName( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case LEFT:
                return this.getLeftVariableName();
            case RIGHT:
                return this.getRightVariableName();
            case BASELINE:
                return this.getBaselineVariableName();
            default:
                throw new IllegalArgumentException( UNEXPECTED_CONTEXT + lrb
                                                    + "': expected LEFT or "
                                                    + "RIGHT or BASELINE." );
        }
    }

    /**
     * @param lrb The side of data for which the variable is required
     * @return The name of the declared variable for the specified side of data
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    @Override
    public String getDeclaredVariableName( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case LEFT:
                return this.getDeclaredLeftVariableName();
            case RIGHT:
                return this.getDeclaredRightVariableName();
            case BASELINE:
                return this.getDeclaredBaselineVariableName();
            default:
                throw new IllegalArgumentException( UNEXPECTED_CONTEXT + lrb
                                                    + "': expected LEFT or "
                                                    + "RIGHT or BASELINE." );
        }
    }

    /**
     * @return the earliest analysis duration
     */

    @Override
    public Duration getEarliestAnalysisDuration()
    {
        return ConfigHelper.getEarliestAnalysisDuration( this.getProjectConfig() );
    }

    /**
     * @return the latest analysis duration
     */

    @Override
    public Duration getLatestAnalysisDuration()
    {
        return ConfigHelper.getLatestAnalysisDuration( this.getProjectConfig() );
    }

    /**
     * @return the earliest possible day in a season. NULL unless specified in the configuration
     */
    @Override
    public MonthDay getEarliestDayInSeason()
    {
        return ConfigHelper.getEarliestDayInSeason( this.getProjectConfig() );
    }

    /**
     * @return the latest possible day in a season. NULL unless specified in the configuration
     */
    @Override
    public MonthDay getLatestDayInSeason()
    {
        return ConfigHelper.getLatestDayInSeason( this.getProjectConfig() );
    }

    @Override
    public boolean usesGriddedData( DataSourceConfig dataSourceConfig )
    {
        Boolean usesGriddedData;

        LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(), dataSourceConfig );

        switch ( lrb )
        {
            case LEFT:
                usesGriddedData = this.leftUsesGriddedData;
                break;
            case RIGHT:
                usesGriddedData = this.rightUsesGriddedData;
                break;
            case BASELINE:
                usesGriddedData = this.baselineUsesGriddedData;
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized enumeration value in this context, '"
                                                    + lrb
                                                    + "'." );
        }

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
            script.addArgument( lrb.toString().toLowerCase() );
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
            switch ( lrb )
            {
                case LEFT:
                    this.leftUsesGriddedData = usesGriddedData;
                    break;
                case RIGHT:
                    this.rightUsesGriddedData = usesGriddedData;
                    break;
                case BASELINE:
                    this.baselineUsesGriddedData = usesGriddedData;
                    break;
                default:
                    throw new IllegalArgumentException( "Unrecognized enumeration value in this context, '"
                                                        + lrb
                                                        + "'." );
            }
        }

        return usesGriddedData;
    }

    /**
     * Returns unique identifier for this project's dataset
     * @return The unique ID
     */
    @Override
    public String getHash()
    {
        return this.hash;
    }

    /**
     * @return Whether or not baseline data is involved in the project
     */
    @Override
    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    /**
     * @return Whether or not there is a generated baseline
     */
    @Override
    public boolean hasGeneratedBaseline()
    {
        return ConfigHelper.hasGeneratedBaseline( this.getBaseline() );
    }

    /**
     * @return the project identity
     */

    @Override
    public long getId()
    {
        return this.projectId;
    }

    /**
     * Return <code>true</code> if the project uses probability thresholds, otherwise <code>false</code>.
     * 
     * @return Whether or not the project uses probability thresholds
     */
    @Override
    public boolean hasProbabilityThresholds()
    {
        return ConfigHelper.hasProbabilityThresholds( this.getProjectConfig() );
    }

    /**
     * Saves the project.
     * @return true if this call resulted in the project being saved, false otherwise
     * @throws DataAccessException if the save fails for any reason
     */
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
     * Sets the features and feature groups.
     * @param db the database
     * @throws DataAccessException if the features and/or feature groups could not be set
     */

    private void setFeaturesAndFeatureGroups( Database db )
    {
        LOGGER.debug( "Setting the features and feature groups for project {}.", this.getId() );
        Pair<Set<FeatureTuple>, Set<FeatureGroup>> innerFeatures = this.getIntersectingFeatures( db );

        // Immutable on construction
        this.featureGroups = innerFeatures.getRight();

        LOGGER.debug( "Finished setting the feature groups for project {}. Discovered {} feature groups: {}.",
                      this.getId(),
                      this.featureGroups.size(),
                      this.featureGroups );

        // Features are the union of the singletons and grouped features
        Set<FeatureTuple> singletons = new HashSet<>( innerFeatures.getLeft() );
        this.featureGroups.stream()
                          .flatMap( next -> next.getFeatures().stream() )
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
     * @throws NoDataException if the conditions select no data
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private void validateEnsembleConditions()
    {
        DataSourceConfig left = this.getProjectConfig().getInputs().getLeft();
        DataSourceConfig right = this.getProjectConfig().getInputs().getRight();
        DataSourceConfig baseline = this.getProjectConfig().getInputs().getBaseline();

        // Show all errors at once rather than drip-feeding
        List<String> failed = new ArrayList<>();
        List<String> failedLeft = this.getInvalidEnsembleConditions( LeftOrRightOrBaseline.LEFT, left );
        List<String> failedRight = this.getInvalidEnsembleConditions( LeftOrRightOrBaseline.RIGHT, right );
        List<String> failedBaseline = this.getInvalidEnsembleConditions( LeftOrRightOrBaseline.BASELINE, baseline );

        failed.addAll( failedLeft );
        failed.addAll( failedRight );
        failed.addAll( failedBaseline );

        if ( !failed.isEmpty() )
        {
            throw new NoDataException( "Of the filters that were defined for ensemble names, "
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
            isVector = ! ( this.usesGriddedData( this.getDeclaredDataSource( LeftOrRightOrBaseline.LEFT ) ) ||
                           this.usesGriddedData( this.getDeclaredDataSource( LeftOrRightOrBaseline.RIGHT ) ) );

            // Validate the variable declaration against the data, when the declaration is present
            if ( isVector )
            {
                String name = this.getDeclaredVariableName( LeftOrRightOrBaseline.LEFT );
                leftTimeSeriesValid = Objects.isNull( name )
                                      || variables.isValid( this.getId(),
                                                            LeftOrRightOrBaseline.LEFT.value(),
                                                            name );
            }

            if ( isVector )
            {
                String name = this.getDeclaredVariableName( LeftOrRightOrBaseline.RIGHT );
                rightTimeSeriesValid = Objects.isNull( name )
                                       || variables.isValid( this.getId(),
                                                             LeftOrRightOrBaseline.RIGHT.value(),
                                                             name );
            }

            if ( isVector && this.hasBaseline() )
            {
                String name = this.getDeclaredVariableName( LeftOrRightOrBaseline.BASELINE );
                baselineTimeSeriesValid = Objects.isNull( name ) || variables.isValid( this.getId(),
                                                                                       LeftOrRightOrBaseline.BASELINE.value(),
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
            message += this.getInvalidVariablesMessage( variables, LeftOrRightOrBaseline.LEFT );
        }
        if ( !rightTimeSeriesValid )
        {
            valid = false;
            message += System.lineSeparator();
            message += this.getInvalidVariablesMessage( variables, LeftOrRightOrBaseline.RIGHT );
        }
        if ( !baselineTimeSeriesValid )
        {
            valid = false;
            message += System.lineSeparator();
            message += this.getInvalidVariablesMessage( variables, LeftOrRightOrBaseline.BASELINE );
        }

        if ( !valid )
        {
            throw new NoDataException( message );
        }
    }

    /**
     * Get the message about invalid variables.
     * @param variables the variables cache
     * @param lrb the orientation
     * @return the message
     */

    private String getInvalidVariablesMessage( Variables variables, LeftOrRightOrBaseline lrb )
    {
        try
        {
            List<String> availableVariables = variables.getAvailableVariables( this.getId(),
                                                                               lrb.value() );
            StringBuilder message = new StringBuilder();
            message.append( "There is no '"
                            + this.getVariableName( lrb )
                            + "' data available for the "
                            + lrb
                            + " evaluation dataset." );

            if ( !availableVariables.isEmpty() )
            {
                message.append( " Available variable(s):" );
                for ( String variable : availableVariables )
                {
                    message.append( System.lineSeparator() )
                           .append( "    " )
                           .append( variable );
                }
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
                                           + this.getVariableName( lrb )
                                           + "' is not a valid "
                                           + lrb
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
        if ( Objects.nonNull( this.getLeft().getVariable() ) )
        {
            String name = this.getLeft().getVariable().getValue();
            leftNames.add( name );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( LeftOrRightOrBaseline.LEFT );
            leftNames.addAll( names );
            leftAuto = true;
        }

        // Right declared?
        if ( Objects.nonNull( this.getRight().getVariable() ) )
        {
            String name = this.getRight().getVariable().getValue();
            rightNames.add( name );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( LeftOrRightOrBaseline.RIGHT );
            rightNames.addAll( names );
            rightAuto = true;
        }

        // Baseline declared?
        if ( this.hasBaseline() )
        {
            if ( Objects.nonNull( this.getBaseline().getVariable() ) )
            {
                String name = this.getBaseline().getVariable().getValue();
                baselineNames.add( name );
            }
            // No, look at data
            else
            {
                Set<String> names = this.getVariableNameByInspectingData( LeftOrRightOrBaseline.BASELINE );
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

        VariableNames variableNames = ProjectUtilities.getVariableNames( this.getProjectConfig(),
                                                                         Collections.unmodifiableSet( leftNames ),
                                                                         Collections.unmodifiableSet( rightNames ),
                                                                         Collections.unmodifiableSet( baselineNames ) );

        this.leftVariable = variableNames.getLeftVariableName();
        this.rightVariable = variableNames.getRightVariableName();
        this.baselineVariable = variableNames.getBaselineVariableName();

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
     * @param lrb the context
     * @return the possible variable names
     * @throws DataAccessException if the variable information could not be determined from the data
     * @throws ProjectConfigException if declaration is required to disambiguate the variable name
     */

    private Set<String> getVariableNameByInspectingData( LeftOrRightOrBaseline lrb )
    {
        DataScripter script = ProjectScriptGenerator.createVariablesScript( this.getDatabase(), this.getId(), lrb );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "The script for auto-detecting variables on the {} side will run with parameters {}:{}{}",
                          lrb,
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
            throw new DataAccessException( "While attempting to determine the variable name for " + lrb + " data.", e );
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
     * @param lrb the orientation of the source
     * @param config the source configuration whose ensemble conditions should be validated
     * @return a string representation of the invalid conditions 
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private List<String> getInvalidEnsembleConditions( LeftOrRightOrBaseline lrb,
                                                       DataSourceConfig config )
    {
        List<String> failed = new ArrayList<>();

        if ( Objects.nonNull( config ) && !config.getEnsemble().isEmpty() )
        {
            List<EnsembleCondition> conditions = config.getEnsemble();
            for ( EnsembleCondition condition : conditions )
            {
                DataScripter script = ProjectScriptGenerator.getIsValidEnsembleCondition( this.getDatabase(),
                                                                                          condition.getName(),
                                                                                          this.getId(),
                                                                                          condition.isExclude() );

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
                                    new ToStringBuilder( condition,
                                                         ToStringStyle.SHORT_PREFIX_STYLE ).append( "orientation", lrb )
                                                                                           .append( "name",
                                                                                                    condition.getName() )
                                                                                           .append( "exclude",
                                                                                                    condition.isExclude() );

                            failed.add( builder.toString() );
                        }
                    }
                }
                catch ( SQLException e )
                {
                    throw new DataAccessException( "While attempting validate ensemble conditions.", e );
                }

                LOGGER.debug( "getIntersectingFeatures finished run: {}", script );
            }
        }

        return Collections.unmodifiableList( failed );
    }

    /**
     * Get a set of features for this project with intersecting data.
     * Does not check if the data is pairable, simply checks that there is data
     * on each of the left and right for this variable at a given feature.
     * @param database The database to use.
     * @return The sets of features to evaluate, ungrouped on the left side and grouped on the right side
     * @throws DataAccessException if the intersecting features could not be determined
     */

    private Pair<Set<FeatureTuple>, Set<FeatureGroup>> getIntersectingFeatures( Database database )
    {
        Set<FeatureTuple> singletons = new HashSet<>(); // Singleton feature tuples
        Set<FeatureTuple> grouped = new HashSet<>(); // Multi-tuple groups

        // Gridded features? #74266
        // Yes
        if ( this.usesGriddedData( this.getRight() ) )
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
            List<Feature> singletonFeatures = this.getProjectConfig()
                                                  .getPair()
                                                  .getFeature();

            // If there are no declared singletons, allow features to be discovered, but only if there are no declared
            // multi-feature groups. TODO: consider whether zero declared features should be supported in future
            List<FeaturePool> declaredGroups = this.getProjectConfig()
                                                   .getPair()
                                                   .getFeatureGroup();
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
            List<Feature> groupedFeatures = declaredGroups.stream()
                                                          .flatMap( next -> next.getFeature().stream() )
                                                          .collect( Collectors.toList() );

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

        // Combine the singletons and feature groups into groups that contain one or more tuples
        Set<FeatureGroup> groups = ProjectUtilities.getFeatureGroups( Collections.unmodifiableSet( singletons ),
                                                                      Collections.unmodifiableSet( grouped ),
                                                                      this.getProjectConfig()
                                                                          .getPair(),
                                                                      this.getId() );

        return Pair.of( Collections.unmodifiableSet( singletons ), Collections.unmodifiableSet( groups ) );
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
        Features fCache = this.getCaches()
                              .getFeaturesCache();
        Set<FeatureKey> griddedFeatures = fCache.getGriddedFeatures();
        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( FeatureKey nextFeature : griddedFeatures )
        {
            Geometry geometry = MessageFactory.parse( nextFeature );
            GeometryTuple geoTuple = null;
            if ( this.hasBaseline() )
            {
                geoTuple = MessageFactory.getGeometryTuple( geometry, geometry, geometry );
            }
            else
            {
                geoTuple = MessageFactory.getGeometryTuple( geometry, geometry, null );
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
                FeatureKey leftKey =
                        fCache.getFeatureKey( leftId );
                int rightId = dataProvider.getInt( "right_id" );
                FeatureKey rightKey =
                        fCache.getFeatureKey( rightId );
                FeatureKey baselineKey = null;

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
        return this.projectConfig.getName();
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
     * @see #getDeclaredLeftVariableName()
     * @return The name of the left variable or null if determined from the data and the data has yet to be inspected
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
     * @return The name of the right variable or null if determined from the data and the data has yet to be inspected
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
     * @return The name of the baseline variable or null if determined from the data and the data has yet to be 
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
     * @see #getLeftVariableName()
     * @return The declared left variable name or null if undeclared
     */
    private String getDeclaredLeftVariableName()
    {
        return ConfigHelper.getVariableName( this.getLeft() );
    }

    /**
     * @see #getRightVariableName()
     * @return The declared right variable name or null if undeclared
     */
    private String getDeclaredRightVariableName()
    {
        return ConfigHelper.getVariableName( this.getRight() );
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
            variableName = ConfigHelper.getVariableName( this.getBaseline() );
        }

        return variableName;
    }

    /**
     * @return The left hand data source configuration
     */
    private DataSourceConfig getLeft()
    {
        return this.projectConfig.getInputs().getLeft();
    }

    /**
     * @return The right hand data source configuration
     */
    private DataSourceConfig getRight()
    {
        return this.projectConfig.getInputs().getRight();
    }

    /**
     * @return The baseline data source configuration
     */
    private DataSourceConfig getBaseline()
    {
        return this.projectConfig.getInputs().getBaseline();
    }
}

