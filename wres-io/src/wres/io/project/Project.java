package wres.io.project;

import java.sql.SQLException;
import java.time.Duration;
import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationBoundsType;
import wres.config.generated.DurationUnit;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.FeatureTuple;
import wres.datamodel.FeatureKey;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.system.SystemSettings;
import wres.util.CalculationException;

/**
 * Encapsulates operations involved with interpreting unpredictable data within the
 * project configuration, storing the results of expensive calculations, and forming database
 * statements based solely on the configured specifications
 */
public class Project
{
    private static final String SELECT_1 = "SELECT 1";

    private static final String PROJECT_ID = "project_id";

    private static final Logger LOGGER = LoggerFactory.getLogger( Project.class );

    /**
     * The member identifier for left handed data in the database
     */
    public static final String LEFT_MEMBER = "left";

    /**
     * The member identifier for right handed data in the database
     */
    public static final String RIGHT_MEMBER = "right";

    /**
     * The member identifier for baseline data in the database
     */
    public static final String BASELINE_MEMBER = "baseline";

    /**
     * Protects access and generation of the feature collection
     */
    private final Object featureLock = new Object();

    private Integer projectID = null;
    private final ProjectConfig projectConfig;
    private final SystemSettings systemSettings;
    private final Database database;
    private final Executor executor;
    private final Variables variablesCache;
    private final Features featuresCache;
    private final Ensembles ensemblesCache;

    /**
     * The set of all features pertaining to the project
     */
    private Set<FeatureTuple> features;

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

    public Project( SystemSettings systemSettings,
                    Database database,
                    Executor executor,
                    ProjectConfig projectConfig,
                    String hash )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( hash );
        this.systemSettings = systemSettings;
        this.database = database;
        this.executor = executor;
        this.projectConfig = projectConfig;
        this.hash = hash;
        this.variablesCache = new Variables( database );
        this.featuresCache = new Features( database );
        this.ensemblesCache = new Ensembles( database );
    }

    public SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }
    public Database getDatabase()
    {
        return this.database;
    }

    public Executor getExecutor()
    {
        return this.executor;
    }

    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    public Variables getVariablesCache()
    {
        return this.variablesCache;
    }

    public Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    /**
     * Performs operations that are needed for the project to run between ingest and evaluation.
     * 
     * @throws SQLException if retrieval of data from the database fails
     * @throws CalculationException if required calculations could not be completed
     * @throws NoDataException if zero features have intersecting data
     */
    public void prepareForExecution() throws SQLException
    {
        LOGGER.trace( "prepareForExecution() entered" );
        Database database = this.getDatabase();

        // Check for features that potentially have intersecting values.
        // The query in getIntersectingFeatures checks that there is some
        // data for each feature on each side, but does not guarantee pairs.
        synchronized ( this.featureLock )
        {
            LOGGER.debug( "Features so far: {}", this.features );
            this.features = this.getIntersectingFeatures( database );
            LOGGER.debug( "Features after getting intersecting features: {}",
                          this.features );
        }

        if ( this.features.isEmpty() )
        {
            throw new NoDataException( "No features had data on both the left and the right for the variables "
                                       + "specified." );
        }
    }

    /**
     * Get a set of features for this project with intersecting data.
     * Does not check if the data is pairable, simply checks that there is data
     * on each of the left and right for this variable at a given feature.
     * @param database The database to use.
     * @return The Set of FeatureDetails with some data on each side
     */

    private Set<FeatureTuple> getIntersectingFeatures( Database database )
            throws SQLException
    {
        Set<FeatureTuple> intersectingFeatures;
        Features featuresCache = this.getFeaturesCache();

        // Gridded features? #74266
        // Yes
        if ( this.usesGriddedData( this.getRight() ) )
        {
            LOGGER.debug( "Getting details of intersecting features for gridded data." );
            intersectingFeatures = featuresCache.getGriddedDetails( this );
        }
        // No
        else
        {
            intersectingFeatures = new HashSet<>();

            // At this point, features should already have been correlated by
            // the declaration or by a location service. In the latter case, the
            // WRES will have generated the List<Feature> and replaced them in
            // a new ProjectConfig, so this code cannot tell the difference.
            DataScripter script =
                    ProjectScriptGenerator.createIntersectingFeaturesScript( database,
                                                                             this.getId(),
                                                                             this.getProjectConfig()
                                                                                 .getPair()
                                                                                 .getFeature(),
                                                                             this.hasBaseline() );

            LOGGER.debug( "getIntersectingFeatures will run: {}", script );

            try ( DataProvider dataProvider = script.buffer() )
            {
                while ( dataProvider.next() )
                {
                    int leftId = dataProvider.getInt( "left_id" );
                    FeatureKey leftKey =
                            featuresCache.getFeatureKey( leftId );
                    int rightId = dataProvider.getInt( "right_id" );
                    FeatureKey rightKey =
                            featuresCache.getFeatureKey( rightId );
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
                                    featuresCache.getFeatureKey( baselineId );
                        }
                    }

                    FeatureTuple featureTuple = new FeatureTuple( leftKey,
                                                                  rightKey,
                                                                  baselineKey );
                    intersectingFeatures.add( featureTuple );
                }
            }

            LOGGER.debug( "getIntersectingFeatures finished run: {}", script );
        }

        LOGGER.info( "Discovered {} features with data on both the left and right sides (statistics should "
                     + "be expected for this many features at most).",
                     intersectingFeatures.size() );

        return Collections.unmodifiableSet( intersectingFeatures );
    }


    /**
     * Returns the set of FeaturesDetails for the project. If none have been
     * created yet, then it is evaluated. If there is no specification within
     * the configuration, all locations that have been ingested are retrieved
     * @return A set of all FeatureDetails involved in the project
     * @throws SQLException Thrown if details about the project's features
     * cannot be retrieved from the database
     */
    public Set<FeatureTuple> getFeatures() throws SQLException
    {
        Database database = this.getDatabase();

        synchronized ( this.featureLock )
        {
            if ( this.features == null )
            {
                LOGGER.debug( "getFeatures(): no features found, populating." );
                this.features = this.getIntersectingFeatures( database );
            }
        }

        return Collections.unmodifiableSet( this.features );
    }

    /**
     * Returns a set of <code>ensemble_id</code> that should be included or excluded. The empty set should be 
     * interpreted as no constraints existing and, hence, that all possible <code>ensemble_id</code> should be included.
     * 
     * @param dataType the data type
     * @param include is true to search for constraints to include, false to exclude
     * @return the members to include
     * @throws SQLException if the ensemble identifiers could not be retrieved
     * @throws NullPointerException if the dataType is null
     * @throws IllegalArgumentException if the dataType declaration is invalid
     */

    public Set<Long> getEnsembleMembersToFilter( LeftOrRightOrBaseline dataType, boolean include )
            throws SQLException
    {
        Objects.requireNonNull( dataType );

        DataSourceConfig config = null;
        switch ( dataType )
        {
            case LEFT:
                config = this.getProjectConfig().getInputs().getLeft();
                break;
            case RIGHT:
                config = this.getProjectConfig().getInputs().getRight();
                break;
            case BASELINE:
                config = this.getProjectConfig().getInputs().getBaseline();
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized data type '" + dataType + "'." );

        }

        Set<Long> returnMe = new TreeSet<>();

        if ( Objects.nonNull( config ) && !config.getEnsemble().isEmpty() )
        {
            List<EnsembleCondition> conditions = config.getEnsemble();
            Ensembles ensemblesCache = this.getEnsemblesCache();

            for ( EnsembleCondition condition : conditions )
            {
                if ( condition.isExclude() != include )
                {
                    returnMe.addAll( ensemblesCache.getEnsembleIDs( condition ) );
                }
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * @return The left hand data source configuration
     */
    public DataSourceConfig getLeft()
    {
        return this.projectConfig.getInputs().getLeft();
    }

    /**
     * @return The right hand data source configuration
     */
    public DataSourceConfig getRight()
    {
        return this.projectConfig.getInputs().getRight();
    }

    /**
     * @return The baseline data source configuration
     */
    public DataSourceConfig getBaseline()
    {
        return this.projectConfig.getInputs().getBaseline();
    }


    /**
     * @return The name of the left variable
     */
    public String getLeftVariableName()
    {
        return this.getLeft().getVariable().getValue();
    }


    /**
     * @return The name of the right variable
     */
    public String getRightVariableName()
    {
        return this.getRight().getVariable().getValue();
    }

    /**
     * @return The name of the baseline variable
     */
    public String getBaselineVariableName()
    {
        String name = null;
        if ( this.hasBaseline() )
        {
            name = this.getBaseline().getVariable().getValue();
        }
        return name;
    }


    /**
     * Returns the earliest analysis duration associated with the project or <code>null</code>.
     * 
     * @return the earliest analysis duration or null
     */

    public Duration getEarliestAnalysisDurationOrNull()
    {
        Duration returnMe = null;

        if ( Objects.nonNull( this.getProjectConfig().getPair().getAnalysisDurations() ) )
        {
            DurationBoundsType analysisDurations = this.getProjectConfig()
                                                       .getPair()
                                                       .getAnalysisDurations();

            returnMe = this.getDurationOrNull( analysisDurations.getGreaterThan(), analysisDurations.getUnit() );
        }

        return returnMe;
    }

    /**
     * Returns the latest analysis duration associated with the project or <code>null</code>.
     * 
     * @return the latest analysis duration or null
     */

    public Duration getLatestAnalysisDurationOrNull()
    {
        Duration returnMe = null;

        if ( Objects.nonNull( this.getProjectConfig().getPair().getAnalysisDurations() ) )
        {
            DurationBoundsType analysisDurations = this.getProjectConfig()
                                                       .getPair()
                                                       .getAnalysisDurations();

            returnMe = this.getDurationOrNull( analysisDurations.getLessThanOrEqualTo(), analysisDurations.getUnit() );
        }

        return returnMe;
    }

    /**
     * Returns a duration from an integer amount and a string unit, else <code>null</null>.
     * 
     * @return a duration or null
     */

    private Duration getDurationOrNull( Integer duration, DurationUnit durationUnit )
    {
        Duration returnMe = null;

        if ( Objects.nonNull( duration ) && Objects.nonNull( durationUnit ) )
        {
            ChronoUnit unit = ChronoUnit.valueOf( durationUnit.toString()
                                                              .toUpperCase() );

            returnMe = Duration.of( duration, unit );
        }

        return returnMe;
    }

    /**
     * @return The earliest possible day in a season. NULL unless specified in
     * the configuration
     */
    public MonthDay getEarliestDayInSeason()
    {
        MonthDay earliest = null;

        PairConfig.Season season = this.projectConfig.getPair().getSeason();

        if ( season != null )
        {
            earliest = MonthDay.of( season.getEarliestMonth(), season.getEarliestDay() );
        }

        return earliest;
    }

    /**
     * @return The latest possible day in a season. NULL unless specified in the
     * configuration
     */
    public MonthDay getLatestDayInSeason()
    {
        MonthDay latest = null;

        PairConfig.Season season = this.projectConfig.getPair().getSeason();

        if ( season != null )
        {
            latest = MonthDay.of( season.getLatestMonth(), season.getLatestDay() );
        }

        return latest;
    }

    public boolean usesGriddedData( DataSourceConfig dataSourceConfig )
            throws SQLException
    {
        Boolean usesGriddedData;

        String name = this.getInputName( dataSourceConfig );

        switch ( name )
        {
            case Project.LEFT_MEMBER:
                usesGriddedData = this.leftUsesGriddedData;
                break;
            case Project.RIGHT_MEMBER:
                usesGriddedData = this.rightUsesGriddedData;
                break;
            case Project.BASELINE_MEMBER:
                usesGriddedData = this.baselineUsesGriddedData;
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized enumeration value in this context, '"
                                                    + name
                                                    + "'." );
        }

        if ( usesGriddedData == null )
        {
            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.addLine( SELECT_1 );
            script.addLine( "FROM wres.ProjectSource PS" );
            script.addLine( "INNER JOIN wres.Source S" );
            script.addTab().addLine( "ON PS.source_id = S.source_id" );
            script.addLine( "WHERE PS.project_id = ?" );
            script.addArgument( this.getId() );
            script.addTab().addLine( "AND PS.member = ?" );
            script.addArgument( this.getInputName( dataSourceConfig ) );
            script.addTab().addLine( "AND S.is_point_data = FALSE" );
            script.setMaxRows( 1 );

            try ( DataProvider provider = script.getData() )
            {
                // If there is a row, then gridded data is used.
                usesGriddedData = provider.next();
            }

            switch ( name )
            {
                case Project.LEFT_MEMBER:
                    this.leftUsesGriddedData = usesGriddedData;
                    break;
                case Project.RIGHT_MEMBER:
                    this.rightUsesGriddedData = usesGriddedData;
                    break;
                case Project.BASELINE_MEMBER:
                    this.baselineUsesGriddedData = usesGriddedData;
                    break;
                default:
                    throw new IllegalArgumentException( "Unrecognized enumeration value in this context, '"
                                                        + name
                                                        + "'." );
            }
        }

        return usesGriddedData;
    }

    /**
     * Returns the name of the member that the DataSourceConfig belongs to
     *
     * <p>
     * If the "this.getInputName(this.getRight())" is called, the result
     * should be "ProjectDetails.RIGHT_MEMBER"
     *</p>
     * @param dataSourceConfig A DataSourceConfig from a project configuration
     * @return The name for how the DataSourceConfig relates to the project
     */
    private String getInputName( DataSourceConfig dataSourceConfig )
    {
        return ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(),
                                                      dataSourceConfig )
                           .value()
                           .toLowerCase();
    }

    /**
     * @return A list of all configurations stating where to store pair output
     */
    public List<DestinationConfig> getPairDestinations()
    {
        return ProjectConfigs.getDestinationsOfType( this.getProjectConfig(), DestinationType.PAIRS );
    }

    private String getProjectName()
    {
        return this.projectConfig.getName();
    }

    /**
     * Returns unique identifier for this project's dataset
     * @return The unique ID
     */
    public String getHash()
    {
        return this.hash;
    }

    /**
     * @return Whether or not baseline data is involved in the project
     */
    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    public Integer getId()
    {
        return this.projectID;
    }

    private DataScripter getInsertSelectStatement()
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
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

    public void save() throws SQLException
    {
        DataScripter saveScript = this.getInsertSelectStatement();
        this.performedInsert = saveScript.execute() > 0;

        if ( this.performedInsert )
        {
            this.projectID = saveScript.getInsertedIds()
                                       .get( 0 )
                                       .intValue();
        }
        else
        {
            Database database = this.getDatabase();
            DataScripter scriptWithId = new DataScripter( database );
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.addLine( "SELECT project_id" );
            scriptWithId.addLine( "FROM wres.Project P" );
            scriptWithId.addLine( "WHERE P.hash = ?;" );
            scriptWithId.addArgument( this.getHash() );

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.projectID = data.getInt( PROJECT_ID );
            }
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Did I create Project ID {}? {}",
                          this.getId(),
                          this.performedInsert );
        }
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

    public boolean performedInsert()
    {
        return this.performedInsert;
    }

    /**
     * Return <code>true</code> if the project uses probability thresholds, otherwise <code>false</code>.
     * 
     * @return Whether or not the project uses probability thresholds
     */
    public boolean hasProbabilityThresholds()
    {
        return ConfigHelper.hasProbabilityThresholds( this.getProjectConfig() );
    }

    public int compareTo( Project other )
    {
        return this.getHash().compareTo( other.getHash() );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof Project && this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getHash() );
    }
}

