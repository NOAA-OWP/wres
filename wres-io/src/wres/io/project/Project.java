package wres.io.project;

import java.io.IOException;
import java.sql.SQLException;
import java.time.MonthDay;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdType;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.NoDataException;
import wres.util.CalculationException;

/**
 * Encapsulates operations involved with interpreting unpredictable data within the
 * project configuration, storing the results of expensive calculations, and forming database
 * statements based solely on the configured specifications
 */
public class Project
{

    private static final String WHERE_PS_PROJECT_ID = "WHERE PS.project_id = ";

    private static final String SELECT_1 = "SELECT 1";

    private static final String INNER_JOIN_WRES_VARIABLE_FEATURE_VF = "INNER JOIN wres.VariableFeature VF";

    private static final String INNER_JOIN_WRES_PROJECT_SOURCE_PS = "INNER JOIN wres.ProjectSource PS";

    private static final String AND_VF_FEATURE_ID_F_FEATURE_ID = "AND VF.feature_id = F.feature_id";

    private static final String PROJECT_ID = "project_id";

    private static final Logger LOGGER = LoggerFactory.getLogger( Project.class );

    /**
     * The member identifier for left handed data in the database
     */
    public static final String LEFT_MEMBER = "'left'";

    /**
     * The member identifier for right handed data in the database
     */
    public static final String RIGHT_MEMBER = "'right'";

    /**
     * The member identifier for baseline data in the database
     */
    public static final String BASELINE_MEMBER = "'baseline'";

    /**
     * Protects access and generation of the feature collection
     */
    private final Object featureLock = new Object();

    private Integer projectID = null;
    private final ProjectConfig projectConfig;

    /**
     * The set of all features pertaining to the project
     */
    private Set<FeatureDetails> features;

    /**
     * The ID for the variable on the left side of the input
     */
    private Integer leftVariableID = null;

    /**
     * The ID for the variable on the right side of the input
     */
    private Integer rightVariableID = null;

    /**
     * The ID for the variable for the baseline
     */
    private Integer baselineVariableID = null;

    /**
     * Indicates whether or not this project was inserted on upon this
     * execution of the project
     */
    private boolean performedInsert;

    /**
     * The overall hash for the data sources used in the project
     */
    private final int inputCode;

    private Boolean leftUsesGriddedData = null;
    private Boolean rightUsesGriddedData = null;
    private Boolean baselineUsesGriddedData = null;

    public Project( ProjectConfig projectConfig,
                    Integer inputCode )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( inputCode );
        this.projectConfig = projectConfig;
        this.inputCode = inputCode;
    }

    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * Performs operations that are needed for the project to run between ingest and evaluation.
     * 
     * @throws SQLException if retrieval of data from the database fails
     * @throws IOException if loading fails
     * @throws CalculationException if required calculations could not be completed
     * @throws NoDataException if zero features have intersecting data
     */
    public void prepareForExecution() throws SQLException, IOException
    {
        LOGGER.trace( "prepareForExecution() entered" );

        // Gridded data will not be present in the database.
        if ( !this.usesGriddedData( this.getRight() ) )
        {
            // Check for features that potentially have intersecting values.
            // The query in getIntersectingFeatures checks that there is some
            // data for each feature on each side, but does not guarantee pairs.
            synchronized ( this.featureLock )
            {
                LOGGER.debug( "Features so far: {}", this.features );
                this.features = this.getIntersectingFeatures();
                LOGGER.debug( "Features after getting intersecting features: {}",
                              this.features );
            }

            if ( this.features.isEmpty() )
            {
                throw new NoDataException( "No features had data on both the left and the right for the variables "
                                           + "specified." );
            }
        }
    }

    /**
     * Get a set of features for this project with intersecting data.
     * Does not check if the data is pairable, simply checks that there is data
     * on each of the left and right for this variable at a given feature.
     * @return The Set of FeatureDetails with some data on each side
     */

    private Set<FeatureDetails> getIntersectingFeatures() throws SQLException
    {
        Set<FeatureDetails> intersectingFeatures = new HashSet<>();
        DataScripter script = ProjectScriptGenerator.createIntersectingFeaturesScript( this );

        LOGGER.debug( "getIntersectingFeatures will run: {}", script );

        try ( DataProvider dataProvider = script.buffer() )
        {
            while ( dataProvider.next() )
            {
                int featureId = dataProvider.getInt( "feature_id" );
                FeatureDetails.FeatureKey key =
                        Features.getFeatureKey( featureId );
                FeatureDetails featureDetail = new FeatureDetails( key );
                intersectingFeatures.add( featureDetail );
            }
        }

        LOGGER.debug( "getIntersectingFeatures finished run: {}", script );

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
    public Set<FeatureDetails> getFeatures() throws SQLException
    {
        synchronized ( this.featureLock )
        {
            if ( this.features == null )
            {
                LOGGER.debug( "getFeatures(): no features found, populating." );
                this.populateFeatures();
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

    public Set<Integer> getEnsembleMembersToFilter( LeftOrRightOrBaseline dataType, boolean include )
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

        Set<Integer> returnMe = new TreeSet<>();

        if ( Objects.nonNull( config ) && !config.getEnsemble().isEmpty() )
        {
            List<EnsembleCondition> conditions = config.getEnsemble();

            for ( EnsembleCondition condition : conditions )
            {
                if ( condition.isExclude() != include )
                {
                    returnMe.addAll( Ensembles.getEnsembleIDs( condition ) );
                }
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Loads metadata about all features that the project needs to use
     * @throws SQLException Thrown if metadata about the features could not be loaded from the database
     */
    private void populateFeatures() throws SQLException
    {
        LOGGER.trace( "populateFeatures entered for {}", this );
        synchronized ( this.featureLock )
        {
            if ( this.usesGriddedData( this.getRight() ) )
            {
                this.features = Features.getGriddedDetails( this );
            }
            // If there is no indication whatsoever of what to look for, we
            // want to query the database specifically for all locations
            // that have data ingested pertaining to the project.
            //
            // The similar function in wres.io.data.caching.Features cannot be
            // used because it doesn't restrict data based on what lies in the
            // database.
            else if ( this.getProjectConfig().getPair().getFeature() == null ||
                      this.projectConfig.getPair().getFeature().isEmpty() )
            {
                this.features = new HashSet<>();

                DataScripter script = new DataScripter();

                // TODO: it is most likely safe to assume the left will be observations

                // Select all features where...
                script.addLine( "SELECT *" );
                script.addLine( "FROM wres.Feature F" );
                script.addLine( "WHERE EXISTS (" );

                if ( ConfigHelper.isForecast( this.getLeft() ) )
                {
                    // There is at least one value pertaining to a forecasted value
                    // indicated by the left hand configuration
                    script.addTab().addLine( SELECT_1 );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab().addLine( INNER_JOIN_WRES_VARIABLE_FEATURE_VF );
                    script.addTab( 2 ).addLine( "ON VF.variablefeature_id = TS.variablefeature_id" );
                    script.addTab()
                          .addLine( INNER_JOIN_WRES_PROJECT_SOURCE_PS );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = TS.source_id" );
                    script.addTab()
                          .addLine( WHERE_PS_PROJECT_ID, this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 ).addLine( "AND PS.source_id = TS.source_id" );
                    script.addTab( 2 )
                          .addLine( AND_VF_FEATURE_ID_F_FEATURE_ID );
                }
                else
                {
                    // There is at least one observed value pertaining to the
                    // configuration for the left sided data
                    script.addTab().addLine( SELECT_1 );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( INNER_JOIN_WRES_VARIABLE_FEATURE_VF );
                    script.addTab( 2 )
                          .addLine(
                                    "ON VF.variablefeature_id = O.variablefeature_id" );
                    script.addTab()
                          .addLine( INNER_JOIN_WRES_PROJECT_SOURCE_PS );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( WHERE_PS_PROJECT_ID, this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 )
                          .addLine( AND_VF_FEATURE_ID_F_FEATURE_ID );
                }

                script.addTab().addLine( ")" );

                // And...
                script.addTab().addLine( "AND EXISTS (" );

                if ( ConfigHelper.isForecast( this.getRight() ) )
                {
                    // There is at least one value pertaining to a forecasted value
                    // indicated by the right hand configuration
                    script.addTab().addLine( SELECT_1 );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab()
                          .addLine( INNER_JOIN_WRES_VARIABLE_FEATURE_VF );
                    script.addTab( 2 )
                          .addLine(
                                    "ON VF.variablefeature_id = TS.variablefeature_id" );
                    script.addTab()
                          .addLine( INNER_JOIN_WRES_PROJECT_SOURCE_PS );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = TS.source_id" );
                    script.addTab()
                          .addLine( WHERE_PS_PROJECT_ID, this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 ).addLine( "AND PS.source_id = TS.source_id" );
                    script.addTab( 2 )
                          .addLine( AND_VF_FEATURE_ID_F_FEATURE_ID );
                }
                else
                {
                    // There is at least one observed value pertaining to the
                    // configuration for the right sided data
                    script.addTab().addLine( SELECT_1 );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( INNER_JOIN_WRES_VARIABLE_FEATURE_VF );
                    script.addTab( 2 )
                          .addLine(
                                    "ON VF.variablefeature_id = O.variablefeature_id" );
                    script.addTab()
                          .addLine( INNER_JOIN_WRES_PROJECT_SOURCE_PS );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( WHERE_PS_PROJECT_ID, this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 )
                          .addLine( AND_VF_FEATURE_ID_F_FEATURE_ID );
                }

                script.addLine( ");" );

                try
                {
                    script.consume( feature -> this.features.add( new FeatureDetails(
                                                                                      feature ) ) );
                }
                catch ( SQLException e )
                {
                    throw new SQLException(
                                            "The features for this project could "
                                            + "not be retrieved from the database.",
                                            e );
                }
            }
            else
            {
                // Get all features that correspond to the feature configuration
                this.features =
                        Features.getAllDetails( this.getProjectConfig() );
            }
        }
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
     * Determines the ID of the left variable
     * @return The ID of the left variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getLeftVariableID() throws SQLException
    {
        if ( this.leftVariableID == null )
        {
            this.leftVariableID = Variables.getVariableID( this.getLeftVariableName() );
        }

        return this.leftVariableID;
    }

    /**
     * @return The name of the left variable
     */
    private String getLeftVariableName()
    {
        return this.getLeft().getVariable().getValue();
    }

    /**
     * Determines the ID of the right variable
     * @return The ID of the right variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getRightVariableID() throws SQLException
    {
        if ( this.rightVariableID == null )
        {
            this.rightVariableID = Variables.getVariableID( this.getRightVariableName() );
        }

        return this.rightVariableID;
    }

    /**
     * @return The name of the right variable
     */
    public String getRightVariableName()
    {
        return this.getRight().getVariable().getValue();
    }

    /**
     * Determines the ID of the baseline variable
     * @return The ID of the baseline variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getBaselineVariableID() throws SQLException
    {
        if ( this.hasBaseline() && this.baselineVariableID == null )
        {
            this.baselineVariableID = Variables.getVariableID( this.getBaselineVariableName() );
        }

        return this.baselineVariableID;
    }

    /**
     * @return The name of the baseline variable
     */
    private String getBaselineVariableName()
    {
        String name = null;
        if ( this.hasBaseline() )
        {
            name = this.getBaseline().getVariable().getValue();
        }
        return name;
    }

    /**
     * Determines the <code>variablefeature_id</code> of the left dataset for a given feature.
     * @param feature the feature
     * @return the left variablefeature_id
     * @throws SQLException if the identifier cannot be determined
     * @throws NullPointerException if the feature is null
     */
    public Integer getLeftVariableFeatureId( Feature feature ) throws SQLException
    {
        Objects.requireNonNull( feature );

        Integer variableId = this.getLeftVariableID();
        return Features.getVariableFeatureID( feature, variableId );
    }

    /**
     * Determines the <code>variablefeature_id</code> of the right dataset for a given feature.
     * @param feature the feature
     * @return the right variablefeature_id
     * @throws SQLException if the identifier cannot be determined
     * @throws NullPointerException if the feature is null
     */
    public Integer getRightVariableFeatureId( Feature feature ) throws SQLException
    {
        Objects.requireNonNull( feature );

        Integer variableId = this.getRightVariableID();
        return Features.getVariableFeatureID( feature, variableId );
    }

    /**
     * Determines the <code>variablefeature_id</code> of the baseline dataset for a given feature.
     * @param feature the feature
     * @return the baseline variablefeature_id
     * @throws SQLException if the identifier cannot be determined
     * @throws NullPointerException if the feature is null
     */
    public Integer getBaselineVariableFeatureId( Feature feature ) throws SQLException
    {
        Objects.requireNonNull( feature );

        Integer returnMe = null;

        if ( this.hasBaseline() )
        {
            Integer variableId = this.getBaselineVariableID();
            returnMe = Features.getVariableFeatureID( feature, variableId );
        }

        return returnMe;
    }


    /**
     * Get the variableFeatureId, also facilitates testing.
     * @param variableName The name of the variable.
     * @param feature The feature.
     * @return The integer id from the wres database, null if not found?
     */

    public Integer getVariableFeatureId( String variableName, Feature feature )
            throws SQLException
    {
        int variableId = Variables.getVariableID( variableName );
        return Features.getVariableFeatureID( feature,
                                              variableId );
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
            DataScripter script = new DataScripter();
            script.addLine( "SELECT EXISTS (" );
            script.addTab().addLine( SELECT_1 );
            script.addTab().addLine( "FROM wres.ProjectSource PS" );
            script.addTab().addLine( "INNER JOIN wres.Source S" );
            script.addTab( 2 ).addLine( "ON PS.source_id = S.source_id" );
            script.addTab().addLine( WHERE_PS_PROJECT_ID, this.getId() );
            script.addTab( 2 ).addLine( "AND PS.member = ", this.getInputName( dataSourceConfig ) );
            script.addTab( 2 ).addLine( "AND S.is_point_data = FALSE" );
            script.addLine( ") AS uses_gridded_data;" );

            usesGriddedData = script.retrieve( "uses_gridded_data" );

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
        return "'"
               + ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(),
                                                        dataSourceConfig )
                             .value()
                             .toLowerCase()
               + "'";
    }    

    /**
     * @return A list of all configurations stating where to store pair output
     */
    public List<DestinationConfig> getPairDestinations()
    {
        return ConfigHelper.getDestinationsOfType( this.getProjectConfig(), DestinationType.PAIRS );
    }

    private String getProjectName()
    {
        return this.projectConfig.getName();
    }

    /**
     * Returns unique identifier for this project's config+data
     * @return The unique ID
     */
    public Integer getInputCode()
    {
        return this.inputCode;
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
        DataScripter script = new DataScripter();
        script.setUseTransaction( true );

        script.retryOnSerializationFailure();
        script.retryOnUniqueViolation();

        script.setHighPriority( true );

        script.addLine( "INSERT INTO wres.Project (project_name, input_code)" );
        script.addTab().addLine( "SELECT ?, ?" );

        script.addArgument( this.getProjectName() );
        script.addArgument( this.getInputCode() );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( SELECT_1 );
        script.addTab( 2 ).addLine( "FROM wres.Project P" );
        script.addTab( 2 ).addLine( "WHERE P.input_code = ?" );

        script.addArgument( this.getInputCode() );

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
            DataScripter scriptWithId = new DataScripter();
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.addLine( "SELECT project_id" );
            scriptWithId.addLine( "FROM wres.Project P" );
            scriptWithId.addLine( "WHERE P.input_code = ?;" );
            scriptWithId.addArgument( this.getInputCode() );

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
               + this.getInputCode()
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
    public boolean usesProbabilityThresholds()
    {
        // Iterate metrics configuration
        for ( MetricsConfig next : this.projectConfig.getMetrics() )
        {
            // Check thresholds           
            if ( next.getThresholds()
                     .stream()
                     .anyMatch( a -> Objects.isNull( a.getType() )
                                     || a.getType() == ThresholdType.PROBABILITY ) )
            {
                return true;
            }
        }
        return false;
    }

    public int compareTo( Project other )
    {
        return this.getInputCode().compareTo( other.getInputCode() );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof Project && this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return this.getInputCode();
    }

}

