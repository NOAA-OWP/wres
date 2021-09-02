package wres.io.project;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
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
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.space.FeatureKey;
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
    private static final String DATA_SOURCES_TO_DISAMBIGUATE = " data sources to disambiguate.";

    private static final String NAME_FOR_THE = "name for the ";

    private static final String POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE =
            "possibilities. Please declare an explicit variable ";

    private static final String NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY =
            "name from the data, failed to identify any ";

    private static final String VARIABLE = " variable ";

    private static final String WHILE_ATTEMPTING_TO_DETECT_THE = "While attempting to detect the ";

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

    private long projectId;

    /**
     * The measurement unit, which is the declared unit, if available, else the most commonly occurring unit among the 
     * project sources, with a preference for the mostly commonly occurring right-sided source unit. See 
     * {@link ProjectScriptGenerator#createUnitScript(Database, int)}.
     */

    private String measurementUnit = null;

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

    /** The left-ish variable to evaluate. */
    private String leftVariable;

    /** The right-ish variable to evaluate. */
    private String rightVariable;

    /** The baseline-ish variable to evaluate. */
    private String baselineVariable;

    public Project( SystemSettings systemSettings,
                    Database database,
                    Features featuresCache,
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
        this.featuresCache = featuresCache;
        this.featuresCache.setOnlyReadFromDatabase();
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

    public Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    /**
     * @return the measurement unit, which is either the declared unit of the analyzed unit, but possibly null
     * @throws SQLException if the measurement unit is not declared and could not be determined from the project sources
     * @throws IllegalArgumentException if the project identity is required and undefined
     */

    public String getMeasurementUnit() throws SQLException
    {
        // Declared unit available?
        String declaredUnit = this.getProjectConfig()
                                  .getPair()
                                  .getUnit();
        if ( Objects.isNull( this.measurementUnit ) && Objects.nonNull( declaredUnit ) && !declaredUnit.isBlank() )
        {
            this.measurementUnit = declaredUnit;

            LOGGER.debug( "Determined the measurement unit from the project declaration as {}.",
                          this.getMeasurementUnit() );
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
                                      + "measurement unit is {} and corresponds to the most commonly occurring unit among "
                                      + "time-series from {} sources. The script used to discover the measurement unit "
                                      + "was: {}{}",
                                      this.measurementUnit,
                                      member,
                                      System.lineSeparator(),
                                      scripter );
                    }
                }
            }
        }

        return this.measurementUnit;
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
        Database db = this.getDatabase();

        // Check for features that potentially have intersecting values.
        // The query in getIntersectingFeatures checks that there is some
        // data for each feature on each side, but does not guarantee pairs.
        synchronized ( this.featureLock )
        {
            LOGGER.debug( "Features so far: {}", this.features );
            this.features = this.getIntersectingFeatures( db );
            LOGGER.debug( "Features after getting intersecting features: {}",
                          this.features );
        }

        if ( this.features.isEmpty() )
        {
            throw new NoDataException( "No features had data on both the left and the right for the variables "
                                       + "specified." );
        }

        // Validate any ensemble conditions
        this.validateEnsembleConditions();

        // Determine and set the variables to evaluate
        this.setVariablesToEvaluate();
    }

    /**
     * Checks that the union of ensemble conditions will select some data, otherwise throws an exception.
     * 
     * @throws NoDataException if the conditions select no data
     * @throws SQLException if one or more ensemble conditions could not be evaluated
     */

    private void validateEnsembleConditions() throws SQLException
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
     * Sets the variables to evaluate. Begins by looking at the declaration. If it cannot find a declared variable for 
     * any particular left/right/baseline context, it looks at the data instead. If there is more than one possible 
     * name and it does not exactly match the name identified for the other side of the pairing, then an exception is 
     * thrown because declaration is require to disambiguate. Otherwise, it chooses the single variable name and warns 
     * about the assumption made when using the data to disambiguate.
     * 
     * @throws SQLException if the variable information could not be determined from the data
     */

    private void setVariablesToEvaluate() throws SQLException
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

        this.setVariableNames( Collections.unmodifiableSet( leftNames ),
                               Collections.unmodifiableSet( rightNames ),
                               Collections.unmodifiableSet( baselineNames ) );
    }

    /**
     * Attempts to set the variable names from a set of left, right and baseline names.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     */

    private void setVariableNames( Set<String> left, Set<String> right, Set<String> baseline )
    {
        // Could not determine variable name
        if ( left.isEmpty() )
        {
            throw new ProjectConfigException( this.getLeft(),
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
            throw new ProjectConfigException( this.getRight(),
                                              WHILE_ATTEMPTING_TO_DETECT_THE + LeftOrRightOrBaseline.RIGHT
                                                              + VARIABLE
                                                              + NAME_FROM_THE_DATA_FAILED_TO_IDENTIFY_ANY
                                                              + POSSIBILITIES_PLEASE_DECLARE_AN_EXPLICIT_VARIABLE
                                                              + NAME_FOR_THE
                                                              + LeftOrRightOrBaseline.RIGHT
                                                              + DATA_SOURCES_TO_DISAMBIGUATE );
        }

        if ( this.hasBaseline() && baseline.isEmpty() )
        {

            throw new ProjectConfigException( this.getBaseline(),
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
            this.leftVariable = left.iterator()
                                    .next();
            this.rightVariable = right.iterator()
                                      .next();

            if ( this.hasBaseline() )
            {
                this.baselineVariable = baseline.iterator()
                                                .next();
            }
            
            LOGGER.debug( "Discovered one variable name for all data sources. The variable name is {}.", 
                          this.leftVariable );
        }
        // More than one for some, need to intersect
        else
        {
             this.setVariableNamesFromIntersection( left, right, baseline );
        }

        this.validateVariableNames();
    }

    /**
     * Attempts to find a unique name by intersecting the left, right and baseline names.
     * @param left the possible left variable names
     * @param right the possible right variable names
     * @param baseline the possible baseline variable names
     * @throws ProjectConfigException if a unique name could not be discovered
     */

    private void setVariableNamesFromIntersection( Set<String> left, Set<String> right, Set<String> baseline )
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

        if ( this.hasBaseline() )
        {
            intersection.retainAll( baseline );
        }

        if ( intersection.size() == 1 )
        {
            this.leftVariable = intersection.iterator()
                                            .next();
            this.rightVariable = this.leftVariable;

            if ( this.hasBaseline() )
            {
                this.baselineVariable = this.leftVariable;
            }

            LOGGER.debug( "After intersecting the variable names, discovered one variable name to evaluate, {}.",
                          this.leftVariable );
        }
        else
        {
            throw new ProjectConfigException( this.getProjectConfig()
                                                  .getInputs(),
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
     * Validates the variable names and emits a warning if assumptions have been made by the software.
     */

    private void validateVariableNames()
    {
        // Warn if the names were not declared and are different
        if ( ( Objects.isNull( this.getDeclaredLeftVariableName() )
               || Objects.isNull( this.getDeclaredRightVariableName() ) )
             && !this.getLeftVariableName().equals( this.getRightVariableName() )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The LEFT and RIGHT variable names were auto-detected, but the detected variable names do not "
                         + "match. The LEFT name is {} and the RIGHT name is {}. Proceeding to pair and evaluate these "
                         + "variables. If this is unexpected behavior, please add explicit variable declaration for "
                         + "both the LEFT and RIGHT data and try again.",
                         this.getLeftVariableName(),
                         this.getRightVariableName() );
        }

        if ( this.hasBaseline() && ( Objects.isNull( this.getDeclaredLeftVariableName() )
                                     || Objects.isNull( this.getDeclaredBaselineVariableName() ) )
             && !this.getLeftVariableName().equals( this.getBaselineVariableName() )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "The LEFT and BASELINE variable names were auto-detected, but the detected variable names do "
                         + "not match. The LEFT name is {} and the BASELINE name is {}. Proceeding to pair and "
                         + "evaluate these variables. If this is unexpected behavior, please add explicit variable "
                         + "declaration for both the LEFT and BASELINE data and try again.",
                         this.getLeftVariableName(),
                         this.getRightVariableName() );
        }
    }

    /**
     * Determines the possible variable names by inspecting the data.
     * 
     * @param lrb the context
     * @return the possible variable names
     * @throws SQLException if the variable information could not be determined from the data
     * @throws ProjectConfigException if declaration is required to disambiguate the variable name
     */

    private Set<String> getVariableNameByInspectingData( LeftOrRightOrBaseline lrb ) throws SQLException
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

        return Collections.unmodifiableSet( names );
    }

    /**
     * Checks for any invalid ensemble conditions and returns a string representation of the invalid conditions.
     * 
     * @param lrb the orientation of the source
     * @param config the source configuration whose ensemble conditions should be validated
     * @return a string representation of the invalid conditions 
     * @throws SQLException if one or more ensemble conditions could not be evaluated
     */

    private List<String> getInvalidEnsembleConditions( LeftOrRightOrBaseline lrb,
                                                       DataSourceConfig config )
            throws SQLException
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
                        boolean dataExists = dataProvider.getBoolean( "exists" );

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
     * @return The Set of FeatureDetails with some data on each side
     */

    private Set<FeatureTuple> getIntersectingFeatures( Database database )
            throws SQLException
    {
        Set<FeatureTuple> intersectingFeatures;
        Features fCache = this.getFeaturesCache();

        // Gridded features? #74266
        // Yes
        if ( this.usesGriddedData( this.getRight() ) )
        {
            LOGGER.debug( "Getting details of intersecting features for gridded data." );
            intersectingFeatures = fCache.getGriddedFeatures();
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

            try ( Connection connection = database.getConnection();
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
        Database db = this.getDatabase();

        synchronized ( this.featureLock )
        {
            if ( this.features == null )
            {
                LOGGER.debug( "getFeatures(): no features found, populating." );
                this.features = this.getIntersectingFeatures( db );
            }
        }

        return Collections.unmodifiableSet( this.features );
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
     * @see #getDeclaredLeftVariableName()
     * @return The name of the left variable or null if determined from the data and the data has yet to be inspected
     */
    public String getLeftVariableName()
    {
        if ( Objects.isNull( this.leftVariable ) )
        {
            return this.getDeclaredLeftVariableName();
        }

        return this.leftVariable;
    }

    /**
     * @see #getLeftVariableName()
     * @return The declared left variable name or null if undeclared
     */
    public String getDeclaredLeftVariableName()
    {
        if ( Objects.nonNull( this.getLeft().getVariable() ) )
        {
            return this.getLeft().getVariable().getValue();
        }

        return null;
    }

    /**
     * @return The name of the right variable or null if determined from the data and the data has yet to be inspected
     */
    public String getRightVariableName()
    {
        if ( Objects.isNull( this.rightVariable ) )
        {
            return this.getDeclaredRightVariableName();
        }

        return this.rightVariable;
    }

    /**
     * @see #getRightVariableName()
     * @return The declared right variable name or null if undeclared
     */
    public String getDeclaredRightVariableName()
    {
        if ( Objects.nonNull( this.getRight().getVariable() ) )
        {
            return this.getRight().getVariable().getValue();
        }

        return null;
    }

    /**
     * @return The name of the baseline variable or null if determined from the data and the data has yet to be 
     *            inspected
     */
    public String getBaselineVariableName()
    {
        if ( Objects.isNull( this.baselineVariable ) )
        {
            return this.getDeclaredBaselineVariableName();
        }

        return this.baselineVariable;
    }

    /**
     * @see #getBaselineVariableName()
     * @return The declared baseline variable name or null if undeclared
     */
    public String getDeclaredBaselineVariableName()
    {
        if ( this.hasBaseline() && Objects.nonNull( this.getBaseline().getVariable() ) )
        {
            return this.getBaseline().getVariable().getValue();
        }

        return null;
    }

    /**
     * @param lrb The side of data for which the variable is required
     * @return The name of the variable for the specified side of data
     * @throws NullPointerException if the lrb is null
     */

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
                throw new UnsupportedOperationException( "Unexpected context '" + lrb
                                                         + "': expected LEFT or "
                                                         + "RIGHT or BASELINE." );
        }
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
            Database db = this.getDatabase();
            DataScripter script = new DataScripter( db );
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

    public long getId()
    {
        return this.projectId;
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

    public void save() throws SQLException
    {
        DataScripter saveScript = this.getInsertSelectStatement();
        this.performedInsert = saveScript.execute() > 0;

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

