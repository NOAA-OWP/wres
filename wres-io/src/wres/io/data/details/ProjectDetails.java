package wres.io.data.details;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdType;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeScaleFunction;
import wres.io.concurrency.DataSetRetriever;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.utilities.DataSet;
import wres.io.utilities.Database;
import wres.io.utilities.LRUMap;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.io.utilities.ScriptGenerator;
import wres.util.Collections;
import wres.util.FormattedStopwatch;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.TimeHelper;

/**
 * Wrapper object linking a project configuration and the data needed to form
 * database statements
 */
public class ProjectDetails extends CachedDetail<ProjectDetails, Integer>
{
    public enum PairingMode
    {
        ROLLING,
        @Deprecated
        BACK_TO_BACK,
        TIME_SERIES
    }

    /**
     * Ensures that multiple copies of a project aren't saved at the same time
     */
    private static final Object PROJECT_SAVE_LOCK = new Object();

    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectDetails.class );

    /**
     * Lock used to protect access to the logic used to determine the left hand scale
     */
    private static final Object LEFT_LEAD_LOCK = new Object();

    /**
     * Lock used to protect access to the logic used to determine the rightt hand scale
     */
    private static final Object RIGHT_LEAD_LOCK = new Object();

    /**
     * Lock used to protect access to the logic used to determine the baseline scale
     */
    private static final Object BASELINE_LEAD_LOCK = new Object();

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

    private Integer projectID = null;
    private final ProjectConfig projectConfig;

    /**
     *  Stores the last possible lead time for each feature
     */
    private final Map<Feature, Integer> lastLeads = new LRUMap<>( 20 );

    /**
     * Stores the lead hour offset for each person
     *
     * <p>
     * If there is a 6 hour offset at feature x, it means that the lead times
     * used to match with observation data need to be increased by 6 hours to
     * provide a valid match
     * </p>
     */
    private final Map<Feature, Integer> leadOffsets = new ConcurrentSkipListMap<>( ConfigHelper.getFeatureComparator() );

    /**
     * Stores the earliest possible observation date for each feature
     */
    private final Map<Feature, String> initialObservationDates = new LRUMap<>( 20 );

    private final Map<Feature, String> initialForecastDates = new LRUMap<>(20);

    private final Map<Feature, Integer> forecastLag = new LRUMap<>(20);

    /**
     * Stores the number of basis times pools for each feature
     *
     * <p>
     * Normally, there will be one pool per feature, but configuring an issue
     * times pooling window will possibly create multiple windows. If a feature
     * is deemed to have x pools, then each set of lead times will be evaluated
     * on x different sets of issue times
     * </p>
     */
    private final Map<Feature, Integer> poolCounts = new LRUMap<>( 20 );

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

    private static final Object SERIES_AMOUNT_LOCK = new Object();
    /**
     * Details the number of time series to pull at once when gathering by time series
     */
    private Integer numberOfSeriesToRetrieve = null;

    /**
     * Indicates whether or not this project was inserted on upon this
     * execution of the project
     */
    private boolean performedInsert;

    /**
     * The overall hash for the total specifications for the project
     */
    private final int inputCode;

    /**
     * The number of application standard wres.config.generated.DurationUnit
     * for the scale of the left side data
     *
     * <p>
     * If the standard DurationUnit is 'HOURS' and the data is generated daily,
     * the scale will be 24
     * </p>
     */
    private long leftScale = -1;

    /**
     * The number of application standard wres.config.generated.DurationUnit
     * for the scale of the right side data
     *
     * <p>
     * If the standard DurationUnit is 'HOURS' and the data is generated daily,
     * the scale will be 24
     * </p>
     */
    private long rightScale = -1;

    /**
     * The number of application standard wres.config.generated.DurationUnit
     * for the scale of the baseline data
     *
     * <p>
     * If the standard DurationUnit is 'HOURS' and the data is generated daily,
     * the scale will be 24
     * </p>
     */
    private long baselineScale = -1;

    /**
     * The desired scale for the project
     *<p>
     * If the configuration doesn't specify the desired scale, the system
     * will determine that data itself
     * </p>
     */
    private TimeScaleConfig desiredTimeScale;

    /**
     * Indicates whether or not lead times should be calculated rather than
     * using discrete numbers
     */
    private Boolean calculateLeads = null;

    /**
     * Stores sets of lead times per feature that need to be evaluated
     * individually if it is deemed that lead times shouldn't be calculated
     *<p>
     * In primitive irregular time series, 'lead = x' statements will be created
     * by pulling values from a feature's list of lead times
     * </p>
     */
    private Map<Feature, Integer[]> discreteLeads;

    /**
     * Guards access to the pool counts
     */
    private static final Object POOL_LOCK = new Object();

    /**
     * Creates a hash for the indicated project configuration based on its
     * specifications and the data it has ingested
     * @param projectConfig The configuration for the project
     * @param leftHashesIngested A collection of the hashes for the left sided
     *                           source data
     * @param rightHashesIngested A collection of the hashes for the right sided
     *                            source data
     * @param baselineHashesIngested A collection of hashes representing the baseline
     *                               source data
     * @return A unique hash code for the project's circumstances
     */
    public static Integer hash( final ProjectConfig projectConfig,
                                final List<String> leftHashesIngested,
                                final List<String> rightHashesIngested,
                                final List<String> baselineHashesIngested )
    {
        StringBuilder hashBuilder = new StringBuilder(  );

        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        hashBuilder.append(left.getType().value());

        for ( EnsembleCondition ensembleCondition : left.getEnsemble())
        {
            hashBuilder.append(ensembleCondition.getName());
            hashBuilder.append(ensembleCondition.getMemberId());
            hashBuilder.append(ensembleCondition.getQualifier());
        }

        // Sort for deterministic hash result for same list of ingested
        List<String> sortedLeftHashes =
                Collections.copyAndSort( leftHashesIngested );

        for ( String leftHash : sortedLeftHashes )
        {
            hashBuilder.append( leftHash );
        }

        hashBuilder.append(left.getVariable().getValue());
        hashBuilder.append(left.getVariable().getUnit());

        hashBuilder.append(right.getType().value());

        for ( EnsembleCondition ensembleCondition : right.getEnsemble())
        {
            hashBuilder.append(ensembleCondition.getName());
            hashBuilder.append(ensembleCondition.getMemberId());
            hashBuilder.append(ensembleCondition.getQualifier());
        }

        // Sort for deterministic hash result for same list of ingested
        List<String> sortedRightHashes =
                Collections.copyAndSort( rightHashesIngested );

        for ( String rightHash : sortedRightHashes )
        {
            hashBuilder.append( rightHash );
        }

        hashBuilder.append(right.getVariable().getValue());
        hashBuilder.append(right.getVariable().getUnit());

        if (baseline != null)
        {

            hashBuilder.append(baseline.getType().value());

            for ( EnsembleCondition ensembleCondition : baseline.getEnsemble())
            {
                hashBuilder.append(ensembleCondition.getName());
                hashBuilder.append(ensembleCondition.getMemberId());
                hashBuilder.append(ensembleCondition.getQualifier());
            }


            // Sort for deterministic hash result for same list of ingested
            List<String> sortedBaselineHashes =
                    Collections.copyAndSort( baselineHashesIngested );

            for ( String baselineHash : sortedBaselineHashes )
            {
                hashBuilder.append( baselineHash );
            }

            hashBuilder.append(baseline.getVariable().getValue());
            hashBuilder.append(baseline.getVariable().getUnit());
        }

        for ( Feature feature : projectConfig.getPair()
                                             .getFeature() )
        {
            hashBuilder.append( ConfigHelper.getFeatureDescription( feature ) );
        }

        return hashBuilder.toString().hashCode();
    }

    public ProjectDetails( ProjectConfig projectConfig,
                           Integer inputCode )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( inputCode );
        this.projectConfig = projectConfig;
        this.inputCode = inputCode;
    }

    @Override
    public Integer getKey()
    {
        return this.getInputCode();
    }

    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
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
    public String getInputName(DataSourceConfig dataSourceConfig)
    {
        String name = null;

        if (dataSourceConfig.equals( this.getLeft() ))
        {
            name = ProjectDetails.LEFT_MEMBER;
        }
        else if (dataSourceConfig.equals( this.getRight() ))
        {
            name = ProjectDetails.RIGHT_MEMBER;
        }
        else if (dataSourceConfig.equals( this.getBaseline() ))
        {
            name = ProjectDetails.BASELINE_MEMBER;
        }

        return name;
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
        if (this.features == null)
        {
            // If there is no indication whatsoever of what to look for, we
            // want to query the database specifically for all locations
            // that have data ingested pertaining to the project.
            //
            // The similar function in wres.io.data.caching.Features cannot be
            // used because it doesn't restrict data based on what lies in the
            // database.
            if (this.getProjectConfig().getPair().getFeature() == null ||
                this.projectConfig.getPair().getFeature().isEmpty())
            {
                this.features = new HashSet<>(  );

                ScriptBuilder script = new ScriptBuilder();

                // Select all features where...
                script.addLine( "SELECT *" );
                script.addLine( "FROM wres.Feature F" );
                script.addLine( "WHERE EXISTS (" );

                if ( ConfigHelper.isForecast( this.getLeft() ) )
                {
                    // There is at least one value pertaining to a forecasted value
                    // indicated by the left hand configuration
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = TS.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ForecastSource FS" );
                    script.addTab( 2 )
                          .addLine( "ON FS.forecast_id = TS.timeseries_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = FS.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }
                else
                {
                    // There is at least one observed value pertaining to the
                    // configuration for the left sided data
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = O.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }

                script.addTab().addLine( ")" );

                // And...
                script.addTab().addLine( "AND EXISTS (" );

                if ( ConfigHelper.isForecast( this.getRight() ) )
                {
                    // There is at least one value pertaining to a forecasted value
                    // indicated by the right hand configuration
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = TS.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ForecastSource FS" );
                    script.addTab( 2 )
                          .addLine( "ON FS.forecast_id = TS.timeseries_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = FS.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }
                else
                {
                    // There is at least one observed value pertaining to the
                    // configuration for the right sided data
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariablePosition VP" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VP.variableposition_id = O.variableposition_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 )
                          .addLine( "AND VP.x_position = F.feature_id" );
                }

                script.addLine( ");" );

                Connection connection = null;
                ResultSet resultSet = null;

                try
                {
                    connection = Database.getConnection();
                    resultSet = Database.getResults( connection,
                                                     script.toString() );

                    // Convert each row into a new Feature for the project
                    while ( resultSet.next() )
                    {
                        this.features.add( new FeatureDetails( resultSet ) );
                    }
                }
                catch ( SQLException e )
                {
                    throw new SQLException(
                            "The features for this project could "
                            + "not be retrieved from the database.",
                            e );
                }
                finally
                {
                    if ( resultSet != null )
                    {
                        try
                        {
                            resultSet.close();
                        }
                        catch ( SQLException se )
                        {
                            // Failure to close shouldn't affect primary output.
                            LOGGER.warn( "Failed to close result set {}.",
                                         resultSet, se );
                        }
                    }

                    if ( connection != null )
                    {
                        Database.returnConnection( connection );
                    }
                }
            }
            else
            {
                // Get all features that correspond to the feature configuration
                this.features = Features.getAllDetails( this.getProjectConfig() );
            }
        }
        return this.features;
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

    public Integer getVariableId(DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        final String side = this.getInputName( dataSourceConfig );
        Integer variableId = null;

        if (ProjectDetails.LEFT_MEMBER.equals( side ))
        {
            variableId = this.getLeftVariableID();
        }
        else if (ProjectDetails.RIGHT_MEMBER.equals( side ))
        {
            variableId = this.getRightVariableID();
        }
        else if (ProjectDetails.BASELINE_MEMBER.equals( side ))
        {
            variableId = this.getBaselineVariableID();
        }

        return variableId;
    }

    /**
     * Determines the ID of the left variable
     * @return The ID of the left variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getLeftVariableID() throws SQLException
    {
        if (this.leftVariableID == null)
        {
            this.leftVariableID = Variables.getVariableID(this.getLeftVariableName());
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
     * @return The unit that the left variable is measured in
     */
    private String getLeftVariableUnit()
    {
        return this.getLeft().getVariable().getUnit();
    }

    /**
     * Determines the ID of the right variable
     * @return The ID of the right variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getRightVariableID() throws SQLException
    {
        if (this.rightVariableID == null)
        {
            this.rightVariableID = Variables.getVariableID( this.getRightVariableName());
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
     * @return The unit that the right variable is measured in
     */
    private String getRightVariableUnit()
    {
        return this.getRight().getVariable().getUnit();
    }

    /**
     * Determines the ID of the baseline variable
     * @return The ID of the baseline variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getBaselineVariableID() throws SQLException
    {
        if (this.hasBaseline() && this.baselineVariableID == null)
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
        if (this.hasBaseline())
        {
            name = this.getBaseline().getVariable().getValue();
        }
        return name;
    }

    /**
     * @return The unit that the baseline variable is measured in
     */
    private String getBaselineVariableUnit()
    {
        String unit = null;
        if (this.hasBaseline())
        {
            unit = this.getBaseline().getVariable().getUnit();
        }
        return unit;
    }

    /**
     * @return The largest possible value for the data. Double.MAX_VALUE by default.
     */
    public Double getMaximumValue()
    {
        Double maximum = Double.MAX_VALUE;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getMaximum() != null)
        {
            maximum = this.projectConfig.getPair().getValues().getMaximum();
        }

        return maximum;
    }

    public Double getDefaultMaximumValue()
    {
        Double maximum = null;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getDefaultMaximum() != null)
        {
            maximum = this.projectConfig.getPair().getValues().getDefaultMaximum();
        }

        return maximum;
    }

    /**
     * @return The smallest possible value for the data. -Double.MAX_VALUE by default
     */
    public Double getMinimumValue()
    {
        Double minimum = -Double.MAX_VALUE;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getMinimum() != null)
        {
            minimum = this.projectConfig.getPair().getValues().getMinimum();
        }

        return minimum;
    }

    public Double getDefaultMinimumValue()
    {
        Double defaultMinimum = null;

        if (this.projectConfig.getPair().getValues() != null &&
                this.projectConfig.getPair().getValues().getDefaultMinimum() != null)
        {
            defaultMinimum = this.projectConfig.getPair().getValues().getDefaultMinimum();
        }

        return defaultMinimum;
    }

    /**
     * @return The earliest possible date for any value. NULL unless specified by
     * the configuration
     */
    public String getEarliestDate()
    {
        String earliestDate = null;

        if (this.projectConfig.getPair().getDates() != null &&
            this.projectConfig.getPair().getDates().getEarliest() != null)
        {
            earliestDate = this.projectConfig.getPair().getDates().getEarliest();
        }

        return earliestDate;
    }

    /**
     * @return The latest possible date for any value. NULL unless specified by
     * the configuration
     */
    public String getLatestDate()
    {
        String latestDate = null;

        if (this.projectConfig.getPair().getDates() != null &&
                this.projectConfig.getPair().getDates().getLatest() != null)
        {
            latestDate = this.projectConfig.getPair().getDates().getLatest();
        }

        return latestDate;
    }

    /**
     * @return The earliest possible issue date for any forecast. NULL unless
     * specified by the configuration
     */
    public String getEarliestIssueDate()
    {
        String earliestDate = null;

        if (this.projectConfig.getPair().getIssuedDates() != null &&
                this.projectConfig.getPair().getIssuedDates().getEarliest() != null)
        {
            earliestDate = this.projectConfig.getPair().getIssuedDates().getEarliest();
        }

        return earliestDate;
    }

    /**
     * @return The latest possible issue date for any forecast. NULL unless
     * specified by the configuration
     */
    public String getLatestIssueDate()
    {
        String latestDate = null;

        if (this.projectConfig.getPair().getIssuedDates() != null &&
            this.projectConfig.getPair().getIssuedDates().getLatest() != null)
        {
            latestDate = this.projectConfig.getPair().getIssuedDates().getLatest();
        }

        return latestDate;
    }

    /**
     * @return Whether or not the project specifies a specific season to evaluate
     */
    public boolean specifiesSeason()
    {
        return this.projectConfig.getPair().getSeason() != null;
    }

    /**
     * @return The earliest possible day in a season. NULL unless specified in
     * the configuration
     */
    public MonthDay getEarliestDayInSeason()
    {
        MonthDay earliest = null;

        PairConfig.Season season = this.projectConfig.getPair().getSeason();

        if (season != null)
        {
            earliest = MonthDay.of(season.getEarliestMonth(), season.getEarliestDay());
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

        if (season != null)
        {
            latest = MonthDay.of(season.getLatestMonth(), season.getLatestDay());
        }

        return latest;
    }

    /**
     * Determines the expected period of lead values to retrieve from the
     * database. If there is a definition provided by a lead times pooling window
     * configuration, that period is used. Otherwise, the period from the scale
     * is used.
     *<p>
     * The period from the scale is taken into account because a range of lead
     * hours is required to scale
     *</p>
     * @return The length of a period of lead time to retrieve from the database
     * @throws NoDataException Thrown if the period could not be determined
     * from the scale
     * @throws SQLException when communication with the database failed
     */
    public Integer getLeadPeriod() throws NoDataException, SQLException
    {
        Integer period;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null &&
            this.projectConfig.getPair().getLeadTimesPoolingWindow().getPeriod() != null)
        {
            period = this.projectConfig.getPair()
                                       .getLeadTimesPoolingWindow()
                                       .getPeriod();
        }
        else
        {
            period = this.getScale().getPeriod();
        }

        return period;
    }

    /**
     * @return The period of issue times to pull from the database at once. 0
     * unless specified from the database
     */
    public int getIssuePoolingWindowPeriod()
    {
        // Indicates one basis per period
        int period = 0;

        if (this.getIssuePoolingWindow().getPeriod() != null)
        {
            period = this.getIssuePoolingWindow().getPeriod();
        }

        return period;
    }

    /**
     * Determines the unit of time that leads should be queried in. If no lead time
     * pooling window has been configured, the default is that of the expected
     * scale of the data
     * @return The unit of time that leads should be queried in
     * @throws NoDataException Thrown if there wasn't enough data available to
     * determine the scale
     * @throws SQLException when communication with the database failed
     */
    public String getLeadUnit() throws NoDataException, SQLException
    {
        String unit;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null)
        {
            unit = this.projectConfig.getPair().getLeadTimesPoolingWindow().getUnit().value();
        }
        else
        {
            unit = this.getScale().getUnit().value();
        }

        // The following ensures that the returned unit may be converted into a ChronoUnit
        if (unit != null)
        {
            unit = unit.toUpperCase();

            if (!unit.endsWith( "S" ))
            {
                unit += "S";
            }
        }

        return unit;
    }

    /**
     * Determines the frequency with which to retrieve the period of leads.
     *<p>
     * If the period is 6 hours, but the frequency is 2 hours, it will evaluate
     * 6 hours of data at a time offset by 2 hours at a time (so leads 1 through 6,
     * 3 through 8, 5 through 10, etc)
     *</p>
     * <p>
     * If a lead time pooling window has been specified, the specified frequency
     * will be used. If no frequency has been specified, the period will be used.
     *</p>
     * <p>
     * If no lead time pooling window has been specified, the specifications from
     * the scale will be used instead
     *</p>
     * @return The frequency will which to retrieve a period of leads
     * @throws NoDataException Thrown if there wasn't enough data available to
     * infer a frequency from.
     * @throws SQLException when communication with the database failed
     */
    public Integer getLeadFrequency() throws NoDataException, SQLException
    {
        Integer frequency;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null)
        {
            if (this.projectConfig.getPair().getLeadTimesPoolingWindow().getFrequency() != null)
            {
                frequency = this.projectConfig.getPair()
                                              .getLeadTimesPoolingWindow()
                                              .getFrequency();
            }
            else
            {
                frequency = this.projectConfig.getPair()
                                              .getLeadTimesPoolingWindow()
                                              .getPeriod();
            }
        }
        else
        {
            frequency = this.getScale().getFrequency();
            if (frequency == null)
            {
                frequency = this.getScale().getPeriod();
            }
        }

        return frequency;
    }

    /**
     * @return Whether or not the system should group leads itself rather than
     * relying on the configuration
     */
    private boolean shouldDynamicallyPoolByLeads()
    {
        // We'll want to pool dynamically pool if the right side is a forecast...
        boolean dynamicallyPool = ConfigHelper.isForecast( this.getRight() );
        // and there's no defined desired scale...
        dynamicallyPool = dynamicallyPool && this.projectConfig.getPair().getDesiredTimeScale() == null;
        // and there's no definition for a lead times pooling window...
        dynamicallyPool = dynamicallyPool && this.projectConfig.getPair().getLeadTimesPoolingWindow() == null;
        // and there's no definition for the existing scale for either the left or right input
        dynamicallyPool = dynamicallyPool &&
                          (this.getLeft().getExistingTimeScale() == null ||
                           this.getRight().getExistingTimeScale() == null);

        return dynamicallyPool;
    }

    /**
     * @return Whether or not data should be scaled
     * @throws NoDataException Thrown if a dynamic scale could not be created
     * @throws SQLException when communication with the database failed
     */
    public boolean shouldScale() throws NoDataException, SQLException
    {
        return this.getScale() != null &&
               !TimeScaleFunction.NONE
                       .value().equalsIgnoreCase(this.getScale().getFunction().value());
    }

    /**
     * Generates a dynamic scale based on both left and right data in the database
     *
     * <p>
     * The least common scale is used. If the left handed data is scaled to
     * 6 hours, but the right is 9, the common scale is determined to be 18
     * hours. If both sides are of the same scale, no scaling aggregation will
     * be performed. Otherwise, the scale is achieved via the mean
     * </p>
     * @return A dynamically generated scale between the left and right handed
     * data
     * @throws NoDataException Thrown if the scales for either the left or
     * right handed data could not be evaluated.
     * @throws SQLException when communication with the database failed
     */
    private TimeScaleConfig getCommonScale() throws NoDataException, SQLException
    {
        Long commonScale;

        // We're assuming we'll be scaling, so we'll need a default function
        // used to aggregate our values. For the time being, we're going to
        // roll with the average
        TimeScaleFunction scaleFunction = TimeScaleFunction.AVG;

        try
        {
            long left = this.getLeftScale();
            long right = this.getRightScale();

            long maxScale = Math.max(left, right);
            long minScale = Math.min(left, right);

            if (left == right)
            {
                commonScale = left;

                // Since the scales are actually the same, we're not going to
                // do any aggregation at all
                scaleFunction = TimeScaleFunction.NONE;
            }
            // This logic will attempt to reconcile the two to find a possible
            // desired scale; i.e. if the left is in a scale of 4 hours and the
            // right in 3, the needed scale would be 12 hours.
            else if (minScale != 0 && maxScale % minScale == 0)
            {
                String message = "The temporal scales of the left and right hand data "
                                 + "don't match. The left hand data is in a "
                                 + "scale of %d hours and the scale on the "
                                 + "right is in %d hours. If the data is "
                                 + "compatible, a scale of %d hours should "
                                 + "suffice.";
                throw new NoDataException( String.format( message, left, right, maxScale ) );
            }
            else if (!(minScale == 0 || maxScale == 0))
            {

                BigInteger bigLeft = BigInteger.valueOf( left );

                Integer greatestCommonFactor =
                        bigLeft.gcd( BigInteger.valueOf( right ) )
                               .intValue();

                commonScale =
                        left * right / greatestCommonFactor;

                String message = "The temporal scales of the left (%d Hours) "
                                 + "and right (%d Hours) hand data are in "
                                 + "different temporal scales and more "
                                 + "information is needed in order to pair "
                                 + "data properly. Please supply a desired time "
                                 + "scale. A scale of %d hours should work if "
                                 + "there is enough data and an appropriate "
                                 + "scaling function is supplied.";
                throw new NoDataException( String.format( message, left, right, commonScale ) );
            }
            else
            {
                throw new NoDataException( "Not enough data was supplied to "
                                           + "evaluate a correct scale between "
                                           + "data sources." );
            }
        }
        catch ( NoDataException e )
        {
            throw new NoDataException( "The common scale between left and "
                                       + "right inputs could not be evaluated.",
                                       e );
        }

        return new TimeScaleConfig(
                scaleFunction,
                commonScale.intValue(),
                commonScale.intValue(),
                DurationUnit.HOURS,
                "Dynamic Scale" );
    }

    /**
     * Determines the scale of the data
     * <p>
     *     If no desired scale has been configured, one is dynamically generated
     * </p>
     * @return The expected scale of the data
     * @throws NoDataException Thrown if there wasn't enough data in the
     * database to determine the scale of both the left and right inputs
     * @throws SQLException when communication with the database failed
     */
    public TimeScaleConfig getScale() throws NoDataException, SQLException
    {
        if (this.desiredTimeScale == null)
        {
            this.desiredTimeScale = this.projectConfig.getPair().getDesiredTimeScale();

            // If there is an explicit desired time scale but the frequency
            // wasn't set, we need to set the default
            if (this.desiredTimeScale != null &&
                this.desiredTimeScale.getFrequency() == null)
            {
                this.desiredTimeScale = new TimeScaleConfig(
                        this.desiredTimeScale.getFunction(),
                        this.desiredTimeScale.getPeriod(),
                        this.desiredTimeScale.getPeriod(),
                        this.desiredTimeScale.getUnit(),
                        null);
            }
        }

        if (this.desiredTimeScale == null && ConfigHelper.isForecast( this.getRight() ))
        {
            this.desiredTimeScale = this.getCommonScale();
        }
        else if(this.desiredTimeScale == null)
        {
            this.desiredTimeScale = new TimeScaleConfig(
                    TimeScaleFunction.NONE,
                    1,
                    1,
                    DurationUnit.fromValue(TimeHelper.LEAD_RESOLUTION.toString().toLowerCase() ),
                    null
            );
        }

        return this.desiredTimeScale;
    }

    /**
     * @return A possible issue dates pooling window configuration
     */
    public PoolingWindowConfig getIssuePoolingWindow()
    {
        return this.projectConfig.getPair().getIssuedDatesPoolingWindow();
    }

    /**
     * Dictates the pooling mode for the project. If an issue times pooling
     * window is established, the pooling mode is "TimeWindowMode.ROLLING",
     * "TimeWindowMode.BACK_TO_BACK" otherwise.
     * @return The pooling mode of the project. Defa
     */
    public PairingMode getPairingMode()
    {
        PairingMode mode = PairingMode.BACK_TO_BACK;

        if ( this.getIssuePoolingWindow() != null )
        {
            mode = PairingMode.ROLLING;
        }
        else if (this.usesTimeSeriesMetrics())
        {
            mode = PairingMode.TIME_SERIES;
        }

        return mode;
    }

    /**
     * @return The temporal unit used for pooling issue times
     */
    public String getIssuePoolingWindowUnit()
    {
        String unit = null;

        if ( this.projectConfig.getPair()
                               .getIssuedDatesPoolingWindow() != null )
        {
            unit = this.projectConfig.getPair()
                                     .getIssuedDatesPoolingWindow()
                                     .getUnit()
                                     .value();
        }

        return unit;
    }

    /**
     * Determines the frequency of issue times pooling windows.
     *
     * <p>
     * The default is 1 in the configured temporal units. If the frequency is
     * configured, the configured frequency is used. If that isn't configured
     * but the the issue times period is greater than 0, that is used instead.
     * </p>
     * @return The frequency with which to pool periods of issue times.
     */
    public int getIssuePoolingWindowFrequency()
    {
        // If there isn't a definition, we want to move a single unit at a time
        int frequency = 1;

        if (this.getIssuePoolingWindow().getFrequency() != null)
        {
            frequency = this.getIssuePoolingWindow().getFrequency();
        }
        else if ( this.getIssuePoolingWindowPeriod() > 0)
        {
            frequency = this.getIssuePoolingWindowPeriod();
        }

        return frequency;
    }

    /**
     * Determines whether or not to calculate ranges of lead times for retrieval
     * programmatically or to pull them from a discrete list.
     *
     * <p>
     *     Leads will be calculated if there is some sort of lead time
     *     specification in the configuration (existing scales, desired scale,
     *     or lead time pooling windows) or if ingested data are determined to
     *     be regular time series.
     * </p>
     *
     * @return Whether or not ranges of lead times should be calculated
     * @throws SQLException
     */
    private boolean shouldCalculateLeads() throws SQLException
    {
        if ( this.calculateLeads == null )
        {
            this.calculateLeads = !this.shouldDynamicallyPoolByLeads();

            if ( !this.calculateLeads )
            {
                ScriptBuilder script = new ScriptBuilder();

                script.addLine( "WITH unique_leads AS" );
                script.addLine( "(" );
                script.addTab()
                      .addLine(
                              "SELECT FV.lead, lag(FV.lead) OVER ( ORDER BY FV.lead)" );
                script.addTab().addLine( "FROM wres.ForecastValue FV" );
                script.addTab().addLine( "WHERE FV.lead > 0" );

                if ( this.getMaximumLeadHour() != Integer.MAX_VALUE )
                {
                    script.addTab( 2 )
                          .addLine( "AND FV.lead <= ",
                                    this.getMaximumLeadHour() );
                }

                script.addTab( 2 ).addLine( "AND EXISTS (" );
                script.addTab( 3 ).addLine( "SELECT 1" );
                script.addTab( 3 ).addLine( "FROM wres.ForecastSource FS" );
                script.addTab( 3 )
                      .addLine( "INNER JOIN wres.ProjectSource PS" );
                script.addTab( 4 )
                      .addLine( "ON PS.source_id = FS.source_id" );
                script.addTab( 3 )
                      .addLine( "WHERE FS.forecast_id = FV.timeseries_id" );
                script.addTab( 4 )
                      .addLine( "AND PS.project_id = ", this.getId() );
                script.addTab( 4 ).addLine( "AND PS.member = 'right'" );
                script.addTab( 2 ).addLine( ")" );
                script.addTab().addLine( "GROUP BY FV.lead" );
                script.addLine( ")" );
                script.addLine( "SELECT MAX(row_number) = 1 AS is_regular" );
                script.addLine( "FROM (" );
                script.addTab()
                      .addLine(
                              "SELECT lead - lag, row_number() OVER (ORDER BY lead - lag)" );
                script.addTab().addLine( "FROM unique_leads" );
                script.addTab().addLine( "WHERE lag IS NOT NULL" );
                script.addTab().addLine( "GROUP BY lead - lag" );
                script.addLine( ") AS differences;" );

                this.calculateLeads = script.retrieve( "is_regular" );
            }
        }

        return this.calculateLeads;
    }

    /**
     * Determines the number of time series that would be prudent to retrieve
     * from the database at once
     * <p>
     *     If small data sets are involved, pulling many series at once is a
     *     good idea. If a large dataset is involved, that may end in retrievals
     *     gathering several megabytes of data per series. If many series are all
     *     loaded within the same task and many tasks are running at once, it
     *     could end up greatly increasing the memory footprint and execution
     *     time of the application.
     * </p>
     * <p>
     *     This value isn't precise and therefore doesn't need to make a huge
     *     effort in doing so. The application can generate a rough estimate of
     *     the size of the data set that will be retrieved, but it cannot
     *     factor in overhead such as IO time on the database server, object
     *     overhead at runtime, or the number of threads that will be
     *     retrieving data all at once.
     * </p>
     * @return The number of Time Series' to return in one window
     * @throws SQLException Thrown if the number could not be computed in the database
     */
    public Integer getNumberOfSeriesToRetrieve() throws SQLException
    {
        synchronized ( ProjectDetails.SERIES_AMOUNT_LOCK )
        {
            if ( this.numberOfSeriesToRetrieve == null )
            {

                ScriptBuilder script = new ScriptBuilder();
                script.addLine( "SELECT (" );
                script.addTab().addLine( "(" );
                script.addTab( 2 )
                      .addLine(
                              "(COUNT(E.ensemble_id) * 8 + 20) * -- This determines the size of a single row" );
                script.addTab( 3 )
                      .addLine(
                              "(  -- This determines the number of expected rows" );
                script.addTab( 4 ).addLine( "SELECT COUNT(*)" );
                script.addTab( 4 ).addLine( "FROM wres.ForecastValue FV" );
                script.addTab( 4 ).addLine( "INNER JOIN (" );
                script.addTab( 5 ).addLine( "SELECT TS.timeseries_id" );
                script.addTab( 5 ).addLine( "FROM wres.TimeSeries TS" );
                script.addTab( 5 )
                      .addLine( "INNER JOIN wres.ForecastSource FS" );
                script.addTab( 6 )
                      .addLine( "ON FS.forecast_id = TS.timeseries_id" );
                script.addTab( 5 )
                      .addLine( "INNER JOIN wres.ProjectSource PS" );
                script.addTab( 6 ).addLine( "ON PS.source_id = FS.source_id" );
                script.addTab( 5 )
                      .addLine( "WHERE PS.project_id = ", this.getId() );
                script.addTab( 6 )
                      .addLine( "AND PS.member = ",
                                ProjectDetails.RIGHT_MEMBER );
                script.addTab( 5 ).addLine( "LIMIT 1" );
                script.addTab( 4 ).addLine( ") AS TS" );
                script.addTab( 4 )
                      .addLine( "ON TS.timeseries_id = FV.timeseries_id" );
                script.addTab( 3 ).addLine( ")" );
                script.addTab( 2 )
                      .addLine(
                              ") / 1000.0)::float AS size     -- We divide by 1000.0 to convert the number to kilobyte scale" );
                script.addLine(
                        "-- We Select from ensemble because the number of ensembles affects" );
                script.addLine(
                        "--   the number of values returned in the resultant array" );
                script.addLine( "FROM wres.Ensemble E" );
                script.addLine( "WHERE EXISTS (" );
                script.addTab().addLine( "SELECT 1" );
                script.addTab().addLine( "FROM wres.TimeSeries TS" );
                script.addTab().addLine( "INNER JOIN wres.ForecastSource FS" );
                script.addTab( 2 )
                      .addLine( "ON FS.forecast_id = TS.timeseries_id" );
                script.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
                script.addTab( 2 ).addLine( "ON PS.source_id = FS.source_id" );
                script.addTab()
                      .addLine( "WHERE PS.project_id = ", this.getId() );
                script.addTab( 2 )
                      .addLine( "AND PS.member = ",
                                ProjectDetails.RIGHT_MEMBER );
                script.addTab( 2 )
                      .addLine( "AND TS.ensemble_id = E.ensemble_id" );
                script.addLine( ");" );

                double timeSeriesSize = script.retrieve( "size" );

                // We're going to limit the size of a time series group to 1MB
                final int sizeCap = 1000;

                // The number of series to retrieve will either be 1 or the number
                // of time series that may be retrieved and still fit under the cap.
                // Using Math.max ensures that we'll pull at least one value at a time.
                // If a single time series has an estimated size of 1186KB, we'll
                // only retrieve one time series at a time. If the estimated size is
                // 400KB, we'll bring in two at a time.
                this.numberOfSeriesToRetrieve =
                        ( ( Double ) Math.max( 1.0, sizeCap / timeSeriesSize ) )
                                .intValue();
            }

            return this.numberOfSeriesToRetrieve;
        }
    }

    /**
     * @return Indicates whether or not pairs will be fed into time series
     * only metrics
     * 
     * TODO: replace this with a call to {@link ProjectConfigs#hasTimeSeriesMetrics(ProjectConfig)}. 
     * JBr: unclear why this was explicitly duplicated again - the reason I haven't removed it again - but this check
     * for nullity is not needed. Implementation details aside, we shouldn't duplicate such helpers.
     */
    public boolean usesTimeSeriesMetrics()
    {
        return Collections.exists(
                this.projectConfig.getMetrics(),
                metric -> !metric.getTimeSeriesMetric().isEmpty()
        );
    }

    /**
     * @return The desired unit of measurement for all values
     */
    public String getDesiredMeasurementUnit()
    {
        return String.valueOf(this.projectConfig.getPair().getUnit());
    }

    /**
     * @return A list of all configurations stating where to store pair output
     */
    public List<DestinationConfig> getPairDestinations()
    {
        return ConfigHelper.getDestinationsOfType( this.getProjectConfig(), DestinationType.PAIRS );
    }

    /**
     * Determines the overall range of leads to use in a given window
     * @param feature The feature who the range belongs to
     * @param windowNumber The iteration number over lead times
     * @return A pair containing the earliest lead and latest lead
     * @throws SQLException Thrown if the lead offset could not be determined
     * @throws IOException Thrown if irregular lead times could not be determined
     */
    public Pair<Integer, Integer> getLeadRange(final Feature feature, final int windowNumber)
            throws SQLException, IOException
    {
        Integer beginning;
        Integer end;

        if (this.shouldCalculateLeads())
        {
            int frequency = (int)TimeHelper.unitsToLeadUnits( this.getLeadUnit(), this.getLeadFrequency() );
            int period = (int)TimeHelper.unitsToLeadUnits( this.getLeadUnit(), this.getLeadPeriod() );
            Integer offset = this.getLeadOffset( feature );
            beginning = windowNumber * frequency + offset;
            end = beginning + period;
        }
        else
        {
            if (this.discreteLeads == null)
            {
                populateDiscreteLeads();
            }

            // We can probably do an operation on period to get a full scale
            // for the irregular series, i.e., if this gives us a period of 1,
            // but we might be able to pull off
            // [this.discreteLeads.get(feature)[x] - period, this.discreteLeads.get(feature)[x]]
            beginning = this.discreteLeads.get( feature )[windowNumber];
            end = beginning;
        }

        return Pair.of( beginning, end );
    }

    /**
     * Creates a SQL where clause stating which leads to use for retrieval
     * @param feature The feature whose leads to retrieve
     * @param windowNumber The identifier for the current iteration over lead times
     * @param alias The alias for a table to be used in the SQL statement
     * @return A SQL where clause stating which leads to use for retrieval
     * @throws IOException Thrown  if the range of leads cannot be determined
     * @throws SQLException Thrown  if the range of leads cannot be determined
     */
    public String getLeadQualifier(Feature feature, Integer windowNumber, String alias)
            throws IOException, SQLException
    {
        Pair<Integer, Integer> range = this.getLeadRange( feature, windowNumber );
        String qualifier = "";

        if (Strings.hasValue( alias ) && !alias.endsWith( "." ))
        {
            alias += ".";
        }
        else
        {
            alias = "";
        }

        if (Math.abs(range.getLeft() - range.getRight()) <= 1)
        {
            qualifier += alias;
            qualifier += "lead = ";
            qualifier += range.getRight();
        }
        else
        {
            qualifier += range.getLeft();
            qualifier += " < ";
            qualifier += alias;
            qualifier += "lead AND ";
            qualifier += alias;
            qualifier += "lead <= ";
            qualifier += range.getRight();
        }

        return qualifier;
    }

    /**
     * Retrieves and caches all discrete lead times for each feature for the project.
     *
     * <p>
     *     Used for organizing lead times to evaluate for irregular time series
     * </p>
     * @throws IOException Thrown if the total list of locations and variables
     * to evaluate could not be loaded
     * @throws IOException Thrown if the process of populating lead times is
     * interrupted
     * @throws IOException Thrown if an error occurred while loading the data
     * to populate the lead times
     */
    private void populateDiscreteLeads() throws IOException
    {
        Connection connection = null;
        ResultSet resultSet = null;
        Map<FeatureDetails, Future<DataSet>> futureLeads = new LinkedHashMap<>(  );

        long width = Math.max(1, this.getMinimumLeadHour());

        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("SELECT FV.lead");
        script.addLine("FROM wres.ForecastValue FV");
        script.addLine("WHERE FV.lead >= ", width);

        if (this.getMaximumLeadHour() != Integer.MAX_VALUE)
        {
            script.addTab().addLine("AND FV.lead <= ", this.getMaximumLeadHour());
        }

        if (this.getMaximumValue() != Double.MAX_VALUE)
        {
            script.addTab().addLine("AND FV.forecasted_value <= ", this.getMaximumValue());
        }

        if (this.getMinimumValue() != -Double.MAX_VALUE)
        {
            script.addTab().addLine("AND FV.forecasted_value >= ", this.getMinimumValue());
        }

        script.addTab().addLine("AND EXISTS (");
        script.addTab( 2 ).addLine( "SELECT 1");
        script.addTab( 2 ).addLine( "FROM wres.TimeSeries TS");
        script.addTab( 2 ).addLine( "INNER JOIN wres.ForecastSource FS");
        script.addTab(  3  ).addLine(  "ON FS.forecast_id = TS.timeseries_id");
        script.addTab( 2 ).addLine( "INNER JOIN wres.ProjectSource PS");
        script.addTab(  3  ).addLine(  "ON PS.source_id = FS.source_id");
        script.addTab( 2 ).addLine( "WHERE TS.timeseries_id = FV.timeseries_id");
        script.addTab(  3  ).addLine(  "AND PS.project_id = ", this.getId());
        script.addTab(  3  ).addLine(  "AND PS.member = 'right'");

        if (Strings.hasValue( this.getEarliestIssueDate() ))
        {
            script.addTab(3).addLine("AND TS.initialization_date >= '", this.getEarliestIssueDate(), "'");
        }

        if (Strings.hasValue( this.getLatestIssueDate() ))
        {
            script.addTab(3).addLine("AND TS.initialization_date <= '", this.getLatestIssueDate(), "'");
        }

        if (Strings.hasValue(this.getEarliestDate()))
        {
            script.addTab(3).addLine("AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead' >= '", this.getEarliestDate(), "'");
        }

        if (Strings.hasValue( this.getLatestDate() ))
        {
            script.addTab(3).addLine("AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead <= '", this.getLatestDate(), "'");
        }

        String beginning = script.toString();

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults( connection,
                                             ScriptGenerator.formVariablePositionLoadScript( this, false ) );

            while (resultSet.next())
            {
                script = new ScriptBuilder( beginning );

                script.addTab(  3  ).addLine(  "AND TS.variableposition_id = ", resultSet.getInt( "forecast_position" ));
                script.addTab().addLine(")");
                script.addLine("GROUP BY FV.lead");
                script.addLine("ORDER BY FV.lead;");

                futureLeads.put( new FeatureDetails( resultSet ),
                                 Database.submit( new DataSetRetriever( script.toString() ) ) );
            }
        }
        catch (SQLException e)
        {
            throw new IOException( "Tasks used to determine discrete lead hours "
                                   + "could not be created.", e );
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch (SQLException e)
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "A database result set {} could not be closed.",
                                 resultSet, e );
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        while (!futureLeads.isEmpty())
        {
            FeatureDetails key = (FeatureDetails)futureLeads.keySet().toArray()[0];
            Feature feature = key.toFeature();
            Future<DataSet> futureDataSet = futureLeads.remove( key );
            List<Integer> leadList = new ArrayList<>();
            DataSet dataSet;
            try
            {
                // If we're on the last/only task, there's no reason to try and
                // cycle through it
                if (futureLeads.isEmpty())
                {
                    dataSet = futureDataSet.get();
                }
                else
                {
                    dataSet = futureDataSet.get( 500, TimeUnit.MILLISECONDS );
                }

                if (dataSet == null)
                {
                    continue;
                }

                while (dataSet.next())
                {
                    leadList.add(dataSet.getInt( "lead" ));
                }

                if (this.discreteLeads == null)
                {
                    this.discreteLeads = new TreeMap<>(ConfigHelper.getFeatureComparator());
                }

                this.discreteLeads.put( feature,
                                        leadList.toArray( new Integer[leadList.size()] ) );
            }
            catch ( InterruptedException e )
            {
                LOGGER.warn( "Population of discrete leads has been interrupted.",
                             e );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "An error occurred while populating discrete leads", e );
            }
            catch ( TimeoutException e )
            {
                LOGGER.trace("It took too long to get the set of discrete leads "
                             + "for '{}'; moving on to another location while "
                             + "we wait for the output for this location.",
                             ConfigHelper.getFeatureDescription( feature ));
                futureLeads.put( key, futureDataSet );
            }

        }
    }

    /**
     * Retrieves the offset for lead times to use to pull a valid window for a
     * feature.
     *
     * <p>
     *     All Offsets are cached at once to reduce load times for projects
     *     with many locations
     * </p>
     * @param feature The feature to find the offset for
     * @return The needed offset to match the first valid window with observations
     * @throws IOException Thrown if tasks used to evaluate all lead offsets
     * could not be executed
     * @throws IOException Thrown if the population of all lead offsets is
     * interrupted
     * @throws SQLException Thrown if offsets cannot be populated due to the
     * system being unable to determine what locations to retrieve offsets from.
     */
    public Integer getLeadOffset(Feature feature)
            throws IOException, SQLException
    {
        if (ConfigHelper.isSimulation( this.getRight() ))
        {
            return 0;
        }

        if (this.leadOffsets.isEmpty())
        {
            this.populateLeadOffsets();
        }

        return this.leadOffsets.get( feature );
    }

    /**
     * Caches the lead time offsets for each location in the project
     * <p>
     *     When each offset is retrieved on demand, it adds several seconds to
     *     the evaluation of each location. When there are many locations, this
     *     adds up. When they are all pulled semi-simultaenously, execution
     *     of the evaluation may occur almost immediately upon evaluating its
     *     location.
     * </p>
     * @throws IOException Thrown if the locations with which to evaluate cannot
     * be loaded
     * @throws IOException Thrown if the loading of the actual offset values fails
     * @throws IOException Thrown if the loading of the offset values is interrupted
     * @throws SQLException Thrown if the script used to determine what
     * locations to find offsets for cannot be formed
     */
    private void populateLeadOffsets() throws IOException, SQLException
    {
        FormattedStopwatch timer = null;

        if (LOGGER.isDebugEnabled())
        {
            timer = new FormattedStopwatch();
            timer.start();
        }

        long width = TimeHelper.unitsToLeadUnits( this.getScale().getUnit().value(),
                                                     this.getScale().getPeriod());

        String script = ScriptGenerator.formVariablePositionLoadScript( this, true );
        ScriptBuilder part = new ScriptBuilder(  );

        part.addLine("SELECT TS.offset");
        part.addLine("FROM (");
        part.addTab().add("SELECT TS.initialization_date + INTERVAL '1 HOUR' * (FV.lead + ", width, ")");

        if (this.getRight().getTimeShift() != null)
        {
            part.add(" + '", this.getRight().getTimeShift().getWidth(), " ", this.getRight().getTimeShift().getUnit().value(), "'");
        }

        part.addLine(" AS valid_time,");
        part.addTab(  2  ).addLine("FV.lead - ", width, " AS offset");
        part.addTab().addLine("FROM wres.TimeSeries TS");
        part.addTab().addLine("INNER JOIN wres.ForecastValue FV");
        part.addTab(  2  ).addLine("ON FV.timeseries_id = TS.timeseries_id");
        part.addTab().add("WHERE TS.variableposition_id = ");

        String beginning = part.toString();

        part = new ScriptBuilder(  );

        if (this.getMinimumLeadHour() != Integer.MIN_VALUE)
        {
            part.addTab(  2  ).addLine( "AND FV.lead >= ", (this.getMinimumLeadHour() - 1) + width);
        }
        else
        {
            part.addTab(  2  ).addLine( "AND FV.lead >= ", width);
        }

        /*if (width > 1 || this.getMinimumLeadHour() != Integer.MIN_VALUE)
        {
            part.addTab( 2 ).addLine( "AND FV.lead >= ", Math.max( width, this.getMinimumLeadHour() ) );
        }*/


        if (this.getMaximumLeadHour() != Integer.MAX_VALUE)
        {
            part.addTab(  2  ).addLine( "AND FV.lead <= ", this.getMaximumLeadHour());
        }

        if (Strings.hasValue( this.getEarliestIssueDate() ))
        {
            part.addTab(  2  ).addLine("AND TS.initialization_date >= '", this.getEarliestIssueDate() + "'");
        }

        if (Strings.hasValue( this.getLatestIssueDate() ))
        {
            part.addTab(  2  ).addLine("AND TS.initialization_date <= '", this.getLatestIssueDate(), "'");
        }

        part.addTab(  2  ).addLine("AND EXISTS (");
        part.addTab(   3   ).addLine("SELECT 1");
        part.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        part.addTab(   3   ).addLine("INNER JOIN wres.ForecastSource FS");
        part.addTab(    4    ).addLine("ON FS.source_id = PS.source_id");
        part.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getId());
        part.addTab(    4    ).addLine("AND PS.member = 'right'");
        part.addTab(    4    ).addLine("AND FS.forecast_id = TS.timeseries_id");
        part.addTab(  2  ).addLine(")");
        part.addTab(  2  ).addLine("ORDER BY valid_time");


        part.addLine(") AS TS");
        part.addLine("INNER JOIN (");
        part.addTab().addLine("SELECT O.observation_time");
        part.addTab().addLine("FROM (");
        part.addTab(  2  ).addLine("SELECT source_id");
        part.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        part.addTab(  2  ).addLine("WHERE PS.project_id = ", this.getId());
        part.addTab(   3  ).addLine("AND PS.member = ", ProjectDetails.LEFT_MEMBER);
        part.addTab().addLine(") AS PS");
        part.addTab().addLine("INNER JOIN (");
        part.addTab(  2  ).add("SELECT source_id, observation_time");

        if (this.getLeft().getTimeShift() != null)
        {
            part.add(" + '", this.getLeft().getTimeShift().getWidth(), " ", this.getLeft().getTimeShift().getUnit().value(), "'");
        }

        part.addLine(" AS observation_time");
        part.addTab(  2  ).addLine("FROM wres.Observation O");
        part.addTab(  2  ).add("WHERE O.variableposition_id = ");

        String middle = part.toString();

        part = new ScriptBuilder(  );

        if (Strings.hasValue( this.getEarliestDate() ))
        {
            part.addTab(   3   ).addLine("AND O.observation_time >= '", this.getEarliestDate(), "'");
        }

        if (Strings.hasValue( this.getLatestDate() ))
        {
            part.addTab(   3   ).addLine("AND O.observation_time <= '", this.getLatestDate(), "'");
        }

        part.addTab().addLine(") AS O");
        part.addTab(  2  ).addLine("ON O.source_id = PS.source_id");
        part.addLine(") AS OT");
        part.addTab().addLine("ON OT.observation_time = TS.valid_time");
        part.addLine("ORDER BY TS.offset");
        part.addLine("LIMIT 1;");

        String end = part.toString();

        Connection connection = null;
        ResultSet resultSet = null;
        Map<FeatureDetails.FeatureKey, Future<Integer>> futureOffsets = new LinkedHashMap<>(  );

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults( connection, script );

            LOGGER.trace("Variable position metadata loaded...");

            LOGGER.info("Loading preliminary metadata...");

            while (resultSet.next())
            {
                ScriptBuilder finalScript = new ScriptBuilder( );
                finalScript.add(beginning);
                finalScript.addLine((Integer)Database.getValue( resultSet, "forecast_position" ));
                finalScript.add(middle);
                finalScript.addLine((Integer)Database.getValue( resultSet, "observation_position" ));
                finalScript.addLine(end);

                FeatureDetails.FeatureKey key = new FeatureDetails.FeatureKey(
                        Database.getValue(resultSet, "comid"),
                        Database.getValue(resultSet, "lid"),
                        Database.getValue( resultSet,"gage_id"),
                        Database.getValue(resultSet,"huc" )
                );

                futureOffsets.put(key, finalScript.submit( "offset" ));

                LOGGER.trace( "A task has been created to find the offset for {}.", key );
            }
        }
        catch ( SQLException e )
        {
            throw new IOException("Tasks used to evaluate lead hour offsets "
                                  + "could not be created.", e);
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch ( SQLException e )
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "Database result set {} could not be closed.",
                                 resultSet, e);
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        ProgressMonitor.resetMonitor();
        ProgressMonitor.setSteps( (long)futureOffsets.size() );

        while (!futureOffsets.isEmpty())
        {
            FeatureDetails.FeatureKey key = ( FeatureDetails.FeatureKey)futureOffsets.keySet().toArray()[0];
            Future<Integer> futureOffset = futureOffsets.remove( key );
            Feature feature = new FeatureDetails( key ).toFeature();
            Integer offset;
            try
            {
                LOGGER.trace( "Loading the offset for '{}'", ConfigHelper.getFeatureDescription( feature ));

                if (futureOffsets.isEmpty())
                {
                    offset = futureOffset.get();
                }
                else
                {
                    offset = futureOffset.get( 500, TimeUnit.MILLISECONDS );
                }

                if (offset == null)
                {
                    continue;
                }
                LOGGER.trace("The offset was: {}", offset);

                this.leadOffsets.put(
                        feature,
                        offset
                );
                LOGGER.trace( "An offset for {} was loaded!", ConfigHelper.getFeatureDescription( feature ) );
                ProgressMonitor.completeStep();
            }
            catch ( InterruptedException e )
            {
                LOGGER.warn( "Population of lead offsets has been interrupted.", e );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                throw new IOException("An error occured while populating the future"
                             + "offsets.", e);
            }
            catch ( TimeoutException e )
            {
                LOGGER.trace("It took too long to get the offset for '{}'; "
                             + "moving on to another location while we wait "
                             + "for the output on this location",
                             ConfigHelper.getFeatureDescription( feature ));
                futureOffsets.put( key, futureOffset );
            }
        }

        if (LOGGER.isDebugEnabled())
        {
            timer.stop();
            LOGGER.debug( "It took {} to get the offsets for all locations.",
                          timer.getFormattedDuration() );
        }

        ProgressMonitor.resetMonitor();
    }

    /**
     * Determines the number of pools for issue date pooling for a feature
     *
     * <p>
     *     If we know that we want to pool data together two issue days at a time,
     *     staggered by a single day, over data spanning four days, this will
     *     tell us that we have three pools to iterate over
     * </p>
     * <p>
     *     Aids in the process of iterating over windows/MetricInputs
     * </p>
     * @param feature The feature whose pool count to use
     * @return The number of issue date pools for a feature
     * @throws SQLException Thrown if the script used to find the count could
     * not be created
     * @throws SQLException Thrown if the count could not be retrieved
     */
    public Integer getIssuePoolCount( Feature feature) throws SQLException
    {
        if ( this.getPairingMode() != PairingMode.ROLLING)
        {
            return -1;
        }

        synchronized ( POOL_LOCK )
        {
            if (!this.poolCounts.containsKey( feature ))
            {
                this.addIssuePoolCount( feature );
            }
        }

        return this.poolCounts.get( feature );
    }

    /**
     * Adds the number of issue pools for a feature to the cache
     * @param feature The feature whose count  we want to cache
     * @throws SQLException Thrown if the script used to find the count could
     * not be created
     * @throws SQLException Thrown if the count could not be retrieved
     */
    private void addIssuePoolCount( Feature feature) throws SQLException
    {
        String rollingScript = ScriptGenerator.formIssuePoolCountScript( this, feature );

        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, rollingScript );

            if (resultSet.isBeforeFirst())
            {
                resultSet.next();

                Integer windowCount = Database.getValue( resultSet, "window_count" );

                if (windowCount != null)
                {
                    this.poolCounts.put( feature,
                                         Database.getValue( resultSet,
                                                            "window_count" ) );
                }
                else
                {
                    throw new SQLException( "There was no intersection between "
                                            + "observation and forecast data for '"
                                            + ConfigHelper.getFeatureDescription( feature )
                                            + "'." );
                }
            }
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch ( SQLException se )
                {
                    // Failure to close resource shouldn't affect primary output
                    LOGGER.warn( "Failed to close result set {}.", resultSet, se );
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }
    }

    private String getProjectName()
    {
        return this.projectConfig.getName();
    }

    /**
     * Returns unique identifier for this project's config+data
     * @return The unique ID
     */
    private Integer getInputCode()
    {
        return this.inputCode;
    }

    /**
     * Evaluates the scale of the left side of the data
     * <p>
     *     If no existing time scale has been dictated, it is evaluated from the
     *     database
     * </p>
     * @return The number of standard temporal units between each value on the
     * left side of the data
     * @throws NoDataException Thrown if there wasn't enough data available to
     * evaluate the scale
     * @throws SQLException Thrown if an error was encountered while retrieving
     * scale information from the database
     */
    private long getLeftScale() throws NoDataException, SQLException
    {
        synchronized ( LEFT_LEAD_LOCK )
        {
            if ( this.leftScale == -1 )
            {
                if ( this.getLeft().getExistingTimeScale() == null )
                {
                    this.leftScale = this.getScale( this.getLeft() );
                }
                else
                {
                    this.leftScale =
                            TimeHelper.unitsToLeadUnits( this.getLeft()
                                                             .getExistingTimeScale()
                                                             .getUnit()
                                                             .value(),
                                                         this.getLeft()
                                                             .getExistingTimeScale()
                                                             .getPeriod() );
                }
            }
        }

        return leftScale;
    }

    /**
     * Evaluates the scale of the right side of the data
     * <p>
     *     If no existing time scale has been dictated, it is evaluated from the
     *     database
     * </p>
     * @return The number of standard temporal units between each value on the
     * right side of the data
     * @throws NoDataException Thrown if there wasn't enough data available to
     * evaluate the scale
     * @throws SQLException Thrown if an error was encountered while retrieving
     * scale information from the database
     */
    private long getRightScale() throws NoDataException, SQLException
    {
        synchronized ( RIGHT_LEAD_LOCK )
        {
            if ( this.rightScale == -1 )
            {
                if ( this.getRight().getExistingTimeScale() == null )
                {
                    this.rightScale = this.getScale( this.getRight() );
                }
                else
                {
                    this.rightScale =
                            TimeHelper.unitsToLeadUnits(
                                    this.getRight()
                                        .getExistingTimeScale()
                                        .getUnit()
                                        .value(),
                                    this.getRight()
                                        .getExistingTimeScale()
                                        .getPeriod()
                            );
                }
            }
        }

        return this.rightScale;
    }

    /**
     * Evaluates the scale of the baseline
     * <p>
     *     If no existing time scale has been dictated, it is evaluated from the
     *     database
     * </p>
     * @return The number of standard temporal units between each value in the
     * baseline
     * @throws NoDataException Thrown if there wasn't enough data available to
     * evaluate the scale
     * @throws SQLException Thrown if an error was encountered while retrieving
     * scale information from the database
     */
    public long getBaselineScale() throws NoDataException, SQLException
    {
        synchronized ( BASELINE_LEAD_LOCK )
        {
            if ( this.getBaseline() != null && this.baselineScale == -1 )
            {
                if ( this.getBaseline().getExistingTimeScale() == null )
                {
                    this.baselineScale = this.getScale( this.getBaseline() );
                }
                else
                {
                    this.baselineScale =
                            TimeHelper.unitsToLeadUnits( this.getBaseline()
                                                             .getExistingTimeScale()
                                                             .getUnit()
                                                             .value(),
                                                         this.getBaseline()
                                                             .getExistingTimeScale()
                                                             .getPeriod() );
                }
            }
        }

        return this.baselineScale;
    }

    /**
     * Determines the number of standard lead units between values
     * @param dataSourceConfig The specification for which values to investigate
     * @return The number of standard lead units between values
     * @throws NoDataException Thrown if the scale could not be determined
     * @throws SQLException Thrown if an error occurred while retrieving the
     * scale from the database
     */
    private int getScale(DataSourceConfig dataSourceConfig)
            throws NoDataException, SQLException
    {
        Integer leadScale;

        if (ConfigHelper.isForecast( dataSourceConfig ))
        {
            leadScale = this.getForecastScale( dataSourceConfig );
        }
        else
        {
            leadScale = this.getObservationScale( dataSourceConfig );
        }

        if (leadScale == null || leadScale < 1)
        {
            throw new NoDataException("The scale for the " +
                                      this.getInputName( dataSourceConfig ) +
                                      " either could not be determined or was "
                                      + "invalid. (lead = " +
                                      leadScale +
                                      ")");
        }

        return leadScale;
    }

    /**
     * Retrieves the scale of forecast data from the database
     * @param dataSourceConfig The specification for the data with which to investigate
     * @return The number of standard lead units between values
     * @throws SQLException Thrown if the number of lead units could not be retrieved
     * from the database
     */
    private Integer getForecastScale(DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("WITH differences AS");
        script.addLine("(");
        script.addLine("    SELECT lead - lag(lead) OVER (ORDER BY FV.timeseries_id, lead) AS difference");
        script.addLine("    FROM wres.ForecastValue FV");
        script.addLine("    INNER JOIN wres.TimeSeries TS");
        script.addLine("        ON TS.timeseries_id = FV.timeseries_id");

        if (this.getMinimumLeadHour() != Integer.MIN_VALUE)
        {
            script.addTab().addLine("WHERE lead > ", this.getMinimumLeadHour());
        }
        else
        {
            script.addLine("    WHERE lead > 0");
        }

        if (this.getMaximumLeadHour() != Integer.MAX_VALUE)
        {
            script.addTab().addLine("AND lead <= ", this.getMaximumLeadHour());
        }
        else
        {
            // Set the maximum to 500. If the maximum is lead is 200, then this
            // not should behave much differently than having no clause at all.
            // If real maximum was 2880, 500 will provide a large enough sample
            // size and produce the correct values in a slightly faster fashion.
            // In one data set, leaving this out causes this to take 11.5s .
            // That was even with a subset of the real data (1 month vs 30 years).
            // If we cut it to 500, it now takes 1.6s. Still not great, but
            // much faster
            script.addTab().addLine( "AND lead <= ", 500 );
        }

        Optional<FeatureDetails> featureDetails = this.getFeatures().stream().findFirst();

        if (featureDetails.isPresent())
        {
            String variablePositionClause = ConfigHelper.getVariablePositionClause(
                    featureDetails.get().toFeature(),
                    this.getVariableId( dataSourceConfig ),
                    "TS" );
            script.addLine("        AND ", variablePositionClause);

        }

        script.addLine("        AND EXISTS (");
        script.addLine("            SELECT 1");
        script.addLine("            FROM wres.ProjectSource PS");
        script.addLine("            INNER JOIN wres.ForecastSource FS");
        script.addLine("                ON FS.source_id = PS.source_id");
        script.addLine("            WHERE PS.project_id = ", this.getId());
        script.addLine("                AND PS.member = ", this.getInputName( dataSourceConfig ));
        script.addLine("                AND FV.timeseries_id = FS.forecast_id");
        script.addLine("        )");
        script.addLine(")");
        script.addLine("SELECT MIN(difference)::integer AS scale");
        script.addLine("FROM differences");
        script.addLine("WHERE difference IS NOT NULL");
        script.addLine("    AND difference > 0");

        return script.retrieve( "scale" );
    }

    /**
     * Retrieves the scale of observation data from the database
     * @param dataSourceConfig The specification for the data with which to investigate
     * @return The number of standard lead units between values
     * @throws SQLException Thrown if the number of lead units could not be retrieved
     * from the database
     */
    private int getObservationScale(DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("WITH differences AS");
        script.addLine("(");
        script.addLine("    SELECT AGE(observation_time, (LAG(observation_time) OVER (ORDER BY observation_time)))");
        script.addLine("    FROM wres.Observation O");

        Optional<FeatureDetails> featureDetails = this.getFeatures().stream().findFirst();
        int tabCount;

        if (featureDetails.isPresent())
        {
            String variablePositionClause = ConfigHelper.getVariablePositionClause(
                    featureDetails.get().toFeature(),
                    this.getVariableId( dataSourceConfig ),
                    "O" );
            script.addLine("    WHERE ", variablePositionClause);
            tabCount = 3;
            script.addTab(  2  ).add("AND ");

        }
        else
        {
            tabCount = 2;
            script.addTab().add("WHERE ");
        }


        script.addLine("EXISTS (");

        script.addTab(tabCount).addLine("SELECT 1");
        script.addTab(tabCount).addLine("FROM wres.ProjectSource PS");
        script.addTab(tabCount).addLine("WHERE PS.project_id = ", this.getId());
        script.addTab(tabCount).addTab().addLine("AND PS.member = ", this.getInputName( dataSourceConfig ) );
        script.addTab(tabCount).addTab().addLine("AND PS.source_id = O.source_id");
        script.addTab().addLine(")");
        script.addTab().addLine("GROUP BY observation_time");
        script.addLine(")");

        // TODO: When we change the scale of the lead column, we need to change this as well
        // We want the number of hours in the scale. It was previously "Extract ( hours ... ",
        // but, for 1 day, was returning 0 hours (considering there were 0 hours into the day
        // of the interval
        script.addLine("SELECT ( EXTRACT( epoch FROM MIN(age))/3600 )::integer AS scale");
        script.addLine("FROM differences");
        script.addLine("WHERE age IS NOT NULL");
        script.addLine("GROUP BY age;");

        return script.retrieve( "scale" );
    }

    /**
     * @return Whether or not baseline data is involved in the project
     */
    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    @Override
    public Integer getId() {
        return this.projectID;
    }

    @Override
    protected String getIDName() {
        return "project_id";
    }

    @Override
    protected void setID(Integer id)
    {
        this.projectID = id;
    }

    @Override
    protected PreparedStatement getInsertSelectStatement( Connection connection )
            throws SQLException
    {
        List<Object> args = new ArrayList<>();
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("WITH new_project AS");
        script.addLine("(");
        script.addTab().addLine("INSERT INTO wres.Project (project_name, input_code)");
        script.addTab().addLine("SELECT ?, ?");

        args.add(this.getProjectName());
        args.add(this.getInputCode());

        script.addTab().addLine("WHERE NOT EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.Project P");
        script.addTab(  2  ).addLine("WHERE P.input_code = ?");

        args.add(this.getInputCode());

        script.addTab().addLine(")");
        script.addTab().addLine("RETURNING project_id");
        script.addLine(")");
        script.addLine("SELECT project_id, TRUE AS wasInserted");
        script.addLine("FROM new_project");
        script.addLine(  );
        script.addLine("UNION");
        script.addLine();
        script.addLine("SELECT project_id, FALSE AS wasInserted");
        script.addLine("FROM wres.Project P");
        script.addLine("WHERE P.input_code = ?;");

        args.add(this.getInputCode());

        return script.getPreparedStatement( connection, args );
    }

    @Override
    protected Object getSaveLock()
    {
        return PROJECT_SAVE_LOCK;
    }


    @Override
    public void save() throws SQLException
    {
        Connection connection = null;
        ResultSet results = null;
        PreparedStatement statement = null;

        try
        {
            connection = Database.getConnection();
            connection.setAutoCommit( false );

            Database.lockTable( connection, "wres.Project" );
            statement = this.getInsertSelectStatement( connection );
            results = statement.executeQuery();
            //results = Database.getResults( connection, this.getInsertSelectStatement() );

            this.setID( Database.getValue( results, this.getIDName() ) );
            this.performedInsert = Database.getValue( results, "wasInserted" );

            connection.commit();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Did I create Project ID {}? {}",
                              this.getId(),
                              this.performedInsert );
            }
        }
        catch (SQLException e)
        {
            if (connection != null)
            {
                try
                {
                    connection.rollback();
                }
                catch ( SQLException se )
                {
                    LOGGER.warn( "Failed to rollback.", se );
                }
            }

            throw e;
        }
        finally
        {
            if (results != null)
            {
                try
                {
                    results.close();
                }
                catch ( SQLException se )
                {
                    // Failure to close resource shouldn't affect primary output
                    LOGGER.warn( "Failed to close result set {}.", results, se );
                }
            }

            if (statement != null)
            {
                try
                {
                    statement.close();
                }
                catch (SQLException e)
                {
                    // Failure to close resource shouldn't affect primary output
                    LOGGER.warn( "Failed to close statement {}.", statement, e );
                }
            }

            if (connection != null)
            {
                connection.setAutoCommit( true );
                Database.returnConnection( connection );
            }
        }
    }

    @Override
    protected Logger getLogger()
    {
        return ProjectDetails.LOGGER;
    }

    @Override
    public String toString()
    {
        return "Project { Name: " + this.getProjectName() +
               ", Code: " + this.getInputCode() + " }";
    }

    public boolean performedInsert()
    {
        return this.performedInsert;
    }

    /**
     * @param feature The feature to investigate
     * @return The last lead time for the feature available for evaluation
     * @throws SQLException Thrown if the value could not be retrieved from the
     * database
     */
    public Integer getLastLead(Feature feature) throws SQLException
    {
        boolean leadIsMissing = !this.lastLeads.containsKey( feature );

        if (leadIsMissing)
        {
            String script = "";

            if ( ConfigHelper.isForecast( this.getRight() ) )
            {
                script += "SELECT MAX(FV.lead) AS last_lead" + NEWLINE;
                script += "FROM wres.TimeSeries TS" + NEWLINE;
                script += "INNER JOIN wres.ForecastValue FV" + NEWLINE;
                script += "    ON TS.timeseries_id = FV.timeseries_id" + NEWLINE;
                script += "WHERE " +
                          ConfigHelper.getVariablePositionClause( feature,
                                                                  this.getRightVariableID(),
                                                                  "TS" ) +
                          NEWLINE;

                if ( this.getMaximumLeadHour() != Integer.MAX_VALUE )
                {
                    script += "    AND FV.lead <= "
                              + this.getMaximumLeadHour( )
                              + NEWLINE;
                }

                if ( this.getMinimumLeadHour() != Integer.MIN_VALUE )
                {
                    script += "    AND FV.lead >= "
                              + this.getMinimumLeadHour( )
                              + NEWLINE;
                }

                if ( Strings.hasValue( this.getEarliestIssueDate()))
                {
                    script += "    AND TS.initialization_date >= '" + this.getEarliestIssueDate() + "'" + NEWLINE;
                }

                if (Strings.hasValue( this.getLatestIssueDate()))
                {
                    script += "    AND TS.initialization_date <= '" + this.getLatestIssueDate() + "'" + NEWLINE;
                }

                if ( Strings.hasValue( this.getEarliestDate() ))
                {
                    script += "    AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead >= '" + this.getEarliestDate() + "'" + NEWLINE;
                }

                if (Strings.hasValue( this.getLatestDate() ))
                {
                    script += "    AND TS.initialization_date + INTERVAL '1 HOUR' * FV.lead <= '" + this.getLatestDate() + "'" + NEWLINE;
                }

                script += "    AND EXISTS (" + NEWLINE;
                script += "        SELECT 1" + NEWLINE;
                script += "        FROM wres.ProjectSource PS" + NEWLINE;
                script += "        INNER JOIN wres.ForecastSource FS" + NEWLINE;
                script += "            ON FS.source_id = PS.source_id" + NEWLINE;
                script += "        WHERE PS.project_id = " + this.getId() + NEWLINE;
                script += "            AND PS.inactive_time IS NULL" + NEWLINE;
                script += "            AND FS.forecast_id = TS.timeseries_id" + NEWLINE;
                script += "    )";
            }
            else
            {
                script += "SELECT COUNT(*)::int AS last_lead" + NEWLINE;
                script += "FROM wres.Observation O" + NEWLINE;
                script += "INNER JOIN wres.ProjectSource PS" + NEWLINE;
                script += "     ON PS.source_id = O.source_id" + NEWLINE;
                script += "WHERE PS.project_id = " + this.getId() + NEWLINE;
                script += "     AND " +
                          ConfigHelper.getVariablePositionClause(
                                  feature,
                                  this.getRightVariableID(),
                                  "O" );
                script += NEWLINE;
            }
            script += ";";


            this.lastLeads.put(feature, Database.getResult( script, "last_lead" ));
        }

        return this.lastLeads.get(feature);
    }


    /**
     * @return the minimum value specified or a default of Integer.MIN_VALUE
     */
    public int getMinimumLeadHour()
    {
        int result = Integer.MIN_VALUE;

        if ( this.getProjectConfig().getPair() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours()
                    .getMinimum() != null )
        {
            result = this.getProjectConfig().getPair()
                         .getLeadHours()
                         .getMinimum();
        }

        return result;
    }

    /**
     * @return the maximum value specified or a default of Integer.MAX_VALUE
     */
    public int getMaximumLeadHour()
    {
        int result = Integer.MAX_VALUE;

        if ( this.getProjectConfig().getPair() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours()
                    .getMaximum() != null )
        {
            result = this.getProjectConfig().getPair()
                         .getLeadHours()
                         .getMaximum();
        }

        return result;
    }

    public int getMaximumLeadHourForFeature(Feature feature)
    {
        if (this.getMaximumLeadHour() < Integer.MAX_VALUE)
        {
            return this.getMaximumLeadHour();
        }

        return 0;
    }

    /**
     * @return The overall number of lead units within a single window
     * @throws NoDataException Thrown if there wasn't enough data available to
     * evaluate
     * @throws SQLException when communication with the database failed
     */
    public long getWindowWidth() throws NoDataException, SQLException
    {
        return TimeHelper.unitsToLeadUnits( this.getLeadUnit(), this.getLeadPeriod() );
    }

    /**
     * Returns the first date of observation data for the feature, for the given input
     * @param sourceConfig The side of the data whose zero date we are interested in
     * @param feature The feature whose zero date we are interested in
     * @return The first date where a feature contains an observed value for a
     * certain configuration
     * @throws SQLException Thrown if the initial date could not be loaded
     * from the database.
     */
    public String getInitialObservationDate( DataSourceConfig sourceConfig, Feature feature) throws SQLException
    {
        synchronized ( this.initialObservationDates )
        {
            if (!this.initialObservationDates.containsKey( feature ))
            {
                String script =
                        ScriptGenerator.generateInitialObservationDateScript( this,
                                                                              sourceConfig,
                                                                              feature );
                this.initialObservationDates.put(
                        feature,
                        Database.getResult( script, "zero_date" )
                );
            }
        }

        return this.initialObservationDates.get( feature);
    }

    public String getInitialForecastDate( DataSourceConfig sourceConfig, Feature feature) throws SQLException
    {
        synchronized ( this.initialForecastDates )
        {
            if (!this.initialForecastDates.containsKey( feature ))
            {
                String script = ScriptGenerator.generateInitialForecastDateScript(
                        this,
                        sourceConfig,
                        feature
                );

                this.initialForecastDates.put(
                        feature,
                        Database.getResult( script, "zero_date" )
                );
            }

            return this.initialForecastDates.get(feature);
        }
    }

    /**
     * Gets and stores the largest leap between two different forecasts in
     * forecast order, measured in the standard lead hour unit
     * <p>
     *     If we have forecasts such as:
     * </p>
     * <table>
     *     <caption>Forecast Dates</caption>
     *     <tr>
     *         <td>1985-01-01 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-02 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-03 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-05 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-06 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-09 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-10 12:00:00</td>
     *     </tr>
     * </table>
     * <p>
     *     We'll find that the greatest gap between two forecasts is three days.
     *     When we go to select data spanning across initialization dates, the
     *     smallest and safest distance is this maximum. If a smaller span is
     *     chosen, you run the risk of generating windows with no data.
     * </p>
     * @param sourceConfig The datasource configuration that dictates what
     *                     type of data to use
     * @param feature The location to find the lag for
     * @return The number of lead hours between forecasts for a feature
     * @throws SQLException Thrown if the ID for the variable could not be evaluated
     * @throws SQLException Thrown if the lag could not be computed in the database
     */
    public Integer getForecastLag(DataSourceConfig sourceConfig, Feature feature) throws SQLException
    {
        synchronized (this.forecastLag)
        {
            if (!this.forecastLag.containsKey( feature ))
            {
                // This script will tell us the maximum distance between
                // sequential forecasts for a feature for this project.
                // We don't need the intended distance. If we go with the
                // intended distance (say 3 hours), but the user just doesn't
                // have or chooses not to use a forecast, resulting in a gap of
                // 6 hours, we'll encounter an error because we're aren't
                // accounting for that weird gap. By going with the maximum, we
                // ensure that we will always cover that gap.
                ScriptBuilder script = new ScriptBuilder();
                script.addLine("WITH initialization_lag AS");
                script.addLine("(");
                script.addTab().addLine("SELECT (");
                script.addTab(  2  ).addLine("EXTRACT (");
                script.addTab(   3   ).addLine( "epoch FROM AGE (");
                script.addTab(    4    ).addLine( "TS.initialization_date,");
                script.addTab(    4    ).addLine( "(");
                script.addTab(     5     ).addLine("LAG(TS.initialization_date) OVER (ORDER BY TS.initialization_date)");
                script.addTab(    4    ).addLine( ")");
                script.addTab(   3   ).addLine(")");
                script.addTab(  2  ).addLine(") / 3600)::int AS lag");
                script.addTab().addLine("FROM wres.TimeSeries TS");
                script.addTab().add("WHERE ");
                script.addLine(ConfigHelper.getVariablePositionClause(
                        feature,
                        Variables.getVariableID( sourceConfig ),
                        "TS" )
                );
                script.addTab(  2  ).addLine("AND EXISTS (");
                script.addTab(   3   ).addLine("SELECT 1");
                script.addTab(   3   ).addLine("FROM wres.ForecastSource FS");
                script.addTab(   3   ).addLine("INNER JOIN wres.ProjectSource PS");
                script.addTab(    4    ).addLine("ON PS.source_id = FS.source_id");
                script.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getId());
                script.addTab(    4    ).addLine("AND PS.member = ", this.getInputName( sourceConfig ));
                script.addTab(    4    ).addLine("AND FS.forecast_id = TS.timeseries_id");
                script.addTab(  2  ).addLine(")");
                script.addTab().addLine("GROUP BY TS.initialization_date");
                script.addTab().addLine("ORDER BY TS.initialization_date");
                script.addLine(")");
                script.addLine("SELECT max(IL.lag) AS typical_gap");
                script.addLine("-- We take the max to ensure that values are contained within;");
                script.addLine("--   If we try to take the mode, we risk being too granular");
                script.addLine("--   and trying to select values that aren't there.");
                script.addLine("FROM initialization_lag IL");
                script.addLine("WHERE IL.lag IS NOT NULL;");

                this.forecastLag.put( feature, script.retrieve( "typical_gap" ) );
            }


            return this.forecastLag.get( feature );
        }
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

    @Override
    public int compareTo(ProjectDetails other)
    {
        return this.getInputCode().compareTo( other.getInputCode() );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null &&
               obj instanceof ProjectDetails &&
               this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return this.getInputCode();
    }

}

