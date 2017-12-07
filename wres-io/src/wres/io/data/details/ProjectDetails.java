package wres.io.data.details;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeAggregationConfig;
import wres.io.concurrency.SQLExecutor;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptGenerator;
import wres.util.Internal;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.Time;

/**
 * Wrapper object linking a project configuration and the data needed to form
 * database statements
 */
@Internal(exclusivePackage = "wres.io")
public class ProjectDetails extends CachedDetail<ProjectDetails, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectDetails.class );

    public static final String LEFT_MEMBER = "'left'";
    public static final String RIGHT_MEMBER = "'right'";
    public static final String BASELINE_MEMBER = "'baseline'";

    private Integer projectID = null;
    private final ProjectConfig projectConfig;

    private final List<Integer> leftForecastIDs = new ArrayList<>(  );
    private final List<Integer> rightForecastIDs = new ArrayList<>(  );
    private final List<Integer> baselineForecastIDs = new ArrayList<>(  );

    private final Map<Integer, String> leftHashes = new ConcurrentHashMap<>(  );
    private final Map<Integer, String> rightHashes = new ConcurrentHashMap<>(  );
    private final Map<Integer, String> baselineHashes = new ConcurrentHashMap<>(  );
    private final Map<Feature, Integer> lastLeads = new ConcurrentHashMap<>(  );
    private final Map<Feature, Integer> leadOffsets = new ConcurrentHashMap<>(  );

    private final List<Integer> leftSources = new ArrayList<>(  );
    private final List<Integer> rightSources = new ArrayList<>(  );
    private final List<Integer> baselineSources = new ArrayList<>(  );

    private Set<FeatureDetails> features;

    private Integer leftVariableID = null;
    private Integer rightVariableID = null;
    private Integer baselineVariableID = null;

    private final int inputCode;

    private final Object LOAD_LOCK = new Object();

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
                ProjectDetails.copyAndSort( leftHashesIngested );

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
                ProjectDetails.copyAndSort( rightHashesIngested );

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
                    ProjectDetails.copyAndSort( baselineHashesIngested );

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

    public Set<FeatureDetails> getFeatures() throws SQLException
    {
        if (this.features == null)
        {
            this.features = Features.getAllDetails(this.projectConfig);
        }
        return this.features;
    }

    public DataSourceConfig getLeft()
    {
        return this.projectConfig.getInputs().getLeft();
    }

    public DataSourceConfig getRight()
    {
        return this.projectConfig.getInputs().getRight();
    }

    public DataSourceConfig getBaseline()
    {
        return this.projectConfig.getInputs().getBaseline();
    }

    public Integer getLeftVariableID() throws SQLException
    {
        if (this.leftVariableID == null)
        {
            this.leftVariableID = Variables.getVariableID(this.getLeftVariableName(),
                                                          this.getLeftVariableUnit());
        }

        return this.leftVariableID;
    }

    public String getLeftVariableName()
    {
        return this.getLeft().getVariable().getValue();
    }

    public String getLeftVariableUnit()
    {
        return this.getLeft().getVariable().getUnit();
    }

    public Integer getRightVariableID() throws SQLException
    {
        if (this.rightVariableID == null)
        {
            this.rightVariableID = Variables.getVariableID( this.getRightVariableName(),
                                                            this.getRightVariableUnit());
        }

        return this.rightVariableID;
    }

    public String getRightVariableName()
    {
        return this.getRight().getVariable().getValue();
    }

    public String getRightVariableUnit()
    {
        return this.getRight().getVariable().getUnit();
    }

    public Integer getBaselineVariableID() throws SQLException
    {
        if (this.hasBaseline() && this.baselineVariableID == null)
        {
            this.baselineVariableID = Variables.getVariableID( this.getBaselineVariableName(),
                                                               this.getBaselineVariableUnit() );
        }

        return this.baselineVariableID;
    }

    public String getBaselineVariableName()
    {
        String name = null;
        if (this.hasBaseline())
        {
            name = this.getBaseline().getVariable().getValue();
        }
        return name;
    }

    public String getBaselineVariableUnit()
    {
        String unit = null;
        if (this.hasBaseline())
        {
            unit = this.getBaseline().getVariable().getUnit();
        }
        return unit;
    }

    public int getLeftTimeShift()
    {
        int shift = 0;

        if (this.getLeft().getTimeShift() != null)
        {
            shift = this.getLeft().getTimeShift().getWidth();
        }

        return shift;
    }

    public int getRightTimeShift()
    {
        int shift = 0;

        if (this.getRight().getTimeShift() != null)
        {
            shift = this.getRight().getTimeShift().getWidth();
        }

        return shift;
    }

    public int getBaselineTimeShift()
    {
        int shift = 0;

        if (this.getBaseline().getTimeShift() != null)
        {
            shift = this.getBaseline().getTimeShift().getWidth();
        }

        return shift;
    }

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

    public boolean specifiesSeason()
    {
        return this.projectConfig.getPair().getSeason() != null;
    }

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

    public Integer getAggregationPeriod()
    {
        Integer period = null;

        if (this.projectConfig.getPair().getDesiredTimeAggregation() != null)
        {
            period = this.projectConfig.getPair().getDesiredTimeAggregation().getPeriod();
        }

        return period;
    }

    public String getAggregationUnit()
    {
        String unit = null;

        if (this.projectConfig.getPair().getDesiredTimeAggregation() != null)
        {
            unit = this.projectConfig.getPair()
                                     .getDesiredTimeAggregation()
                                     .getUnit()
                                     .value();
        }

        return unit;
    }

    public String getAggregationFunction()
    {
        String function = null;

        if (this.projectConfig.getPair().getDesiredTimeAggregation() != null)
        {
            function = this.projectConfig.getPair()
                                         .getDesiredTimeAggregation()
                                         .getFunction()
                                         .value();
        }

        return function;
    }

    public String getDesiredMeasurementUnit()
    {
        return String.valueOf(this.projectConfig.getPair().getUnit());
    }

    public List<DestinationConfig> getPairDestinations()
    {
        return ConfigHelper.getPairDestinations( this.projectConfig );
    }

    public Integer getLeadOffset(Feature feature)
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        boolean offsetIsMissing = !this.leadOffsets.containsKey( feature );

        if ( offsetIsMissing && ConfigHelper.isSimulation( this.getRight() ))
        {
            this.leadOffsets.put(feature, 0);
        }
        else if (offsetIsMissing)
        {
            Integer leadOffset = ConfigHelper.getLeadOffset( this,
                                                             feature);

            if (leadOffset == null)
            {
                String message = "A valid offset for matching lead values could not be determined. ";
                message += "The first acceptable lead time is ";
                message += String.valueOf(
                        ConfigHelper.getMinimumLeadHour( this.projectConfig )
                );

                if ( ConfigHelper.isMaximumLeadHourSpecified( this.projectConfig ) )
                {
                    message += ", the last acceptable lead time is ";
                    message += String.valueOf(
                            ConfigHelper.getMaximumLeadHour( this.projectConfig )
                    );
                    message += ",";
                }

                message += " and the size of each evaluation window is ";
                message += String.valueOf(this.getAggregationPeriod());
                message += " ";
                message += String.valueOf(this.getAggregationPeriod());
                message += "s. ";

                message += "A full evaluation window could not be found ";
                message += "between the left hand data and the right ";
                message += "data that fits within these parameters.";

                throw new NoDataException( message );
            }
            else
            {
                this.leadOffsets.put( feature, leadOffset );
            }
        }

        return this.leadOffsets.get( feature );
    }

    public String getProjectName()
    {
        return this.projectConfig.getName();
    }

    public void addSource( String hash, DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        if (!Strings.hasValue( hash ))
        {
            throw new IllegalArgumentException( "Attempting to save " +
                                                "a non-existent set of " +
                                                "data to a project is " +
                                                "not a valid operation." );
        }

        String member;

        if ( ConfigHelper.isLeft( dataSourceConfig, projectConfig ))
        {
            member = ProjectDetails.LEFT_MEMBER;
        }
        else if ( ConfigHelper.isRight( dataSourceConfig, projectConfig ))
        {
            member = ProjectDetails.RIGHT_MEMBER;
        }
        else
        {
            member = ProjectDetails.BASELINE_MEMBER;
        }

        if (ConfigHelper.isForecast( dataSourceConfig ))
        {
            this.addForecastSource( hash, member );
        }
        else
        {
            this.addObservationSource( hash, member );
        }
    }

    // TODO: Convert to database function
    private void addSource(Integer sourceID, String member)
    {
        if (member == null ||
            !(member.equalsIgnoreCase( ProjectDetails.LEFT_MEMBER ) ||
              member.equalsIgnoreCase( ProjectDetails.RIGHT_MEMBER ) ||
              member.equalsIgnoreCase( ProjectDetails.BASELINE_MEMBER )))
        {
            throw new IllegalArgumentException( "The member " +
                                                member +
                                                " is not a valid member for source data." );
        }

        if (sourceID == null)
        {
            throw new IllegalArgumentException( "Attempting to add a " +
                                                "non-existant source to a " +
                                                "project is not a valid operation." );
        }

        String script =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member)"
                + NEWLINE +
                "SELECT " + this.projectID + ", " +
                sourceID + ", " +
                member +
                NEWLINE +
                "WHERE NOT EXISTS (" + NEWLINE +
                "     SELECT 1" + NEWLINE +
                "     FROM wres.ProjectSource PS" + NEWLINE +
                "     WHERE project_id = " + this.projectID + NEWLINE +
                "         AND source_id = " + sourceID + NEWLINE +
                "         AND member = " + member + NEWLINE +
                ");";

        SQLExecutor executor = new SQLExecutor( script );
        executor.setOnRun( ProgressMonitor.onThreadStartHandler() );
        executor.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
        Database.execute( executor );
    }

    public void addObservationSource(String hash, String member) throws SQLException
    {
        Integer sourceID = DataSources.getActiveSourceID( hash );

        if (sourceID == null)
        {
            LOGGER.warn( "Source data could not be attached to '{}' " +
                         "as {} data because no data was ever ingested for it.",
                         this.getProjectName(),
                         member);
            return;
        }

        //this.addSource( sourceID, member );

        if (member.equalsIgnoreCase( ProjectDetails.LEFT_MEMBER ))
        {
            this.getLeftHashes().putIfAbsent( sourceID, hash );
            this.getLeftSources().add(sourceID);
        }
        else if (member.equalsIgnoreCase( ProjectDetails.RIGHT_MEMBER ))
        {
            this.getRightHashes().putIfAbsent( sourceID, hash );
            this.getRightSources().add( sourceID );
        }
        else
        {
            this.getBaselineHashes().putIfAbsent( sourceID, hash );
            this.getBaselineSources().add( sourceID );
        }
    }

    public void addForecastSource(String hash, String member) throws SQLException
    {
        Integer sourceID = DataSources.getActiveSourceID( hash );

        if (sourceID == null)
        {
            LOGGER.warn( "Source data could not be attached to '{}' " +
                         "as {} data because no data was ever ingested for it.",
                         this.getProjectName(),
                         member);
            return;
        }

        //this.addSource( sourceID, member );

        String script = "SELECT FS.forecast_id" + NEWLINE +
                        "FROM wres.ForecastSource FS" + NEWLINE +
                        "WHERE FS.source_id = " + sourceID + ";";

        Integer forecastID = Database.getResult( script, "forecast_id");

        if (member.equalsIgnoreCase( ProjectDetails.LEFT_MEMBER ))
        {
            this.getLeftForecastIDs().add( forecastID );
            this.getLeftSources().add(sourceID);
            this.getLeftHashes().putIfAbsent( sourceID, hash );
        }
        else if (member.equalsIgnoreCase( ProjectDetails.RIGHT_MEMBER ))
        {
            this.getRightForecastIDs().add( forecastID );
            this.getRightSources().add( sourceID );
            this.getRightHashes().putIfAbsent( sourceID, hash );
        }
        else
        {
            this.getBaselineForecastIDs().add( forecastID );
            this.getBaselineSources().add( sourceID );
            this.getBaselineHashes().putIfAbsent( sourceID, hash );
        }
    }

    /**
     * Returns unique identifier for this project's config+data
     * @return The unique ID
     */
    private Integer getInputCode()
    {
        return this.inputCode;
    }

    public List<Integer> getLeftForecastIDs() throws SQLException
    {
        if (this.leftForecastIDs.size() == 0)
        {
            this.loadForecastIDs( ProjectDetails.LEFT_MEMBER );
        }
        return this.leftForecastIDs;
    }

    public List<Integer> getRightForecastIDs() throws SQLException
    {
        if (this.rightForecastIDs.size() == 0)
        {
            this.loadForecastIDs( ProjectDetails.RIGHT_MEMBER );
        }
        return this.rightForecastIDs;
    }

    public List<Integer> getBaselineForecastIDs() throws SQLException
    {
        if (this.hasBaseline() && this.baselineForecastIDs.size() == 0)
        {
            this.loadForecastIDs( ProjectDetails.BASELINE_MEMBER );
        }
        return this.baselineForecastIDs;
    }

    private void loadForecastIDs(String member) throws SQLException
    {

        Integer memberVariableID;
        List<Integer> forecastIDs;

        if (member.equals( ProjectDetails.LEFT_MEMBER ))
        {
            memberVariableID = this.getLeftVariableID();
            forecastIDs = this.leftForecastIDs;
        }
        else if (member.equals( ProjectDetails.RIGHT_MEMBER ))
        {
            memberVariableID = this.getRightVariableID();
            forecastIDs = this.rightForecastIDs;
        }
        else
        {
            memberVariableID = this.getBaselineVariableID();
            forecastIDs = this.baselineForecastIDs;
        }

        StringBuilder script = new StringBuilder(  );
        script.append( "SELECT TS.timeseries_id" ).append(NEWLINE);
        script.append( "FROM wres.ForecastSource FS").append(NEWLINE);
        script.append( "INNER JOIN wres.ProjectSource PS").append(NEWLINE);
        script.append( "    ON PS.source_id = FS.source_id").append(NEWLINE);
        script.append( "INNER JOIN wres.TimeSeries TS").append(NEWLINE);
        script.append( "    ON TS.timeseries_id = FS.forecast_id").append(NEWLINE);
        script.append( "INNER JOIN wres.VariablePosition VP").append(NEWLINE);
        script.append( "    ON VP.variableposition_id = TS.variableposition_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(member).append(NEWLINE);
        script.append("     AND PS.inactive_time IS NULL").append(NEWLINE);
        script.append("     AND VP.variable_id = ").append(memberVariableID).append(";");

        Database.populateCollection(forecastIDs,
                                    script.toString(),
                                    "timeseries_id");
    }

    public List<Integer> getLeftSources() throws SQLException
    {
        if (this.leftSources.size() == 0)
        {
            this.loadSources( ProjectDetails.LEFT_MEMBER );
        }
        return this.leftSources;
    }

    public Map<Integer, String> getLeftHashes() throws SQLException
    {
        if (this.leftHashes.size() == 0)
        {
            this.loadSources( ProjectDetails.LEFT_MEMBER );
        }
        return this.leftHashes;
    }

    public List<Integer> getRightSources() throws SQLException
    {
        if (this.rightSources.size() == 0)
        {
            this.loadSources( ProjectDetails.RIGHT_MEMBER );
        }
        return this.rightSources;
    }

    public Map<Integer, String> getRightHashes() throws SQLException
    {
        if (this.rightHashes.size() == 0)
        {
            this.loadSources( ProjectDetails.RIGHT_MEMBER );
        }
        return this.rightHashes;
    }

    public List<Integer> getBaselineSources() throws SQLException
    {
        if (this.hasBaseline() && this.baselineSources.size() == 0)
        {
            this.loadSources( ProjectDetails.BASELINE_MEMBER );
        }

        return this.baselineSources;
    }

    public Map<Integer, String> getBaselineHashes() throws SQLException
    {
        if (this.hasBaseline() && this.baselineHashes.size() == 0)
        {
            this.loadSources( ProjectDetails.BASELINE_MEMBER );
        }
        return this.baselineHashes;
    }

    private void loadSources(String member) throws SQLException
    {
        StringBuilder script = new StringBuilder(  );

        script.append( "SELECT PS.source_id, S.hash").append(NEWLINE);
        script.append( "FROM wres.ProjectSource PS").append(NEWLINE);
        script.append( "INNER JOIN wres.Source S").append(NEWLINE);
        script.append( "    ON S.source_id = PS.source_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(member).append(NEWLINE);
        script.append( "    AND PS.inactive_time IS NULL;").append(NEWLINE);

        Connection connection = null;
        ResultSet resultSet = null;

        Map<Integer, String> sourceHashes;
        List<Integer> sourceIDs;

        if (member.equals( ProjectDetails.LEFT_MEMBER ))
        {
            sourceHashes = this.leftHashes;
            sourceIDs = this.leftSources;
        }
        else if (member.equals( ProjectDetails.RIGHT_MEMBER ))
        {
            sourceHashes = this.rightHashes;
            sourceIDs = this.rightSources;
        }
        else
        {
            sourceHashes = this.baselineHashes;
            sourceIDs = this.baselineSources;
        }

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, script.toString( ) );

            while (resultSet.next())
            {
                sourceIDs.add( resultSet.getInt( "source_id" ) );
                sourceHashes.put(resultSet.getInt( "source_id" ),
                                    resultSet.getString( "hash" ));
            }
        }
        finally
        {
            if (resultSet != null)
            {
                resultSet.close();
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }
    }

    public boolean isEmpty()
    {
        int sourceTotal = 0;

        try
        {
            sourceTotal = this.getLeftSources().size() +
                          this.getRightSources().size() +
                          this.getBaselineSources().size();
        }
        catch ( SQLException e )
        {
            LOGGER.error( "Could not evaluate whether or not this project had data:" );
            LOGGER.error( Strings.getStackTrace( e ) );
        }
        return sourceTotal == 0;
    }

    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    public boolean hasSource(String foundHash, DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        Collection<String> sources;

        if ( ConfigHelper.isLeft( dataSourceConfig, projectConfig ))
        {
            sources = this.getLeftHashes().values();
        }
        else if ( ConfigHelper.isRight( dataSourceConfig, projectConfig ))
        {
            sources = this.getRightHashes().values();
        }
        else
        {
            sources = this.getBaselineHashes().values();
        }

        if (sources.size() == 0)
        {
            return false;
        }

        synchronized ( LOAD_LOCK )
        {
            return wres.util.Collections.exists( sources,
                                                 hash -> hash.equals( foundHash ));
        }
    }

    public boolean hasMatchingData(FeatureDetails feature)
    {
        boolean matchExists = false;

        StringBuilder script = new StringBuilder();

        return matchExists;
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
    public void setID(Integer id) {
        this.projectID = id;
    }

    @Override
    protected String getInsertSelectStatement() {
        String script = "WITH new_project AS" + NEWLINE +
                        "(" + NEWLINE +
                        "     INSERT INTO wres.project (project_name, input_code)"
                        + NEWLINE +
                        "     SELECT '" + this.getProjectName() + "', " + this.getInputCode() + NEWLINE;

        script +=
                        "     WHERE NOT EXISTS (" + NEWLINE +
                        "         SELECT 1" + NEWLINE +
                        "         FROM wres.Project P" + NEWLINE +
                        "         WHERE P.input_code = " + this.getInputCode() + NEWLINE;

        script +=
                        "     )" + NEWLINE +
                        "     RETURNING project_id" + NEWLINE +
                        ")" + NEWLINE +
                        "SELECT project_id" + NEWLINE +
                        "FROM new_project" + NEWLINE +
                        NEWLINE +
                        "UNION" + NEWLINE +
                        NEWLINE +
                        "SELECT project_id" + NEWLINE +
                        "FROM wres.Project P" + NEWLINE +
                        "WHERE P.input_code = " + this.getInputCode() + ";";

        return script;
    }

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
                script += "    AND EXISTS (" + NEWLINE;
                script += "        SELECT 1" + NEWLINE;
                script += "        FROM wres.ProjectSource PS" + NEWLINE;
                script += "        INNER JOIN wres.ForecastSource FS" + NEWLINE;
                script += "            ON FS.source_id = PS.source_id" + NEWLINE;
                script += "        WHERE PS.project_id = " + this.getId() + NEWLINE;
                script += "            AND PS.inactive_time IS NULL" + NEWLINE;
                script += "            AND FS.forecast_id = TS.timeseries_id" + NEWLINE;
                script += "    )";

                if ( ConfigHelper.isMaximumLeadHourSpecified( this.projectConfig ) )
                {
                    script += "    AND FV.lead <= "
                              + ConfigHelper.getMaximumLeadHour( this.projectConfig )
                              + NEWLINE;
                }

                if ( ConfigHelper.isMinimumLeadHourSpecified( this.projectConfig ) )
                {
                    script += "    AND FV.lead >= "
                              + ConfigHelper.getMinimumLeadHour( this.projectConfig )
                              + NEWLINE;
                }
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

    public Integer getLead(int windowNumber)
            throws InvalidPropertiesFormatException
    {
        TimeAggregationConfig
                timeAggregationConfig = ConfigHelper.getTimeAggregation( projectConfig );
        return Time.unitsToHours( timeAggregationConfig.getUnit().name(),
                                  1.0 * windowNumber * timeAggregationConfig.getPeriod()).intValue();
    }

    public Integer getMinimumLeadHour()
    {
        return ConfigHelper.getMinimumLeadHour( this.projectConfig );
    }

    public int getWindowWidth() throws InvalidPropertiesFormatException
    {
        return ConfigHelper.getWindowWidth( this.projectConfig ).intValue();
    }

    public String getZeroDate(DataSourceConfig sourceConfig, Feature feature) throws SQLException
    {
        String script = ScriptGenerator.generateZeroDateScript( this.projectConfig,
                                                                sourceConfig,
                                                                feature);
        return "'" + Database.getResult( script, "zero_date" ) + "'";
    }

    public boolean usesProbabilityThresholds()
    {
        return ConfigHelper.usesProbabilityThresholds( this.projectConfig );
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

    private static List<String> copyAndSort( List<String> someList )
    {
        List<String> result = new ArrayList<>( someList.size() );
        result.addAll( someList );
        result.sort( Comparator.naturalOrder() );
        return Collections.unmodifiableList( result );
    }

}
