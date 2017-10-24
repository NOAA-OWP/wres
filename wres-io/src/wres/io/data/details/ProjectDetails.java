package wres.io.data.details;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.SQLExecutor;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.Internal;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * Wrapper object linking a project configuration and the data needed to form
 * database statements
 */
@Internal(exclusivePackage = "wres.io")
public class ProjectDetails extends CachedDetail<ProjectDetails, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectDetails.class );

    private Integer projectID = null;
    private final ProjectConfig projectConfig;

    private final List<Integer> leftForecastIDs = new ArrayList<>(  );
    private final List<Integer> rightForecastIDs = new ArrayList<>(  );
    private final List<Integer> baselineForecastIDs = new ArrayList<>(  );

    private final Map<Integer, String> leftHashes = new ConcurrentHashMap<>(  );
    private final Map<Integer, String> rightHashes = new ConcurrentHashMap<>(  );
    private final Map<Integer, String> baselineHashes = new ConcurrentHashMap<>(  );

    private final List<Integer> leftSources = new ArrayList<>(  );
    private final List<Integer> rightSources = new ArrayList<>(  );
    private final List<Integer> baselineSources = new ArrayList<>(  );

    private Integer leftVariableID = null;
    private Integer rightVariableID = null;
    private Integer baselineVariableID = null;

    private Integer lastLead = null;

    public static final String LEFT_MEMBER = "'left'";
    public static final String RIGHT_MEMBER = "'right'";
    public static final String BASELINE_MEMBER = "'baseline'";

    private final int inputCode;

    private final Object LOAD_LOCK = new Object();

    public static Integer hash(ProjectConfig projectConfig)
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

        for ( DataSourceConfig.Source source : left.getSource())
        {
            if (source.getFormat() != null)
            {
                hashBuilder.append( source.getFormat().value() );
            }

            hashBuilder.append(source.getValue());
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

        for ( DataSourceConfig.Source source : right.getSource())
        {
            if (source.getFormat() != null)
            {
                hashBuilder.append( source.getFormat().value() );
            }

            hashBuilder.append(source.getValue());
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

            for ( DataSourceConfig.Source source : baseline.getSource())
            {
                if (source.getFormat() != null)
                {
                    hashBuilder.append( source.getFormat().value() );
                }

                hashBuilder.append(source.getValue());
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

    public ProjectDetails(ProjectConfig projectConfig)
    {
        this.projectConfig = projectConfig;
        this.inputCode = ProjectDetails.hash( this.projectConfig );
    }

    @Override
    public Integer getKey()
    {
        return this.getInputCode();
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

        this.addSource( sourceID, member );

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

        this.addSource( sourceID, member );

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
            return Collections.exists( sources,
                                       hash -> hash.equals( foundHash ));
        }
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
        if (this.lastLead == null)
        {
            String script = "";

            if ( ConfigHelper.isForecast( this.getRight() ) )
            {
                script += "SELECT FV.lead AS last_lead" + NEWLINE;
                script += "FROM wres.ForecastValue FV" + NEWLINE;
                script += "INNER JOIN wres.TimeSeries TS" + NEWLINE;
                script +=
                        "    ON TS.timeseries_id = FV.timeseries_id" + NEWLINE;
                script += "INNER JOIN wres.ForecastSource FS" + NEWLINE;
                script += "    ON FS.forecast_id = TS.timeseries_id" + NEWLINE;
                script += "INNER JOIN wres.ProjectSource PS" + NEWLINE;
                script += "   ON PS.source_id = FS.source_id" + NEWLINE;
                script += "WHERE PS.project_id = " + this.getId() + NEWLINE;
                script += "    AND  " +
                          ConfigHelper.getVariablePositionClause( feature,
                                                                  this.getRightVariableID(),
                                                                  "TS" ) +
                          NEWLINE;

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

                script += "GROUP BY FV.lead" + NEWLINE;
                script += "ORDER BY last_lead DESC" + NEWLINE;
                script += "LIMIT 1";
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


            this.lastLead = Database.getResult( script, "last_lead" );
        }

        return this.lastLead;
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
