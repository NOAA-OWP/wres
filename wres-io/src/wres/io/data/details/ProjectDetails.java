package wres.io.data.details;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
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
 * Represents details about a type of forecast (such as short range, long range, analysis and assimilation, etc)
 */
@Internal(exclusivePackage = "wres.io")
public class ProjectDetails extends CachedDetail<ProjectDetails, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectDetails.class );

    private String projectName = null;
    private Integer projectID = null;
    private ProjectConfig projectConfig = null;

    private final List<Integer> leftForecastIDs = new ArrayList<>(  );
    private final List<Integer> rightForecastIDs = new ArrayList<>(  );
    private final List<Integer> baselineForecastIDs = new ArrayList<>(  );

    private final Map<Integer, String> leftHashes = new TreeMap<>(  );
    private final Map<Integer, String> rightHashes = new TreeMap<>(  );
    private final Map<Integer, String> baselineHashes = new TreeMap<>(  );

    private final List<Integer> leftSources = new ArrayList<>(  );
    private final List<Integer> rightSources = new ArrayList<>(  );
    private final List<Integer> baselineSources = new ArrayList<>(  );

    private Integer leftVariableID = null;
    private String leftVariableName = null;
    private Integer rightVariableID = null;
    private String rightVariableName = null;
    private Integer baselineVariableID = null;
    private String baselineVariableName = null;

    public static final String LEFT_MEMBER = "'left'";
    public static final String RIGHT_MEMBER = "'right'";
    public static final String BASELINE_MEMBER = "'baseline'";

    private final Object LOAD_LOCK = new Object();

    @Override
    public String getKey() {
        return this.projectName;
    }

    public void load( ProjectConfig projectConfig ) throws SQLException
    {
        synchronized ( LOAD_LOCK )
        {
            boolean load = false;

            this.projectConfig = projectConfig;

            if (leftVariableWillChange())
            {
                load = true;
                this.leftVariableName = projectConfig.getInputs().getLeft().getVariable().getValue();
                this.leftVariableID = Variables.getVariableID( this.leftVariableName, "none" );
            }

            if (rightVariableWillChange())
            {
                load = true;
                this.rightVariableName = projectConfig.getInputs().getRight().getVariable().getValue();
                this.rightVariableID = Variables.getVariableID( this.rightVariableName, "none" );
            }

            // TODO: This might break if it was null before, but not anymore
            if ( projectConfig.getInputs().getBaseline() != null &&
                 this.baselineVariableWillChange())
            {
                load = true;
                this.baselineVariableName = projectConfig.getInputs().getBaseline().getVariable().getValue();
                this.baselineVariableID = Variables.getVariableID( this.baselineVariableName, "none" );
            }

            if (load)
            {
                this.loadSources();
                this.loadForecastIDs();
            }
        }
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
                         this.projectName,
                         member);
            return;
        }

        this.addSource( sourceID, member );

        if (member.equalsIgnoreCase( ProjectDetails.LEFT_MEMBER ))
        {
            this.leftHashes.putIfAbsent( sourceID, hash );
            this.leftSources.add(sourceID);
        }
        else if (member.equalsIgnoreCase( ProjectDetails.RIGHT_MEMBER ))
        {
            this.rightHashes.putIfAbsent( sourceID, hash );
            this.rightSources.add( sourceID );
        }
        else
        {
            this.baselineHashes.putIfAbsent( sourceID, hash );
            this.baselineSources.add( sourceID );
        }
    }

    public void addForecastSource(String hash, String member) throws SQLException
    {
        Integer sourceID = DataSources.getActiveSourceID( hash );

        if (sourceID == null)
        {
            LOGGER.warn( "Source data could not be attached to '{}' " +
                         "as {} data because no data was ever ingested for it.",
                         this.projectName,
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
            this.leftForecastIDs.add( forecastID );
            this.leftSources.add(sourceID);
            this.leftHashes.putIfAbsent( sourceID, hash );
        }
        else if (member.equalsIgnoreCase( ProjectDetails.RIGHT_MEMBER ))
        {
            this.rightForecastIDs.add( forecastID );
            this.rightSources.add( sourceID );
            this.rightHashes.putIfAbsent( sourceID, hash );
        }
        else
        {
            this.baselineForecastIDs.add( forecastID );
            this.baselineSources.add( sourceID );
            this.baselineHashes.putIfAbsent( sourceID, hash );
        }
    }

    public List<Integer> getLeftForecastIDs()
    {
        return this.leftForecastIDs;
    }

    public List<Integer> getRightForecastIDs()
    {
        return this.rightForecastIDs;
    }

    public List<Integer> getBaselineForecastIDs()
    {
        return this.baselineForecastIDs;
    }

    public List<Integer> getLeftSources()
    {
        return this.leftSources;
    }

    public List<Integer> getRightSources()
    {
        return this.rightSources;
    }

    public List<Integer> getBaselineSources()
    {
        return this.baselineSources;
    }

    public boolean isEmpty()
    {
        return (this.leftSources.size() + this.rightSources.size() + this.baselineSources.size()) == 0;
    }

    private boolean leftVariableWillChange()
    {
        String leftVariableName = this.projectConfig.getInputs()
                                                    .getLeft()
                                                    .getVariable()
                                                    .getValue();
        return leftVariableName != null && (
                    this.leftVariableName == null ||
                    !this.leftVariableName.equalsIgnoreCase( leftVariableName )
                );
    }

    private boolean rightVariableWillChange()
    {
        String rightVariableName = this.projectConfig.getInputs()
                                                     .getRight()
                                                     .getVariable()
                                                     .getValue();
        return rightVariableName != null && (
                    this.rightVariableName == null ||
                    !this.rightVariableName.equalsIgnoreCase( rightVariableName )
                );
    }

    private boolean baselineVariableWillChange()
    {
        DataSourceConfig baseline = this.projectConfig.getInputs().getBaseline();
        if (baseline == null)
        {
            return false;
        }

        String baselineVariableName = baseline.getVariable().getValue();

        return baselineVariableName != null && (
                    this.baselineVariableName == null ||
                    !this.baselineVariableName.equalsIgnoreCase( baselineVariableName )
                );
    }

    public boolean hasSource(String foundHash, DataSourceConfig dataSourceConfig)
    {
        Collection<String> sources;

        if ( ConfigHelper.isLeft( dataSourceConfig, projectConfig ))
        {
            sources = leftHashes.values();
        }
        else if ( ConfigHelper.isRight( dataSourceConfig, projectConfig ))
        {
            sources = rightHashes.values();
        }
        else
        {
            sources = baselineHashes.values();
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

    public void setProjectName( String projectName )
    {
        if ( this.projectName == null || !this.projectName.equalsIgnoreCase(
                projectName ))
        {
            this.projectName = projectName;
            this.projectID = null;
        }
    }

    @Override
    protected String getInsertSelectStatement() {
        String script = "WITH new_project AS" + NEWLINE +
                        "(" + NEWLINE +
                        "     INSERT INTO wres.project (project_name)"
                        + NEWLINE +
                        "     SELECT '" + this.projectName + "'" + NEWLINE;

        script +=
                        "     WHERE NOT EXISTS (" + NEWLINE +
                        "         SELECT 1" + NEWLINE +
                        "         FROM wres.Project P" + NEWLINE +
                        "         WHERE P.project_name = '" + this.projectName + "'" + NEWLINE;

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
                        "WHERE P.project_name = '" + this.projectName + "';";

        return script;
    }

    private void loadForecastIDs() throws SQLException
    {
        StringBuilder script = new StringBuilder(  );
        script.append( "SELECT FS.forecast_id" ).append(NEWLINE);
        script.append( "FROM wres.ForecastSource FS").append(NEWLINE);
        script.append( "INNER JOIN wres.ProjectSource PS").append(NEWLINE);
        script.append( "    ON PS.source_id = FS.source_id").append(NEWLINE);
        script.append( "INNER JOIN wres.ForecastEnsemble FE").append(NEWLINE);
        script.append( "    ON FE.forecast_id = FS.forecast_id").append(NEWLINE);
        script.append( "INNER JOIN wres.VariablePosition VP").append(NEWLINE);
        script.append( "    ON VP.variableposition_id = FE.variableposition_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(ProjectDetails.LEFT_MEMBER).append(NEWLINE);
        script.append("     AND PS.inactive_time IS NULL").append(NEWLINE);
        script.append("     AND VP.variable_id = ").append(this.leftVariableID).append(";");

        Database.populateCollection(this.leftForecastIDs, Integer.class, script.toString(), "forecast_id");

        script = new StringBuilder(  );
        script.append( "SELECT FS.forecast_id" ).append(NEWLINE);
        script.append( "FROM wres.ForecastSource FS").append(NEWLINE);
        script.append( "INNER JOIN wres.ProjectSource PS").append(NEWLINE);
        script.append( "    ON PS.source_id = FS.source_id").append(NEWLINE);
        script.append( "INNER JOIN wres.ForecastEnsemble FE").append(NEWLINE);
        script.append( "    ON FE.forecast_id = FS.forecast_id").append(NEWLINE);
        script.append( "INNER JOIN wres.VariablePosition VP").append(NEWLINE);
        script.append( "    ON VP.variableposition_id = FE.variableposition_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(ProjectDetails.RIGHT_MEMBER).append(NEWLINE);
        script.append("     AND PS.inactive_time IS NULL").append(NEWLINE);
        script.append("     AND VP.variable_id = ").append(this.rightVariableID).append(";");

        Database.populateCollection(this.rightForecastIDs, Integer.class, script.toString(), "forecast_id");

        if (this.baselineVariableID == null)
        {
            return;
        }

        script = new StringBuilder();
        script.append( "SELECT FS.forecast_id" ).append( NEWLINE );
        script.append( "FROM wres.ForecastSource FS" ).append( NEWLINE );
        script.append( "INNER JOIN wres.ProjectSource PS" )
              .append( NEWLINE );
        script.append( "    ON PS.source_id = FS.source_id" )
              .append( NEWLINE );
        script.append( "INNER JOIN wres.ForecastEnsemble FE" )
              .append( NEWLINE );
        script.append( "    ON FE.forecast_id = FS.forecast_id" )
              .append( NEWLINE );
        script.append( "INNER JOIN wres.VariablePosition VP" )
              .append( NEWLINE );
        script.append(
                "    ON VP.variableposition_id = FE.variableposition_id" )
              .append( NEWLINE );
        script.append( "WHERE PS.project_id = " )
              .append( this.projectID )
              .append( NEWLINE );
        script.append( "    AND PS.member = " )
              .append( ProjectDetails.RIGHT_MEMBER )
              .append( NEWLINE );
        script.append( "     AND PS.inactive_time IS NULL" )
              .append( NEWLINE );
        script.append( "     AND VP.variable_id = " )
              .append( this.baselineVariableID )
              .append( ";" );

        Database.populateCollection( this.baselineForecastIDs,
                                     Integer.class,
                                     script.toString(),
                                     "forecast_id" );
    }

    private void loadSources()
    {
        StringBuilder script = new StringBuilder();

        script.append( "SELECT PS.source_id, S.hash").append(NEWLINE);
        script.append( "FROM wres.ProjectSource PS").append(NEWLINE);
        script.append( "INNER JOIN wres.Source S").append(NEWLINE);
        script.append( "    ON S.source_id = PS.source_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(ProjectDetails.LEFT_MEMBER).append(NEWLINE);
        script.append( "    AND PS.inactive_time IS NULL").append(NEWLINE);

        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, script.toString() );

            while (resultSet.next())
            {
                this.leftSources.add( resultSet.getInt( "source_id" ) );
                this.leftHashes.put(resultSet.getInt( "source_id" ),
                                    resultSet.getString("hash"));
            }
        }
        catch ( SQLException e )
        {
            LOGGER.error( Strings.getStackTrace(e) );
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
                    LOGGER.error( Strings.getStackTrace( e ));
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }

        script = new StringBuilder();

        script.append( "SELECT PS.source_id, S.hash").append(NEWLINE);
        script.append( "FROM wres.ProjectSource PS").append(NEWLINE);
        script.append( "INNER JOIN wres.Source S").append(NEWLINE);
        script.append( "    ON S.source_id = PS.source_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(ProjectDetails.RIGHT_MEMBER).append(NEWLINE);
        script.append( "    AND PS.inactive_time IS NULL").append(NEWLINE);

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, script.toString() );

            while (resultSet.next())
            {
                this.rightSources.add( resultSet.getInt( "source_id" ) );
                this.rightHashes.put(resultSet.getInt( "source_id" ),
                                     resultSet.getString("hash"));
            }
        }
        catch ( SQLException e )
        {
            LOGGER.error( Strings.getStackTrace(e) );
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
                    LOGGER.error( Strings.getStackTrace( e ));
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }

        if (this.baselineVariableID == null)
        {
            return;
        }

        script = new StringBuilder();

        script.append( "SELECT PS.source_id, S.hash").append(NEWLINE);
        script.append( "FROM wres.ProjectSource PS").append(NEWLINE);
        script.append( "INNER JOIN wres.Source S").append(NEWLINE);
        script.append( "    ON S.source_id = PS.source_id").append(NEWLINE);
        script.append( "WHERE PS.project_id = ").append(this.projectID).append(NEWLINE);
        script.append( "    AND PS.member = ").append(ProjectDetails.BASELINE_MEMBER).append(NEWLINE);
        script.append( "    AND PS.inactive_time IS NULL").append(NEWLINE);

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, script.toString() );

            while (resultSet.next())
            {
                this.baselineSources.add( resultSet.getInt( "source_id" ) );
                this.baselineHashes.put(resultSet.getInt( "source_id" ),
                                        resultSet.getString("hash"));
            }
        }
        catch ( SQLException e )
        {
            LOGGER.error( Strings.getStackTrace(e) );
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
                    LOGGER.error( Strings.getStackTrace( e ));
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }
    }

    @Override
    public int compareTo(ProjectDetails other) {
        int equality;

        if (other == null)
        {
            equality = 1;
        }
        else
        {
            if (this.projectID == null && other.projectID == null)
            {
                equality = 0;
            }
            else if (this.projectID == null && other.projectID != null)
            {
                equality = -1;
            }
            else if (this.projectID != null && other.projectID == null)
            {
                equality = 1;
            }
            else
            {
                return this.projectID.compareTo( other.projectID );
            }

            if (equality == 0)
            {
                if ( this.projectName == null && other.projectName == null )
                {
                    equality = 0;
                }
                else if ( this.projectName == null
                          && other.projectName != null )
                {
                    equality = -1;
                }
                else if ( this.projectName != null
                          && other.projectName == null )
                {
                    equality = 1;
                }
                else
                {
                    equality = this.projectName.trim()
                                               .compareTo( other.projectName.trim() );
                }
            }
        }
        return equality;
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
        return Objects.hash( this.projectID,
                             this.projectName);
    }
}
