package wres.io.data.details;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.SQLExecutor;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
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

    private final Map<Integer, String> leftHashes = new TreeMap<>(  );
    private final Map<Integer, String> rightHashes = new TreeMap<>(  );
    private final Map<Integer, String> baselineHashes = new TreeMap<>(  );

    private final List<Integer> leftSources = new ArrayList<>(  );
    private final List<Integer> rightSources = new ArrayList<>(  );
    private final List<Integer> baselineSources = new ArrayList<>(  );

    private Integer leftVariableID = null;
    private Integer rightVariableID = null;
    private Integer baselineVariableID = null;

    public static final String LEFT_MEMBER = "'left'";
    public static final String RIGHT_MEMBER = "'right'";
    public static final String BASELINE_MEMBER = "'baseline'";

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

        for ( Feature feature : left.getFeatures())
        {
            hashBuilder.append( ConfigHelper.getFeatureDescription( feature ) );
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

        for ( Feature feature : right.getFeatures())
        {
            hashBuilder.append( ConfigHelper.getFeatureDescription( feature ) );
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

            for ( Feature feature : baseline.getFeatures())
            {
                hashBuilder.append( ConfigHelper.getFeatureDescription( feature ) );
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

        return hashBuilder.toString().hashCode();
    }

    public ProjectDetails(ProjectConfig projectConfig)
    {
        this.projectConfig = projectConfig;
    }

    @Override
    public Integer getKey() {
        return this.getInputCode();
    }

    private String getLeftVariableName()
    {
        return this.projectConfig.getInputs().getLeft().getVariable().getValue();
    }

    private String getLeftVariableUnit()
    {
        return this.projectConfig.getInputs().getLeft().getVariable().getUnit();
    }

    private String getRightVariableName()
    {
        return this.projectConfig.getInputs().getRight().getVariable().getValue();
    }

    private String getRightVariableUnit()
    {
        return this.projectConfig.getInputs().getRight().getVariable().getUnit();
    }

    private String getBaselineVariableName()
    {
        String name = null;
        if (this.projectConfig.getInputs().getBaseline() != null)
        {
            name = this.projectConfig.getInputs().getBaseline().getVariable().getValue();
        }
        return name;
    }

    private String getBaselineVariableUnit()
    {
        String unit = null;
        if (this.projectConfig.getInputs().getBaseline() != null)
        {
            unit = this.projectConfig.getInputs().getBaseline().getVariable().getUnit();
        }
        return unit;
    }

    private String getProjectName()
    {
        return this.projectConfig.getName();
    }

    @Override
    public void save() throws SQLException
    {
        super.save();

        this.loadSources();
        this.loadForecastIDs();
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

    private Integer getInputCode()
    {
        return ProjectDetails.hash( this.projectConfig );
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
