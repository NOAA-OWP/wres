package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;


import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.ScriptBuilder;

public abstract class Scripter extends ScriptBuilder
{
    Scripter( ProjectDetails projectDetails,
                        DataSourceConfig dataSourceConfig,
                        Feature feature,
                        int progress,
                        int sequenceStep )
    {
        this.projectDetails = projectDetails;
        this.dataSourceConfig = dataSourceConfig;
        this.feature = feature;
        this.progress = progress;
        this.sequenceStep = sequenceStep;
    }

    public static String getLoadScript( ProjectDetails projectDetails,
                                        DataSourceConfig dataSourceConfig,
                                        Feature feature,
                                        int progress,
                                        int sequenceStep)
            throws SQLException, IOException, ProjectConfigException
    {
        Scripter loadScripter;

        boolean isForecast = ConfigHelper.isForecast( dataSourceConfig );

        switch ( projectDetails.getPairingMode() )
        {
            case BACK_TO_BACK:
                if (isForecast)
                {
                    loadScripter = new BackToBackForecastScripter(
                            projectDetails,
                            dataSourceConfig,
                            feature,
                            progress,
                            sequenceStep
                    );
                }
                else
                {
                    loadScripter = new BackToBackObservationScripter(
                            projectDetails,
                            dataSourceConfig,
                            feature,
                            progress,
                            sequenceStep
                    );
                }
                break;
            case ROLLING:
                if (isForecast)
                {
                    loadScripter = new PoolingForecastScripter(
                            projectDetails,
                            dataSourceConfig,
                            feature,
                            progress,
                            sequenceStep
                    );
                }
                else
                {
                    throw new ProjectConfigException(
                            projectDetails.getProjectConfig(),
                            "Only forecasts may perform evaluations based on "
                            + "issue times. This configuration is attempting to "
                            + "use observations or simulations instead." );
                }
                break;
            case TIME_SERIES:
                if (isForecast)
                {
                    loadScripter = new TimeSeriesScripter(
                            projectDetails,
                            dataSourceConfig,
                            feature,
                            progress,
                            sequenceStep
                    );
                }
                else
                {
                    throw new ProjectConfigException(
                            projectDetails.getProjectConfig(),
                            "Only forecasts may perform time series evaluations."
                            + " This configuration is attempting to use "
                            + "observations or simulations instead." );
                }
                break;
            default:
                throw new IllegalStateException( "A script used to retrieve "
                                                 + "evaluation pairs could not "
                                                 + "be generated." );
        }

        return loadScripter.formScript();
    }

    public static String getPersistenceLoadScript( ProjectDetails projectDetails,
                                                   DataSourceConfig dataSourceConfig,
                                                   Feature feature,
                                                   List<Instant> basisTimes )
            throws SQLException
    {
        Objects.requireNonNull( projectDetails );
        Objects.requireNonNull( dataSourceConfig );
        Objects.requireNonNull( feature );
        Objects.requireNonNull( basisTimes );

        if ( !ConfigHelper.isPersistence( projectDetails.getProjectConfig(),
                                          dataSourceConfig ) )
        {
            throw new IllegalArgumentException( "Must pass a persistence dataSourceConfig" );
        }

        PersistenceForecastScripter s = new PersistenceForecastScripter( projectDetails,
                                                                         dataSourceConfig,
                                                                         feature,
                                                                         basisTimes );
        return s.formScript();
    }

    abstract String formScript() throws SQLException, IOException;

    abstract String getBaseDateName();

    abstract String getValueDate();

    ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

    DataSourceConfig getDataSourceConfig()
    {
        return this.dataSourceConfig;
    }

    Feature getFeature()
    {
        return this.feature;
    }

    int getProgress() throws IOException
    {
        return this.progress;
    }

    int getSequenceStep()
    {
        return this.sequenceStep;
    }


    void applyEnsembleConstraint() throws SQLException
    {
        if ( !this.getDataSourceConfig().getEnsemble().isEmpty() )
        {
            int includeCount = 0;
            int excludeCount = 0;

            StringJoiner
                    include = new StringJoiner( ",", "ANY('{", "}'::integer[])");
            StringJoiner exclude = new StringJoiner(",", "ANY('{", "}'::integer[])");

            for ( EnsembleCondition condition : this.getDataSourceConfig().getEnsemble())
            {
                List<Integer> ids = Ensembles.getEnsembleIDs( condition );
                if ( condition.isExclude() )
                {
                    excludeCount += ids.size();
                    ids.forEach( id -> exclude.add(id.toString()) );
                }
                else
                {
                    includeCount += ids.size();
                    ids.forEach( id -> include.add(id.toString()) );
                }
            }

            if (includeCount > 0)
            {
                this.addLine( "    AND ensemble_id = ", include.toString() );
            }

            if (excludeCount > 0)
            {
                this.addLine( "    AND NOT ensemble_id = ", exclude.toString() );
            }
        }
    }

    Integer getVariableID() throws SQLException
    {
        if (this.variableID == null)
        {
            if (this.getMember().equalsIgnoreCase( ProjectDetails.LEFT_MEMBER))
            {
                this.variableID = this.projectDetails.getLeftVariableID();
            }
            else if (this.getMember().equalsIgnoreCase( ProjectDetails.RIGHT_MEMBER ))
            {
                this.variableID = this.projectDetails.getRightVariableID();
            }
            else
            {
                this.variableID = this.projectDetails.getBaselineVariableID();
            }
        }
        return this.variableID;
    }

    String getVariablePositionClause() throws SQLException
    {
        if (this.variablePositionClause == null)
        {
            this.variablePositionClause = ConfigHelper.getVariablePositionClause(
                    this.getFeature(),
                    this.getVariableID(),
                    "" );
        }
        return this.variablePositionClause;
    }

    Integer getTimeShift()
    {
        if (this.timeShift == null &&
            dataSourceConfig.getTimeShift() != null &&
            dataSourceConfig.getTimeShift().getWidth() != 0)
        {
            // TODO: Is it safe to assume this will always be hours?
            // if not, where do we get the units? Most specifications for time
            // have their own specification for units
            this.timeShift = dataSourceConfig.getTimeShift().getWidth();
        }

        return this.timeShift;
    }

    void applyValueDate()
    {
        this.add("(EXTRACT(epoch FROM ", this.getValueDate(), ")");
        if (this.getTimeShift() != null)
        {
            // The time shift is in hours; we want to convert to seconds
            this.add(" + ", this.getTimeShift() * 3600);
        }

        this.addLine(")::bigint AS value_date,");
    }

    void applyTimeShift()
    {
        if (this.getTimeShift() != null)
        {
            this.add(" + INTERVAL '1 HOUR' * ");
            this.add(this.getTimeShift());
        }
    }

    void applySeasonConstraint()
    {
        this.add(ConfigHelper.getSeasonQualifier( this.getProjectDetails(),
                                                  this.getBaseDateName(),
                                                  this.getTimeShift() ));
    }

    void applyVariablePositionClause() throws SQLException
    {
        this.addLine( "WHERE ", this.getVariablePositionClause());
    }

    void applyEarliestIssueDateConstraint()
    {
        if ( this.getProjectDetails().getEarliestIssueDate() != null)
        {
            this.add("    AND ", this.getBaseDateName());
            this.applyTimeShift();
            this.addLine( " >= '", this.getProjectDetails().getEarliestIssueDate(), "'" );
        }
    }

    void applyLatestIssueDateConstraint()
    {
        if ( this.getProjectDetails().getLatestIssueDate() != null)
        {
            this.add("    AND ", this.getBaseDateName());
            this.applyTimeShift();
            this.addLine( " <= '", this.getProjectDetails().getLatestIssueDate(), "'" );
        }
    }

    void applyEarliestDateConstraint() throws SQLException
    {
        if (this.getProjectDetails().getEarliestDate() != null)
        {
            this.add("    AND ", this.getValueDate());
            this.applyTimeShift();
            this.addLine(" >= '", this.getProjectDetails().getEarliestDate(), "'");
        }
    }

    void applyLatestDateConstraint()
    {
        if (this.getProjectDetails().getLatestDate() != null)
        {
            this.add("    AND ", this.getValueDate());
            this.applyTimeShift();
            this.addLine(" <= '", this.getProjectDetails().getLatestDate(), "'");
        }
    }

    String getMember()
    {
        if (this.member == null)
        {
            this.member = this.projectDetails.getInputName( this.dataSourceConfig );
        }
        return this.member;
    }

    String getScript()
    {
        return this.toString();
    }

    private final ProjectDetails projectDetails;
    private final DataSourceConfig dataSourceConfig;
    private final Feature feature;
    private final int progress;
    private final int sequenceStep;

    private Integer variableID;
    private String variablePositionClause;
    private Integer timeShift;
    private String member;

}
