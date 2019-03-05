package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.data.caching.Ensembles;
import wres.io.project.Project;
import wres.io.utilities.ScriptBuilder;

public abstract class Scripter extends ScriptBuilder
{
    Scripter( OrderedSampleMetadata sampleMetadata, DataSourceConfig dataSourceConfig)
    {
        this.sampleMetadata = sampleMetadata;
        this.dataSourceConfig = dataSourceConfig;
    }

    public static String getLoadScript( OrderedSampleMetadata sampleMetadata,
                                        DataSourceConfig dataSourceConfig)
            throws SQLException, IOException
    {
        Scripter loadScripter;

        boolean isForecast = ConfigHelper.isForecast( dataSourceConfig );

        switch ( sampleMetadata.getProject().getPairingMode() )
        {
            case BY_TIMESERIES:
                loadScripter = new SingleTimeSeriesScripter( sampleMetadata, dataSourceConfig );
                break;
            case BASIC:
                if (isForecast)
                {
                    loadScripter = new BasicForecastScripter(
                            sampleMetadata,
                            dataSourceConfig
                    );
                }
                else
                {
                    loadScripter = new BasicObservationScripter(
                            sampleMetadata,
                            dataSourceConfig
                    );
                }
                break;
            case ROLLING:
                if (isForecast)
                {
                    loadScripter = new PoolingForecastScripter(
                            sampleMetadata,
                            dataSourceConfig
                    );
                }
                else
                {
                    throw new ProjectConfigException(
                            sampleMetadata.getProject().getProjectConfig(),
                            "Only forecasts may perform evaluations based on "
                            + "issue times. This configuration is attempting to "
                            + "use observations or simulations instead." );
                }
                break;
            case TIME_SERIES:
                if (isForecast)
                {
                    loadScripter = new TimeSeriesScripter(
                            sampleMetadata,
                            dataSourceConfig
                    );
                }
                else
                {
                    throw new ProjectConfigException(
                            sampleMetadata.getProject().getProjectConfig(),
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

    public static String getPersistenceLoadScript( OrderedSampleMetadata sampleMetadata,
                                                   DataSourceConfig dataSourceConfig,
                                                   List<Instant> basisTimes )
            throws SQLException, IOException
    {
        Objects.requireNonNull( sampleMetadata );
        Objects.requireNonNull( dataSourceConfig );
        Objects.requireNonNull( basisTimes );

        if ( !ConfigHelper.isPersistence( sampleMetadata.getProject().getProjectConfig(),
                                          dataSourceConfig ) )
        {
            throw new IllegalArgumentException( "Must pass a persistence dataSourceConfig" );
        }

        PersistenceForecastScripter s = new PersistenceForecastScripter( sampleMetadata,
                                                                         dataSourceConfig,
                                                                         basisTimes );
        return s.formScript();
    }

    abstract String formScript() throws SQLException, IOException;

    abstract String getBaseDateName();

    abstract String getValueDate();

    Project getProjectDetails()
    {
        return this.sampleMetadata.getProject();
    }

    DataSourceConfig getDataSourceConfig()
    {
        return this.dataSourceConfig;
    }

    Feature getFeature()
    {
        return this.sampleMetadata.getFeature();
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

    private Integer getVariableID() throws SQLException
    {
        if (this.variableID == null)
        {
            if (this.getMember().equalsIgnoreCase( Project.LEFT_MEMBER))
            {
                this.variableID = this.getProjectDetails().getLeftVariableID();
            }
            else if (this.getMember().equalsIgnoreCase( Project.RIGHT_MEMBER ))
            {
                this.variableID = this.getProjectDetails().getRightVariableID();
            }
            else
            {
                this.variableID = this.getProjectDetails().getBaselineVariableID();
            }
        }
        return this.variableID;
    }

    String getVariableFeatureClause() throws SQLException
    {
        if (this.VariableFeatureClause == null)
        {
            this.VariableFeatureClause = ConfigHelper.getVariableFeatureClause(
                    this.getFeature(),
                    this.getVariableID(),
                    "" );
        }
        return this.VariableFeatureClause;
    }

    Duration getTimeShift()
    {
        if (this.timeShift == null &&
            dataSourceConfig.getTimeShift() != null &&
            dataSourceConfig.getTimeShift().getWidth() != 0)
        {
            // TODO: Is it safe to assume this will always be hours?
            // if not, where do we get the units? Most specifications for time
            // have their own specification for units
            this.timeShift = Duration.of(
                    dataSourceConfig.getTimeShift().getWidth(),
                    ChronoUnit.valueOf( dataSourceConfig.getTimeShift().getUnit().toString().toUpperCase() )
            );
        }

        return this.timeShift;
    }

    void applyValueDate()
    {
        this.add("(EXTRACT(epoch FROM ", this.getValueDate(), ")");
        if (this.getTimeShift() != null)
        {
            // The time shift is in hours; we want to convert to seconds
            this.add(" + ", this.getTimeShift().getSeconds());
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
        this.add( ConfigHelper.getSeasonQualifier( this.getProjectDetails(),
                                                   this.getBaseDateName(),
                                                   this.getTimeShift() ) );
    }

    void applyVariableFeatureClause() throws SQLException
    {
        this.addLine( "WHERE ", this.getVariableFeatureClause());
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
            this.member = this.getProjectDetails().getInputName( this.dataSourceConfig );
        }
        return this.member;
    }

    OrderedSampleMetadata getSampleMetadata()
    {
        return this.sampleMetadata;
    }

    String getScript()
    {
        return this.toString();
    }

    private final OrderedSampleMetadata sampleMetadata;
    private final DataSourceConfig dataSourceConfig;

    private Integer variableID;
    private String VariableFeatureClause;
    private Duration timeShift;
    private String member;
}
