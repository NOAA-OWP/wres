package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.TimeWindowMode;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.NotImplementedException;

public abstract class Scripter
{
    private static final String NEWLINE = System.lineSeparator();

    protected Scripter( ProjectDetails projectDetails,
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
            throws NoDataException, SQLException,
            InvalidPropertiesFormatException
    {
        Scripter loadScripter;

        TimeWindowMode mode = projectDetails.getPoolingMode();
        boolean isForecast = ConfigHelper.isForecast( dataSourceConfig );

        switch ( mode )
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
                    loadScripter = new PoolingObservationScripter(
                            projectDetails,
                            dataSourceConfig,
                            feature,
                            progress,
                            sequenceStep
                    );
                }
                break;
            default:
                throw new NotImplementedException( "Comparison data could not be " +
                                                   "loaded due to an incorrect " +
                                                   "configuration. The configuration " +
                                                   "mode of '" +
                                                   String.valueOf(mode) +
                                                   "' is not supported." );
        }

        return loadScripter.formScript();
    }

    abstract String formScript() throws SQLException,
            InvalidPropertiesFormatException, NoDataException;

    abstract String getBaseDateName();

    abstract String getValueDate();

    protected ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

    protected DataSourceConfig getDataSourceConfig()
    {
        return this.dataSourceConfig;
    }

    protected Feature getFeature()
    {
        return this.feature;
    }

    protected int getProgress() throws NoDataException
    {
        return this.progress;
    }

    protected int getSequenceStep()
    {
        return this.sequenceStep;
    }

    protected Integer getVariableID() throws SQLException
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

    protected String getVariablePositionClause() throws SQLException
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

    protected Integer getTimeShift()
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

    protected void add(Object... details)
    {
        if (this.script == null)
        {
            this.script = new StringBuilder(  );
        }

        for (Object detail : details)
        {
            this.script.append( detail );
        }
    }

    protected void addLine()
    {
        this.add(NEWLINE);
    }

    protected void addLine(Object... details)
    {
        this.add(details);
        this.addLine();
    }

    protected void applyValueDate()
    {
        this.add("(", this.getValueDate());
        this.applyTimeShift();
        this.addLine(") AS value_date,");
    }

    protected void applyTimeShift()
    {
        if (this.getTimeShift() != null)
        {
            this.add(" + INTERVAL '1 HOUR' * ");
            this.add(this.getTimeShift());
        }
    }

    protected void applySeasonConstraint()
    {
        this.add(ConfigHelper.getSeasonQualifier( this.getProjectDetails(),
                                                  this.getBaseDateName(),
                                                  this.getTimeShift() ));
    }

    protected void applyVariablePositionClause() throws SQLException
    {
        this.addLine( "WHERE ", this.getVariablePositionClause());
    }

    protected void applyEarliestIssueDateConstraint()
    {
        if ( this.getProjectDetails().getEarliestIssueDate() != null)
        {
            this.add("    AND ", this.getBaseDateName());
            this.applyTimeShift();
            this.addLine( " >= '", this.getProjectDetails().getEarliestIssueDate(), "'" );
        }
    }

    protected void applyLatestIssueDateConstraint()
    {
        if ( this.getProjectDetails().getLatestIssueDate() != null)
        {
            this.add("    AND ", this.getBaseDateName());
            this.applyTimeShift();
            this.addLine( " <= '", this.getProjectDetails().getLatestIssueDate(), "'" );
        }
    }

    protected void applyEarliestDateConstraint() throws SQLException
    {
        if (this.getProjectDetails().getEarliestDate() != null)
        {
            this.add("    AND ", this.getValueDate());
            this.applyTimeShift();
            this.addLine(" >= '", this.getProjectDetails().getEarliestDate(), "'");
        }
    }

    protected void applyLatestDateConstraint()
    {
        if (this.getProjectDetails().getLatestDate() != null)
        {
            this.add("    AND ", this.getValueDate());
            this.applyTimeShift();
            this.addLine(" <= '", this.getProjectDetails().getLatestDate(), "'");
        }
    }

    protected String getMember()
    {
        if (this.member == null)
        {
            this.member = this.projectDetails.getInputName( this.dataSourceConfig );
        }
        return this.member;
    }

    protected String getScript()
    {
        return this.script.toString();
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
    private StringBuilder script;

}
