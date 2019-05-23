package wres.io.concurrency;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.TimeScaleConfig;
import wres.io.data.caching.Ensembles;
import wres.io.project.Project;
import wres.io.utilities.DataScripter;
import wres.util.CalculationException;
import wres.util.Strings;
import wres.util.TimeHelper;

public class OffsetEvaluator extends WRESCallable<Integer>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( OffsetEvaluator.class );

    private final int observationVariableFeatureID;
    private final int timeseriesVariableFeatureID;
    private final Project project;

    public OffsetEvaluator( final Project project, final int observationVariableFeatureID, final int timeseriesVariableFeatureID)
    {
        this.project = project;
        this.observationVariableFeatureID = observationVariableFeatureID;
        this.timeseriesVariableFeatureID = timeseriesVariableFeatureID;
    }

    @Override
    protected Integer execute() throws SQLException
    {
        DataScripter script;

        if ( this.project.getPairingMode() == Project.PairingMode.BY_TIMESERIES)
        {
            script = this.formPerTimeSeriesScript();
        }
        else
        {
            script = this.formScript();
        }
        return script.retrieve( "offset" );
    }

    /**
     * @throws CalculationException
     */
    private DataScripter formScript()
    {
        DataScripter script = new DataScripter(  );

        TimeScaleConfig scale = this.project.getScale();
        long width = TimeHelper.unitsToLeadUnits( scale.getUnit().value(), scale.getPeriod() );

        Integer arbitraryEnsembleId;

        try
        {
            arbitraryEnsembleId = Ensembles.getSingleEnsembleID( this.project.getId(), this.timeseriesVariableFeatureID);
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "An ensemble member used to evaluate the "
                                            + "offset of a forecast could not be loaded.",
                                            e );
        }

        script.addLine("SELECT TS.offset");
        script.addLine("FROM (");
        script.addTab().add( "SELECT TS.initialization_date + INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * (TSV.lead + ", width, ")");

        if ( this.project.getRightTimeShift() != null)
        {
            script.add( " + '", this.project.getRightTimeShift(), "'");
        }

        script.addLine(" AS valid_time,");
        script.addTab(  2  ).addLine("TSV.lead - ", width, " AS offset");
        script.addTab().addLine("FROM (");
        script.addTab(  2  ).addLine("SELECT TS.timeseries_id, TS.initialization_date");
        script.addTab(  2  ).addLine("FROM (");
        script.addTab(   3   ).addLine("SELECT TS.timeseries_id, TS.initialization_date");
        script.addTab(   3   ).addLine("FROM wres.TimeSeries TS");
        script.addTab(   3   ).addLine("WHERE TS.variablefeature_id = ?");

        script.addArgument( this.timeseriesVariableFeatureID );

        script.addTab(    4    ).addLine("AND TS.ensemble_id = ", arbitraryEnsembleId);

        if ( Strings.hasValue(this.project.getEarliestIssueDate()))
        {
            script.addTab(    4    ).addLine( "AND TS.initialization_date >= '", this.project.getEarliestIssueDate(), "'");
        }

        if (Strings.hasValue( this.project.getLatestIssueDate() ))
        {
            script.addTab(    4    ).addLine( "AND TS.initialization_date <= '", this.project.getLatestIssueDate(), "'" );
        }

        script.addTab(  2  ).addLine(") AS TS");
        script.addTab(  2  ).addLine("INNER JOIN (");
        script.addTab(   3   ).addLine("SELECT TSS.timeseries_id");
        script.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        script.addTab(   3   ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab(    4    ).addLine("ON TSS.source_id = PS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", this.project.getId());
        script.addTab(    4    ).addLine("AND PS.member = ", Project.RIGHT_MEMBER);
        script.addTab(  2  ).addLine(") AS TSS");
        script.addTab(    3   ).addLine("ON TSS.timeseries_id = TS.timeseries_id");
        script.addTab(  2  ).addLine("GROUP BY TS.timeseries_id, TS.initialization_date");
        script.addTab().addLine(") AS TS");
        script.addTab().addLine("INNER JOIN (");
        script.addTab(  2  ).addLine("SELECT TSV.timeseries_id, TSV.lead");
        script.addTab(  2  ).addLine("FROM wres.TimeSeriesValue TSV");
        script.addTab(  2  ).add("WHERE TSV.lead >= ");

        //This is giving numbers like -60
        if ( this.project.getMinimumLead() != Integer.MIN_VALUE)
        {
            // The minimum lead is configured in hours, but we work with minues, so we need to adjust
            script.addLine( this.project.getMinimumLead() - TimeHelper.unitsToLeadUnits( "HOURS", 1 ) + width);
        }
        else
        {
            script.addLine(width);
        }

        script.addTab(   3   ).add("AND TSV.lead <= ");

        if ( this.project.getMaximumLead() != Integer.MAX_VALUE)
        {
            script.addLine(this.project.getMaximumLead());
        }
        else
        {
            // We want to set a limit to how far ahead we're going to look. If we are going to group
            // values across 24 hours, a 5 day sliding buffer should be enough to find an offset
            script.addLine(width * 5);
        }

        script.addTab().addLine(") AS TSV");
        script.addTab(  2  ).addLine("ON TSV.timeseries_id = TS.timeseries_id");

        script.addTab().addLine("GROUP BY TS.initialization_date, TSV.lead");
        script.addTab().addLine("ORDER BY valid_time, TSV.lead");
        script.addLine(") AS TS");
        script.addLine("INNER JOIN (");
        script.addTab().addLine("SELECT O.observation_time");
        script.addTab().addLine("FROM (");
        script.addTab(  2  ).addLine("SELECT source_id");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ", this.project.getId());
        script.addTab(   3   ).addLine( "AND PS.member = ", Project.LEFT_MEMBER);
        script.addTab().addLine(") AS PS");
        script.addTab().addLine("INNER JOIN (");
        script.addTab(  2  ).add("SELECT source_id, observation_time");

        if ( this.project.getLeftTimeShift() != null)
        {
            script.add( " + '", this.project.getLeftTimeShift(), "'");
        }

        script.addLine(" AS observation_time");

        script.addTab(  2  ).addLine("FROM wres.Observation O");
        script.addTab(  2  ).addLine("WHERE O.variablefeature_id = ?");

        script.addArgument( this.observationVariableFeatureID );

        if (Strings.hasValue( this.project.getEarliestDate() ))
        {
            script.addTab(   3   ).addLine( "AND O.observation_time >= '", this.project.getEarliestDate(), "'");
        }

        if (Strings.hasValue(this.project.getLatestDate()))
        {
            script.addTab(   3   ).addLine( "AND O.observation_time <= '", this.project.getLatestDate(), "'");
        }

        script.addTab().addLine(") AS O");
        script.addTab(  2  ).addLine(" ON O.source_id = PS.source_id");

        script.addLine(") AS OT");
        script.addTab().addLine("ON OT.observation_time = TS.valid_time");
        script.addLine("ORDER BY TS.offset");
        script.add("LIMIT 1;");

        return script;
    }

    /**l
     * Forms a script specialized for creating the offset value for cases where whole time series are to
     * be evaluated in an isolated fashion
     * @return A script to find the offset specialized for by-timeseries evaluations
     */
    private DataScripter formPerTimeSeriesScript()
    {
        DataScripter script = new DataScripter(  );

        script.addLine("SELECT 0 AS offset");
        script.addLine("FROM (");
        script.addTab().add("SELECT TS.initialization_date + INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * TSV.lead");

        if (this.project.getRightTimeShift() != null)
        {
            script.add(" + '", this.project.getRightTimeShift(), "'");
        }

        script.addLine(" AS valid_time");
        script.addTab().addLine("FROM (");
        script.addTab(  2  ).addLine("SELECT TS.timeseries_id, TS.initialization_date");
        script.addTab(  2  ).addLine("FROM (");
        script.addTab(   3   ).addLine("SELECT TS.timeseries_id, TS.initialization_date");
        script.addTab(   3   ).addLine("FROM wres.TimeSeries TS");
        script.addTab(   3   ).addLine("WHERE TS.variablefeature_id = ?");

        script.addArgument( this.timeseriesVariableFeatureID );

        if (this.project.getEarliestIssueDate() != null)
        {
            script.addTab(    4    ).addLine("AND TS.initialization_date >= CAST(? AS TIMESTAMP WITHOUT TIME ZONE)");
            script.addArgument( this.project.getEarliestIssueDate() );
        }

        if (this.project.getLatestIssueDate() != null)
        {
            script.addTab(    4    ).addLine("AND TS.initialization_date <= CAST(? AS TIMESTAMP WITHOUT TIME ZONE)");
            script.addArgument( this.project.getLatestIssueDate() );
        }

        script.addTab(  2  ).addLine(") AS TS");
        script.addTab(  2  ).addLine("INNER JOIN (");
        script.addTab(   3   ).addLine("SELECT TSS.timeseries_id");
        script.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        script.addTab(   3   ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab(    4    ).addLine("ON TSS.source_id = PS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ?");

        script.addArgument( this.project.getId() );

        script.addTab(    4    ).addLine("AND PS.member = ", Project.RIGHT_MEMBER);
        script.addTab(  2  ).addLine(") AS TSS");
        script.addTab(   3   ).addLine("ON TSS.timeseries_id = TS.timeseries_id");
        script.addTab().addLine(") AS TS");
        script.addTab().addLine("INNER JOIN (");
        script.addTab(  2  ).addLine("SELECT TSV.timeseries_id, TSV.lead");
        script.addTab(  2  ).addLine("FROM wres.TimeSeriesValue TSV");
        script.addTab(  2  ).addLine("WHERE TSV.series_value IS NOT NULL");

        if (this.project.getMinimumLead() > Integer.MIN_VALUE)
        {
            script.addTab(   3   ).addLine("AND TSV.lead >= ?");
            script.addArgument( this.project.getMinimumValue() );
        }

        if (this.project.getMaximumLead() < Integer.MAX_VALUE)
        {
            script.addTab(   3   ).addLine("AND TSV.lead <= ?");
            script.addArgument( this.project.getMaximumLead() );
        }

        if (this.project.getMinimumValue() > -Double.MAX_VALUE && this.project.getDefaultMinimumValue() == null)
        {
            script.addTab(   3   ).addLine("AND series_value >= ?");
            script.addArgument( this.project.getMinimumValue() );
        }

        if (this.project.getMaximumValue() < Double.MAX_VALUE && this.project.getDefaultMaximumValue() == null)
        {
            script.addTab(   3   ).addLine("AND series_value <= ?");
            script.addArgument( this.project.getMaximumValue() );
        }

        script.addTab().addLine(") AS TSV");
        script.addTab(  2  ).addLine("ON TSV.timeseries_id = TS.timeseries_id");
        script.addTab().addLine("GROUP BY TS.initialization_date, TSV.lead");
        script.addTab().addLine("ORDER BY valid_time");
        script.addLine(") AS TS");
        script.addLine("WHERE EXISTS (");
        script.addTab().addLine("SELECT 1");
        script.addTab().addLine("FROM (");
        script.addTab(  2  ).addLine("SELECT source_id");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ?");

        script.addArgument( this.project.getId() );

        script.addTab(   3   ).addLine("AND PS.member = ", Project.LEFT_MEMBER);
        script.addTab().addLine(") AS PS");
        script.addTab().addLine("INNER JOIN (");
        script.addTab(  2  ).add("SELECT source_id, observation_time");

        if (this.project.getLeftTimeShift() != null)
        {
            script.add(" + '", this.project.getLeftTimeShift(), "' AS observation_time");
        }

        script.addLine();
        script.addTab(  2  ).addLine("FROM wres.Observation");
        script.addTab(  2  ).addLine("WHERE variablefeature_id = ?");

        script.addArgument( this.observationVariableFeatureID );

        script.addTab(   3   ).addLine("AND observed_value IS NOT NULL");

        if (this.project.getEarliestDate() != null)
        {
            script.addTab(   3   ).add("AND observation_time");

            if (this.project.getLeftTimeShift() != null)
            {
                script.add(" + '", this.project.getLeftTimeShift(), "'");
            }

            script.addLine(" >= CAST(? AS TIMESTAMP WITHOUT TIME ZONE)");

            script.addArgument( this.project.getEarliestDate() );
        }

        if (this.project.getLatestDate() != null)
        {
            script.addTab(   3   ).add("AND observation_time");

            if (this.project.getLeftTimeShift() != null)
            {
                script.add(" + '", this.project.getLeftTimeShift(), "'");
            }

            script.addLine(" <= CAST(? AS TIMESTAMP WITHOUT TIME ZONE)");
            script.addArgument( this.project.getLatestDate() );
        }

        if (this.project.getMinimumValue() > -Double.MAX_VALUE && this.project.getDefaultMinimumValue() == null)
        {
            script.addTab(   3   ).addLine("AND observed_value >= ?");
            script.addArgument( this.project.getMinimumValue() );
        }

        if (this.project.getMaximumValue() < Double.MAX_VALUE && this.project.getDefaultMaximumValue() == null)
        {
            script.addTab(   3   ).addLine("AND observed_value <= ?");
            script.addArgument( this.project.getMaximumValue() );
        }

        script.addTab().addLine(") AS O");
        script.addTab(  2  ).addLine("ON O.source_id = PS.source_id");
        script.addTab().addLine("WHERE O.observation_time = TS.valid_time");
        script.addLine(")");
        script.add("LIMIT 1;");

        return script;
    }

    @Override
    protected Logger getLogger()
    {
        return OffsetEvaluator.LOGGER;
    }
}
