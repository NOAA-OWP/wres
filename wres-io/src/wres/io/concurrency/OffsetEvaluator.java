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
    protected Integer execute() throws Exception
    {
        DataScripter scripter = this.formScript();
        return scripter.retrieve( "offset" );
    }

    private DataScripter formScript() throws CalculationException
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

    @Override
    protected Logger getLogger()
    {
        return OffsetEvaluator.LOGGER;
    }
}
