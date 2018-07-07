package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.util.TimeHelper;

class PoolingForecastScripter extends Scripter
{
    PoolingForecastScripter( ProjectDetails projectDetails,
                                       DataSourceConfig dataSourceConfig,
                                       Feature feature,
                                       int progress,
                                       int sequenceStep)
    {
        super( projectDetails, dataSourceConfig, feature, progress, sequenceStep );
    }

    @Override
    String formScript() throws SQLException, IOException
    {
        boolean usesNetcdf = ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() );

        // TODO: Split out into other functions
        this.add("SELECT (EXTRACT(epoch FROM TS.initialization_date");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift() * 3600);
        }

        this.addLine("))::bigint AS basis_epoch_time,");
        this.addTab().add("(EXTRACT(epoch FROM TS.initialization_date + INTERVAL '1 HOUR' * FV.lead");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift() * 3600);
        }

        this.addLine("))::bigint AS value_date,");
        this.addTab().addLine("FV.lead,");
        this.addTab().addLine("ARRAY_AGG(FV.forecasted_value ORDER BY TS.ensemble_id) AS measurements,");
        this.addTab().addLine("TS.measurementunit_id");
        this.addLine("FROM wres.TimeSeries TS");
        this.addLine("INNER JOIN wres.ForecastValue FV");
        this.addTab().addLine("ON FV.timeseries_id = TS.timeseries_id");
        this.addLine("INNER JOIN wres.ForecastSource FS");
        this.addTab().addLine("ON FS.forecast_id = TS.timeseries_id");
        this.addLine("INNER JOIN (");
        this.addTab().addLine("SELECT PS.source_id");

        if (usesNetcdf)
        {
            this.addTab( 2 ).addLine( ", S.lead" );
        }

        this.addTab().addLine("FROM wres.ProjectSource PS");

        if (usesNetcdf)
        {
            this.addTab().addLine( "INNER JOIN wres.Source S" );
            this.addTab( 2 ).addLine( "ON S.source_id = PS.source_id" );
        }

        this.addTab().addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
        this.addTab(  2  ).addLine("AND PS.member = ", this.getMember());
        this.addLine(") AS PS");
        this.addTab().addLine("ON PS.source_id = FS.source_id");

        if ( usesNetcdf )
        {
            this.addTab( 2 ).addLine( "AND PS.lead = FV.lead" );
        }

        this.addLine("WHERE TS.", this.getVariablePositionClause());
        this.addTab().addLine("AND ", this.getProjectDetails().getLeadQualifier( this.getFeature(), this.getProgress(), "FV" ));

        this.applyEnsembleConstraint();
        String issueQualifier = this.getProjectDetails().getIssueDatesQualifier(
                this.getSequenceStep(),
                "TS.initialization_date"
        );
        this.addTab().addLine("AND ", issueQualifier);

        if (this.getProjectDetails().getEarliestDate() != null)
        {
            this.addTab().addLine("AND TS.initialization_date + ",
                                   "INTERVAL '1 HOUR' * FV.lead >= '",
                                   this.getProjectDetails().getEarliestDate(),
                                   "'");
        }

        if (this.getProjectDetails().getLatestDate() != null)
        {
            this.addTab().addLine("AND TS.initialization_date + ",
                                  "INTERVAL '1 HOUR' * FV.lead <= '",
                                  this.getProjectDetails().getLatestDate(),
                                  "'");
        }

        this.addTab().addLine("AND EXISTS (");
        this.addTab(  2  ).addLine("SELECT 1");
        this.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        this.addTab(  2  ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
        this.addTab(   3   ).addLine("AND PS.member = ", this.getMember());
        this.addTab(   3   ).addLine("AND PS.source_id = FS.source_id");
        this.addTab().addLine(")");
        this.add("GROUP BY TS.initialization_date, FV.lead, ");

        if (!usesNetcdf)
        {
            this.add( "FS.source_id, " );
        }

        this.addLine("TS.measurementunit_id");

        this.add("ORDER BY TS.initialization_date, ");

        if (!usesNetcdf)
        {
            this.add( "FS.source_id, " );
        }

        this.add("FV.lead;");

        return this.getScript();
    }

    @Override
    String getBaseDateName()
    {
        return "F.basis_time";
    }

    @Override
    String getValueDate()
    {
        return "F.valid_time";
    }
}
