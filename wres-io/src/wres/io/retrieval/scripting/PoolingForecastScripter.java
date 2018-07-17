package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

import org.apache.commons.lang3.tuple.Pair;

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
        this.addTab().add("(EXTRACT(epoch FROM TS.initialization_date + INTERVAL '1 HOUR' * TSV.lead");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift() * 3600);
        }

        this.addLine("))::bigint AS value_date,");
        this.addTab().addLine("TSV.lead,");
        this.addTab().addLine("ARRAY_AGG(TSV.series_value ORDER BY TS.ensemble_id) AS measurements,");
        this.addTab().addLine("TS.measurementunit_id");
        this.addLine("FROM wres.TimeSeries TS");
        this.addLine("INNER JOIN wres.TimeSeriesValue TSV");
        this.addTab().addLine("ON TSV.timeseries_id = TS.timeseries_id");
        this.addLine("INNER JOIN wres.TimeSeriesSource TSS");
        this.addTab().addLine("ON TSS.timeseries_id = TS.timeseries_id");
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
        this.addTab().addLine("ON PS.source_id = TSS.source_id");

        if ( usesNetcdf )
        {
            this.addTab( 2 ).addLine( "AND PS.lead = TSV.lead" );
        }

        this.addLine("WHERE TS.", this.getVariableFeatureClause());
        this.addTab().addLine("AND ", this.getProjectDetails().getLeadQualifier( this.getFeature(), this.getProgress(), "TSV" ));

        this.applyEnsembleConstraint();

        Pair<Instant, Instant> issueRange = this.getProjectDetails().getIssueDateRange( this.getSequenceStep() );

        String issueQualifier;
        if (issueRange.getLeft().equals(issueRange.getRight()))
        {
            issueQualifier = "TS.initialization_date = '" + issueRange.getLeft() + "'::timestamp without time zone ";
        }
        else
        {
            issueQualifier = "TS.initialization_date >= '" + issueRange.getLeft() + "'::timestamp without time zone ";
            issueQualifier += "AND TS.initialization_date <= '" + issueRange.getRight() + "'::timestamp without time zone";
        }
        this.addTab().addLine("AND ", issueQualifier);

        if (this.getProjectDetails().getEarliestDate() != null)
        {
            this.addTab().addLine("AND TS.initialization_date + ",
                                   "INTERVAL '1 HOUR' * TSV.lead >= '",
                                   this.getProjectDetails().getEarliestDate(),
                                   "'");
        }

        if (this.getProjectDetails().getLatestDate() != null)
        {
            this.addTab().addLine("AND TS.initialization_date + ",
                                  "INTERVAL '1 HOUR' * TSV.lead <= '",
                                  this.getProjectDetails().getLatestDate(),
                                  "'");
        }

        this.addTab().addLine("AND EXISTS (");
        this.addTab(  2  ).addLine("SELECT 1");
        this.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        this.addTab(  2  ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
        this.addTab(   3   ).addLine("AND PS.member = ", this.getMember());
        this.addTab(   3   ).addLine("AND PS.source_id = TSS.source_id");
        this.addTab().addLine(")");
        this.add("GROUP BY TS.initialization_date, TSV.lead, ");

        if (!usesNetcdf)
        {
            this.add( "TSS.source_id, " );
        }

        this.addLine("TS.measurementunit_id");

        this.add("ORDER BY TS.initialization_date, ");

        if (!usesNetcdf)
        {
            this.add( "TSS.source_id, " );
        }

        this.add("TSV.lead;");

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
