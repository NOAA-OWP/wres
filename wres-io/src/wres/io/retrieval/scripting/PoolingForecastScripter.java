package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.util.TimeHelper;

class PoolingForecastScripter extends Scripter
{
    PoolingForecastScripter( OrderedSampleMetadata sampleMetadata,
                             DataSourceConfig dataSourceConfig)
    {
        super( sampleMetadata, dataSourceConfig);
    }

    @Override
    String formScript() throws SQLException, IOException
    {
        boolean usesNetcdf = ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() );
        this.addLine("-- ", this.getSampleMetadata());
        // TODO: Split out into other functions
        this.add("SELECT (EXTRACT(epoch FROM TS.initialization_date");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift().getSeconds());
        }

        this.addLine("))::bigint AS basis_epoch_time,");
        this.addTab().add("(EXTRACT(epoch FROM TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift().getSeconds());
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
        this.applyLeadQualifier();

        this.applyEnsembleConstraint();

        String issueQualifier;

        if ( this.getSampleMetadata()
                 .getTimeWindow()
                 .getEarliestLeadDuration()
                 .equals( this.getSampleMetadata().getTimeWindow().getLatestLeadDuration() ) )
        {
            issueQualifier =
                    "TS.initialization_date = '" + this.getSampleMetadata().getTimeWindow().getEarliestLeadDuration()
                             + "'::timestamp without time zone ";
        }
        else
        {
            // TODO: Change ">=" to ">" when we move to exclusive-inclusive
            issueQualifier =
                    "TS.initialization_date >= '" + this.getSampleMetadata().getTimeWindow().getEarliestReferenceTime()
                             + "'::timestamp without time zone ";
            issueQualifier += "AND TS.initialization_date <= '"
                              + this.getSampleMetadata().getTimeWindow().getLatestReferenceTime()
                              + "'::timestamp without time zone";
        }
        this.addTab().addLine("AND ", issueQualifier);

        if (this.getProjectDetails().getEarliestDate() != null)
        {
            this.addTab().addLine("AND TS.initialization_date + ",
                                   "INTERVAL '1 MINUTE' * TSV.lead >= '",
                                   this.getProjectDetails().getEarliestDate(),
                                   "'");
        }

        if (this.getProjectDetails().getLatestDate() != null)
        {
            this.addTab().addLine("AND TS.initialization_date + ",
                                  "INTERVAL '1 MINUTE' * TSV.lead <= '",
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

    private void applyLeadQualifier()
    {
        long earliest = TimeHelper.durationToLead( this.getSampleMetadata().getMinimumLead() );
        long latest = TimeHelper.durationToLead( this.getSampleMetadata().getTimeWindow().getLatestLeadDuration() );

        if (earliest == latest)
        {
            this.addTab().addLine("AND TSV.lead = ", earliest);
        }
        else
        {
            this.addTab().addLine( "AND TSV.lead > ", earliest);
            this.addTab().addLine( "AND TSV.lead <= ", latest);
        }
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
