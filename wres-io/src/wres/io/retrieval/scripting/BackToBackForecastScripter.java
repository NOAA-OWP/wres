package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;

class BackToBackForecastScripter extends Scripter
{
    BackToBackForecastScripter( ProjectDetails projectDetails,
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
        this.add("SELECT ");
        this.applyValueDate();
        this.addTab().addLine( "TSV.lead,");
        this.addTab().addLine( "ARRAY_AGG(TSV.series_value ORDER BY TS.ensemble_id) AS measurements," );
        this.applyBasisTime();
        this.addTab().addLine( "TS.measurementunit_id" );
        this.addLine( "FROM wres.TimeSeries TS" );
        this.addLine( "INNER JOIN wres.TimeSeriesValue TSV");
        this.addLine( "    ON TSV.timeseries_id = TS.timeseries_id" );

        this.applySourceConstraint();

        this.applyVariableFeatureClause();
        this.applyLeadQualifier();

        this.applyEarliestDateConstraint();
        this.applyLatestDateConstraint();
        this.applyEarliestIssueDateConstraint();
        this.applyLatestIssueDateConstraint();

        this.applyEnsembleConstraint();

        this.applySeasonConstraint();

        this.applyProjectConstraint();
        this.applyGrouping();
        this.applyOrdering();

        return this.getScript();
    }

    @Override
    String getBaseDateName()
    {
        return "TS.initialization_date";
    }

    private void applyBasisTime()
    {
        this.addTab().add( "(EXTRACT( epoch FROM TS.initialization_date)");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift() * 3600);
        }

        this.addLine(")::bigint AS basis_epoch_time,");
    }

    private void applyLeadQualifier() throws SQLException, IOException
    {
        this.addTab().addLine("AND ",
                              this.getProjectDetails()
                                  .getLeadQualifier(
                                          this.getFeature(),
                                          this.getProgress(),
                                          "TSV"
                                  )
        );
    }

    @Override
    protected String getValueDate()
    {
        if (this.validTimeCalculation == null)
        {
            this.validTimeCalculation = this.getBaseDateName() +
                                        " + INTERVAL '1 HOUR' * TSV.lead";
        }
        return this.validTimeCalculation;
    }

    private void applyProjectConstraint()
    {
        this.addLine( "    AND PS.project_id = ", this.getProjectDetails().getId() );
        this.addLine( "    AND PS.member = ", this.getMember());
    }

    private void applySourceConstraint()
    {
        this.addLine( "INNER JOIN wres.TimeSeriesSource AS TSS" );
        this.addTab().addLine( "ON TSS.timeseries_id = TS.timeseries_id" );

        if (ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() ))
        {
            this.addTab(  2  ).addLine("AND TSS.lead = TSV.lead");
        }

        this.addLine( "INNER JOIN wres.ProjectSource AS PS" );
        this.addTab().addLine( "ON PS.source_id = TSS.source_id" );
    }

    private void applyGrouping()
    {
        this.add( "GROUP BY ", this.getBaseDateName(), ", TSV.lead, TS.measurementunit_id" );

        if (ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() ))
        {
            this.addLine();
        }
        else
        {
            this.addLine(", TSS.source_id");
        }
    }

    private void applyOrdering()
    {
        this.add("ORDER BY ", this.getBaseDateName());

        // We generally need to keep similar data from other files separate, so
        // we do so by grouping by source id. All ensembles for NetCDF data
        // will always be in separate files, however, so we need to exclude
        // this logic for NetCDF data
        if (!ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() ))
        {
            this.add(", TSS.source_id");
        }

        this.addLine(", lead");
    }

    private String validTimeCalculation;
}
