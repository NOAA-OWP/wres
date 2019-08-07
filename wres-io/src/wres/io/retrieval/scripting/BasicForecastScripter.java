package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.util.TimeHelper;

class BasicForecastScripter extends Scripter
{
    BasicForecastScripter( OrderedSampleMetadata sampleMetadata,
                           DataSourceConfig dataSourceConfig)
    {
        super( sampleMetadata, dataSourceConfig );
    }

    @Override
    String formScript() throws SQLException, IOException
    {
        this.addLine("-- ", this.getSampleMetadata());
        this.add("SELECT ");
        this.applyValueDate();
        this.addTab().addLine( "TSV.lead,");
        this.addTab().addLine( "ARRAY_AGG(TSV.series_value ORDER BY TS.ensemble_id) AS measurements," );
        this.addTab().addLine( "ARRAY_AGG(TS.ensemble_id ORDER BY TS.ensemble_id) AS members,");
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
            this.add(" + ", this.getTimeShift().getSeconds());
        }

        this.addLine(")::bigint AS basis_epoch_time,");
    }

    @Override
    protected String getValueDate()
    {
        if (this.validTimeCalculation == null)
        {
            this.validTimeCalculation = this.getBaseDateName() +
                                        " + INTERVAL '1 MINUTE' * TSV.lead";
        }
        return this.validTimeCalculation;
    }

    private void applyProjectConstraint()
    {
        this.addTab().addLine( "AND PS.project_id = ", this.getProjectDetails().getId() );
        this.addTab().addLine( "AND PS.member = ", this.getMember());
    }

    private void applySourceConstraint()
    {
        this.addLine( "INNER JOIN wres.TimeSeriesSource AS TSS" );
        this.addTab().addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
        this.addTab(  2  ).addLine("AND (TSS.lead IS NULL OR TSS.lead = TSV.lead)");
        this.addLine( "INNER JOIN wres.ProjectSource AS PS" );
        this.addTab().addLine( "ON PS.source_id = TSS.source_id" );
    }

    private void applyGrouping()
    {
        this.add( "GROUP BY ", this.getBaseDateName(), ", TSV.lead, TS.measurementunit_id" );

        if (ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() ) ||
                ConfigHelper.usesS3Data( this.getProjectDetails().getProjectConfig() ))
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
        if (!ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() ) &&
            !ConfigHelper.usesS3Data( this.getProjectDetails().getProjectConfig() ))
        {
            this.add(", TSS.source_id");
        }

        this.addLine(", lead");
    }

    private String validTimeCalculation;
}
