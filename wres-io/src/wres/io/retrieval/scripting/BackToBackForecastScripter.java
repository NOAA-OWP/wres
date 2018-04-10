package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
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
        this.addTab().addLine( "FV.lead,");
        this.addTab().addLine( "ARRAY_AGG(FV.forecasted_value ORDER BY TS.ensemble_id) AS measurements," );
        this.applyBasisTime();
        this.addTab().addLine( "TS.measurementunit_id" );
        this.addLine( "FROM wres.TimeSeries TS" );
        this.addLine( "INNER JOIN wres.ForecastValue FV");
        this.addLine( "    ON FV.timeseries_id = TS.timeseries_id" );

        this.applySourceConstraint();

        this.applyVariablePositionClause();
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
                                          "FV"
                                  )
        );
    }

    @Override
    protected String getValueDate()
    {
        if (this.validTimeCalculation == null)
        {
            this.validTimeCalculation = this.getBaseDateName() +
                                        " + INTERVAL '1 HOUR' * FV.lead";
        }
        return this.validTimeCalculation;
    }

    private void applyEnsembleConstraint() throws SQLException
    {
        if ( !this.getDataSourceConfig().getEnsemble().isEmpty() )
        {
            int includeCount = 0;
            int excludeCount = 0;
            StringJoiner include = new StringJoiner(",", "ANY('{", "}'::integer[])");
            StringJoiner exclude = new StringJoiner(",", "ANY('{", "}'::integer[])");

            for ( EnsembleCondition condition : this.getDataSourceConfig().getEnsemble())
            {
                if ( condition.isExclude() )
                {
                    excludeCount++;
                    exclude.add(String.valueOf( Ensembles.getEnsembleID( condition)));
                }
                else
                {
                    includeCount++;
                    include.add( String.valueOf( Ensembles.getEnsembleID( condition ) ) );
                }
            }

            if (includeCount > 0)
            {
                this.addLine( "    AND TS.ensemble_id = ", include.toString() );
            }

            if (excludeCount > 0)
            {
                this.addLine( "    AND NOT TS.ensemble_id = ", exclude.toString() );
            }
        }
    }

    private void applyProjectConstraint()
    {
        this.addLine( "    AND PS.project_id = ", this.getProjectDetails().getId() );
        this.addLine( "    AND PS.member = ", this.getMember());
    }

    private void applySourceConstraint()
    {
        this.addLine( "INNER JOIN wres.ForecastSource AS FS" );
        this.addLine( "    ON FS.forecast_id = TS.timeseries_id" );
        this.addLine( "INNER JOIN wres.ProjectSource AS PS" );
        this.addLine( "    ON PS.source_id = FS.source_id" );

        if (ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() ))
        {
            this.addLine( "INNER JOIN wres.Source S");
            this.addTab().addLine("ON S.source_id = PS.source_id");
            this.addTab( 2 )
                .addLine( "AND (S.lead IS NULL OR S.lead = FV.lead)" );
        }
    }

    private void applyGrouping()
    {
        this.addLine( "GROUP BY ", this.getBaseDateName(), ", FV.lead, TS.measurementunit_id" );
    }

    private void applyOrdering()
    {
        this.addLine("ORDER BY ", this.getBaseDateName(), ", lead");
    }

    private String validTimeCalculation;
}
