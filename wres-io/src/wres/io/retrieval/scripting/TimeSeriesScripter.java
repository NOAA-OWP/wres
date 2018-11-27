package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.util.CalculationException;
import wres.util.Strings;

class TimeSeriesScripter extends Scripter
{
    TimeSeriesScripter( OrderedSampleMetadata sampleMetadata,
                        DataSourceConfig dataSourceConfig)
    {
        super( sampleMetadata, dataSourceConfig );
    }

    @Override
    String formScript() throws SQLException, IOException
    {
        this.addLine( "-- ", this.getSampleMetadata() );
        this.add("SELECT ");
        this.applyValueDate();
        this.applyBasisTime();
        this.addTab().addLine( "TSV.lead,");
        this.addTab().addLine( "ARRAY_AGG(TSV.series_value ORDER BY TS.ensemble_id) AS measurements," );
        this.addTab().addLine( "TS.measurementunit_id" );
        this.addLine("FROM (");

        try
        {
            this.applyTimeSeriesSelect();
        }
        catch ( CalculationException e )
        {
            throw new IOException( "The logic used to select time series could not be formed.", e );
        }

        this.addLine(") AS TS");
        this.addLine( "INNER JOIN wres.TimeSeriesValue TSV");
        this.addLine( "    ON TSV.timeseries_id = TS.timeseries_id" );

        this.applyLeadQualifier();

        this.applyEarliestDateConstraint();
        this.applyLatestDateConstraint();
        this.applyEarliestIssueDateConstraint();
        this.applyLatestIssueDateConstraint();

        this.applyEnsembleConstraint();

        this.applySeasonConstraint();

        this.applyGrouping();
        this.applyOrdering();

        return this.getScript();
    }

    @Override
    String getBaseDateName()
    {
        return "TS.initialization_date";
    }

    private void applyTimeSeriesSelect() throws SQLException, CalculationException
    {
        this.addTab().add("SELECT TS.initialization_date, TS.ensemble_id, TS.timeseries_id, TS.measurementunit_id, ");

        if (this.usesNetCDF())
        {
            this.addLine("E.ensemble_name, E.qualifier_id");
        }
        else
        {
            this.addLine("TSS.source_id");
        }

        this.addTab().addLine("FROM wres.TimeSeries TS");

        if (this.usesNetCDF())
        {
            this.addTab().addLine("INNER JOIN wres.Ensemble E");
            this.addTab(  2  ).addLine("ON E.ensemble_id = TS.ensemble_id");
        }
        else
        {
            this.addTab().addLine("INNER JOIN wres.TimeSeriesSource TSS");
            this.addTab(  2  ).addLine("ON TSS.timeseries_id = TS.timeseries_id");
        }

        this.addTab().addLine("WHERE ", this.getVariableFeatureClause());

        // Handles the case when every value lies on the exact initialization date
        if (this.getForecastLag() != 0)
        {
            this.addTab( 2 ).addLine(
                                      "AND TS.initialization_date >= '",
                                      this.getSampleMetadata().getTimeWindow().getEarliestReferenceTime(),
                                      "'" );
            this.addTab( 2 ).addLine(
                                      "AND TS.initialization_date < '",
                                      this.getSampleMetadata().getTimeWindow().getLatestReferenceTime(),
                                      "'" );
        }

        this.addTab(  2  ).addLine("AND EXISTS (");
        this.addTab(   3   ).addLine( "SELECT 1");
        this.addTab(   3   ).addLine( "FROM wres.ProjectSource PS");

        if (this.usesNetCDF())
        {
            this.addTab(   3   ).addLine("INNER JOIN wres.TimeSeriesSource TSS" );
            this.addTab(    4    ).addLine( "ON TSS.source_id = PS.source_id");
            this.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
            this.addTab(    4    ).addLine( "AND PS.member = ", this.getProjectDetails().getInputName( this.getDataSourceConfig() ));
            this.addTab(    4    ).addLine( "AND TSS.timeseries_id = TS.timeseries_id");
        }
        else
        {
            this.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
            this.addTab(    4    ).addLine( "AND PS.member = ", this.getProjectDetails().getInputName( this.getDataSourceConfig() ));
            this.addTab(    4    ).addLine( "AND PS.source_id = TSS.source_id");
        }

        this.addTab(  2  ).addLine(")");
    }

    private void applyLeadQualifier() throws SQLException, IOException
    {
        if (this.getProjectDetails().getMaximumLead() < Integer.MAX_VALUE)
        {
            this.addTab();
            this.addWhere();
            this.addLine("TSV.lead <= ", this.getProjectDetails().getMaximumLead());
        }

        if (this.getProjectDetails().getMinimumLead() > Integer.MIN_VALUE)
        {
            this.addTab();
            this.addWhere();
            this.addLine("TSV.lead >= ", this.getProjectDetails().getMinimumLead());
        }
    }

    @Override
    protected void applyEarliestDateConstraint() throws SQLException
    {
        if (this.getProjectDetails().getEarliestDate() != null)
        {
            this.addTab();
            this.addWhere();
            this.add(this.getValueDate());
            this.applyTimeShift();
            this.addLine(" >= '", this.getProjectDetails().getEarliestDate(), "'");
        }
    }

    @Override
    protected void applyLatestDateConstraint()
    {
        if (this.getProjectDetails().getLatestDate() != null)
        {
            this.addTab();
            this.addWhere();
            this.add(this.getValueDate());
            this.applyTimeShift();
            this.addLine(" <= '", this.getProjectDetails().getLatestDate(), "'");
        }
    }

    @Override
    protected void applySeasonConstraint()
    {
        String constraint = ConfigHelper.getSeasonQualifier( this.getProjectDetails(), this.getBaseDateName(), this.getTimeShift() );

        if ( Strings.hasValue(constraint))
        {
            this.addTab();

            if (!this.whereAdded)
            {
                // The season qualifier hardcodes the "and" keyword. If this
                // ends up being the first where clause, this needs to start with
                // "WHERE", not "AND".
                constraint = constraint.replaceFirst( "^\\s+(and|AND)", "WHERE" );
            }

            this.add( constraint );
        }
        super.applySeasonConstraint();
    }

    private void applyBasisTime()
    {
        this.addTab().addLine("EXTRACT(epoch FROM TS.initialization_date)::bigint AS basis_epoch_time,");
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

    private void applyGrouping()
    {
        this.add("GROUP BY ");

        if (this.usesNetCDF())
        {
            this.add("TS.ensemble_name, TS.qualifier, ");
        }
        else
        {
            this.add("TS.source_id, ");
        }
        this.addLine(this.getBaseDateName(), ", TSV.lead, TS.measurementunit_id");
    }

    private void applyOrdering()
    {
        this.add("ORDER BY ", this.getBaseDateName(), ", ");

        if (this.usesNetCDF())
        {
            this.add("TS.ensemble_name, TS.qualifier, ");
        }
        else
        {
            this.add("TS.source_id, ");
        }
        this.addLine("lead");
    }

    private boolean usesNetCDF()
    {
        return ConfigHelper.usesNetCDFData( this.getProjectDetails().getProjectConfig() );
    }

    private String getMinimumForecastDate() throws SQLException
    {
        return this.getProjectDetails().getInitialForecastDate( this.getDataSourceConfig(), this.getFeature() );
    }

    private Integer getForecastLag() throws CalculationException
    {
        return this.getProjectDetails().getForecastLag( this.getDataSourceConfig(), this.getFeature() );
    }

    private void addWhere()
    {
        if (this.whereAdded)
        {
            this.add("AND ");
        }
        else
        {
            this.whereAdded = true;
            this.add("WHERE ");
        }
    }

    private String validTimeCalculation;
    private boolean whereAdded = false;
}
