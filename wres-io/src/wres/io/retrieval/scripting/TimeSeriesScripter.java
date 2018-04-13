package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.details.ProjectDetails;
import wres.util.Strings;

class TimeSeriesScripter extends Scripter
{
    TimeSeriesScripter( ProjectDetails projectDetails,
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
        this.applyBasisTime();
        this.addTab().addLine( "FV.lead,");
        this.addTab().addLine( "ARRAY_AGG(FV.forecasted_value ORDER BY TS.ensemble_id) AS measurements," );
        this.addTab().addLine( "TS.measurementunit_id" );
        this.addLine("FROM (");
        this.applyTimeSeriesSelect();
        this.addLine(") AS TS");
        this.addLine( "INNER JOIN wres.ForecastValue FV");
        this.addLine( "    ON FV.timeseries_id = TS.timeseries_id" );

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

    private void applyTimeSeriesSelect() throws SQLException
    {
        this.addTab().add("SELECT TS.initialization_date, TS.ensemble_id, TS.timeseries_id, TS.measurementunit_id, ");

        if (this.usesNetCDF())
        {
            this.addLine("E.ensemble_name, E.qualifier_id");
        }
        else
        {
            this.addLine("FS.source_id");
        }

        this.addTab().addLine("FROM wres.TimeSeries TS");

        if (this.usesNetCDF())
        {
            this.addTab().addLine("INNER JOIN wres.Ensemble E");
            this.addTab(  2  ).addLine("ON E.ensemble_id = TS.ensemble_id");
        }
        else
        {
            this.addTab().addLine("INNER JOIN wres.ForecastSource FS");
            this.addTab(  2  ).addLine("ON FS.forecast_id = TS.timeseries_id");
        }

        this.addTab().addLine("WHERE ", this.getVariablePositionClause());

        // Handles the case when every value lies on the exact initialization date
        if (this.getForecastLag() != 0)
        {
            this.addTab( 2 ).addLine(
                    "AND TS.initialization_date >= ",
                    this.getMinimumForecastDate(),
                    "::timestamp without time zone + (INTERVAL '",
                    this.getForecastLag(),
                    " HOUR') * ",
                    this.getProjectDetails().getNumberOfSeriesToRetrieve()
                    * this.getSequenceStep()
            );
            this.addTab( 2 ).addLine(
                    "AND TS.initialization_date < ",
                    this.getMinimumForecastDate(),
                    "::timestamp without time zone + (INTERVAL '",
                    this.getForecastLag(),
                    " HOUR') * ",
                    this.getProjectDetails().getNumberOfSeriesToRetrieve() * (
                            this.getSequenceStep() + 1 ) );
        }

        this.addTab(  2  ).addLine("AND EXISTS (");
        this.addTab(   3   ).addLine( "SELECT 1");
        this.addTab(   3   ).addLine( "FROM wres.ProjectSource PS");

        if (this.usesNetCDF())
        {
            this.addTab(   3   ).addLine("INNER JOIN wres.ForecastSource FS" );
            this.addTab(    4    ).addLine( "ON FS.source_id = PS.source_id");
            this.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
            this.addTab(    4    ).addLine( "AND PS.member = ", this.getProjectDetails().getInputName( this.getDataSourceConfig() ));
            this.addTab(    4    ).addLine( "AND FS.forecast_id = TS.timeseries_id");
        }
        else
        {
            this.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
            this.addTab(    4    ).addLine( "AND PS.member = ", this.getProjectDetails().getInputName( this.getDataSourceConfig() ));
            this.addTab(    4    ).addLine( "AND PS.source_id = FS.source_id");
        }

        this.addTab(  2  ).addLine(")");
    }

    private void applyLeadQualifier() throws SQLException, IOException
    {
        if (this.getProjectDetails().getMaximumLeadHour() < Integer.MAX_VALUE)
        {
            this.addTab();
            this.addWhere();
            this.addLine("FV.lead <= ", this.getProjectDetails().getMaximumLeadHour());
        }

        if (this.getProjectDetails().getMinimumLeadHour() > Integer.MIN_VALUE)
        {
            this.addTab();
            this.addWhere();
            this.addLine("FV.lead >= ", this.getProjectDetails().getMinimumLeadHour());
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
    int getSequenceStep()
    {
        return super.getSequenceStep() - 1;
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
                this.addTab();
                this.addWhere();
                this.addLine( "TS.ensemble_id = ", include.toString() );
            }

            if (excludeCount > 0)
            {
                this.addTab();
                this.addWhere();
                this.addLine( "NOT TS.ensemble_id = ", exclude.toString() );
            }
        }
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
        this.addLine(this.getBaseDateName(), ", FV.lead, TS.measurementunit_id");
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

    private Integer getForecastLag() throws SQLException
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
