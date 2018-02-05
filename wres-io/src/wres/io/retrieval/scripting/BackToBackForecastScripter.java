package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.TimeScaleConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.details.ProjectDetails;
import wres.util.TimeHelper;

class BackToBackForecastScripter extends Scripter
{
    protected BackToBackForecastScripter( ProjectDetails projectDetails,
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
        this.applyScaleMember();
        this.addLine( "    FV.lead,");
        this.addLine( "    ARRAY_AGG(FV.forecasted_value ORDER BY TS.ensemble_id) AS measurements," );
        this.addLine( "    TS.measurementunit_id" );
        this.addLine( "FROM wres.TimeSeries TS" );
        this.addLine( "INNER JOIN wres.ForecastValue FV");
        this.addLine( "    ON FV.timeseries_id = TS.timeseries_id" );

        this.addLine( "INNER JOIN wres.ForecastSource AS FS" );
        this.addLine( "    ON FS.forecast_id = TS.timeseries_id" );
        this.addLine( "INNER JOIN wres.ProjectSource AS PS" );
        this.addLine( "    ON PS.source_id = FS.source_id" );

        // TODO: move wres.Source.lead to wres.ForecastSource to avoid the extremely expensive join to wres.Source
        // Source is needed due to NWM having the lead information
        this.addLine( "INNER JOIN wres.Source as S" );
        this.addLine( "    ON S.source_id = PS.source_id" );
        // S.lead IS NULL when using "a forecast came from a file", whereas
        // S.lead = FV.lead when using "a forecast came from several files"
        this.addLine( "        AND ( S.lead IS NULL OR S.lead = FV.lead )" );

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

    private int getLeadOffset() throws SQLException, IOException
    {
        if (this.leadOffset == null)
        {
            this.leadOffset = this.getProjectDetails().getLeadOffset( this.getFeature() );
        }
        return this.leadOffset;
    }

    private void applyLeadQualifier() throws SQLException, IOException
    {
        this.addLine(
                "    AND ",
                ConfigHelper.getLeadQualifier(
                        this.getProjectDetails(),
                        this.getProgress(),
                        this.getLeadOffset()
                )
        );
    }

    private void applyScaleMember() throws IOException, SQLException
    {
        TimeScaleConfig timeScaleConfig = this.getProjectDetails().getScale();

        this.add("    ");

        if ( !this.getProjectDetails().shouldScale())
        {
            this.add("FV.lead");
        }
        else
        {
            Integer firstLead = this.getProgress() * this.getProjectDetails().getLeadFrequency() + 1;
            firstLead += this.getLeadOffset();
            this.add( "(FV.lead - ",
                      firstLead,
                      ") % ");
            this.add( TimeHelper.unitsToLeadUnits( this.getProjectDetails()
                                                       .getScale().getUnit().value(),
                                                   this.getProjectDetails()
                                                       .getScale().getPeriod() ) );
        }
        this.addLine(" AS scale_member,");
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

    private void applyGrouping()
    {
        this.addLine( "GROUP BY ", this.getBaseDateName(), ", FV.lead, TS.measurementunit_id" );
    }

    private void applyOrdering()
    {
        this.addLine("ORDER BY ", this.getBaseDateName(), ", lead, scale_member");
    }

    private String validTimeCalculation;
    private Integer leadOffset;
}
