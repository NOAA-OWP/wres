package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;

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
    String formScript() throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        this.add("SELECT ");
        this.applyValueDate();
        this.addLine("    FV.lead - ",
                     this.getProjectDetails().getLead( this.getProgress() - 1 ) +
                     this.getLeadOffset(),
                     " AS agg_hour,");
        this.addLine( "    ARRAY_AGG(FV.forecasted_value ORDER BY TS.ensemble_id) AS measurements," );
        this.addLine( "    TS.measurementunit_id" );
        this.addLine( "FROM wres.TimeSeries TS" );
        this.addLine( "INNER JOIN wres.ForecastValue FV");
        this.addLine( "    ON FV.timeseries_id = TS.timeseries_id" );

        this.applyVariablePositionClause();
        this.applyLeadQualifier();

        this.applyEarliestDateConstraint();
        this.applyLatestDateConstraint();
        this.applyEarliestIssueDateConstraint();
        this.applyLatestIssueDateConstraint();

        this.applyEnsembleConstraint();

        this.applySeasonConstraint();

        this.applyProjectExistenceCheck();
        this.applyGrouping();
        this.applyOrdering();

        return this.getScript();
    }

    @Override
    String getBaseDateName()
    {
        return "TS.initialization_date";
    }

    private int getLeadOffset() throws NoDataException, SQLException,
            InvalidPropertiesFormatException
    {
        if (this.leadOffset == null)
        {
            this.leadOffset = this.getProjectDetails().getLeadOffset( this.getFeature() );
        }
        return this.leadOffset;
    }

    private void applyLeadQualifier() throws SQLException, NoDataException,
            InvalidPropertiesFormatException
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

    @Override
    protected String getValueDate()
    {
        if (this.validTimeCalculation == null)
        {
            this.validTimeCalculation = this.getBaseDateName() +
                                        " + INTERVAL '1 HOUR' * lead";
        }
        return this.validTimeCalculation;
    }

    private void applyEnsembleConstraint() throws SQLException
    {
        if (this.getDataSourceConfig().getEnsemble() != null &&
            this.getDataSourceConfig().getEnsemble().size() > 0)
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

    private void applyProjectExistenceCheck()
    {
        this.addLine("    AND EXISTS (");
        this.addLine("        SELECT 1");
        this.addLine("        FROM wres.ProjectSource PS");
        this.addLine("        INNER JOIN wres.ForecastSource FS");
        this.addLine("            ON FS.source_id = PS.source_id");
        this.addLine("        WHERE PS.project_id = ", this.getProjectDetails().getId());
        this.addLine("            AND PS.member = ", this.getMember());
        this.addLine("            AND FS.forecast_id = TS.timeseries_id");
        this.addLine("    )");
    }

    private void applyGrouping()
    {
        this.addLine( "GROUP BY ", this.getBaseDateName(), ", FV.lead, TS.measurementunit_id" );
    }

    private void applyOrdering()
    {
        this.addLine("ORDER BY ", this.getBaseDateName(), ", agg_hour");
    }

    private String validTimeCalculation;
    private Integer leadOffset;
}
