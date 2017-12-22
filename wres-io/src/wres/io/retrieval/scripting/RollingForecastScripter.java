package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.TimeHelper;

class RollingForecastScripter extends Scripter
{
    protected RollingForecastScripter( ProjectDetails projectDetails,
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
        // TODO: Break out into separate functions
        this.addLine("WITH forecasts AS");
        this.addLine("(");
        this.addLine("    SELECT F.basis_time,");
        this.addLine( "       F.valid_time,");
        this.addLine("        F.lead,");
        this.addLine("        F.forecasted_value,");
        this.addLine("        F.ensemble_id,");
        this.addLine("        F.measurementunit_id");
        this.addLine("    FROM wres.Forecasts F");
        this.addLine("    WHERE F.variable_id = ", this.getVariableID());

        Double frequency = TimeHelper.unitsToHours( this.getProjectDetails().getAggregationUnit(), this.getProjectDetails().getAggregation().getFrequency() );
        Double halfSpan = TimeHelper.unitsToHours( this.getProjectDetails().getAggregationUnit(), this.getProjectDetails().getAggregation().getSpan() ) / 2;

        this.addLine( "        AND F.feature_id = ", Features.getFeatureID(this.getFeature()));
        this.addLine( "        AND F.lead > ", this.getProgress() );
        this.add( "        AND F.lead <= ");
        this.addLine(
                this.getProgress() +
                TimeHelper.unitsToHours(
                        this.getProjectDetails().getAggregationUnit(),
                        this.getProjectDetails().getAggregationPeriod()
                )
        );

        // TODO: Update this to handle left and right focused windows
        this.add( "        AND F.basis_time >= ('", this.getInitialRollingDate(), "'::timestamp without time zone + (INTERVAL '1 HOUR' * ");
        this.add( TimeHelper.unitsToHours( getProjectDetails().getAggregationUnit(), frequency ) );
        this.addLine(" ) * ", this.getSequenceStep(), ") - INTERVAL '", halfSpan, " HOUR'");
        this.add( "        AND F.basis_time <= ('", this.getInitialRollingDate() );
        this.add("'::timestamp without time zone + (INTERVAL '1 HOUR' * ", frequency, ") * ", this.getSequenceStep(), ") ");
        this.addLine( "+ INTERVAL '", halfSpan, " HOUR'");

        this.applyEnsembleConstraint();

        this.addLine( "        AND EXISTS (" );
        this.addLine( "            SELECT 1");
        this.addLine( "            FROM wres.ForecastSource FS");
        this.addLine( "            INNER JOIN wres.ProjectSource PS" );
        this.addLine( "                ON PS.source_id = FS.source_id" );
        this.addLine( "            WHERE PS.project_id = ", this.getProjectDetails().getId() );
        this.addLine( "                AND PS.member = ", this.getMember() );
        this.addLine( "                AND FS.forecast_id = F.timeseries_id" );
        this.addLine( "        )" );
        this.addLine(")");
        this.addLine("SELECT F.lead AS agg_hour,");
        this.applyValueDate();
        this.addLine("    ARRAY_AGG(F.forecasted_value ORDER BY F.ensemble_id) AS measurements,");
        this.addLine("    F.measurementunit_id");
        this.addLine( "FROM forecasts F" );
        this.addLine( "GROUP BY F.valid_time, F.lead, F.measurementunit_id" );
        this.addLine( "ORDER BY F.valid_time, F.lead;" );

        return this.getScript();
    }

    private String getInitialRollingDate()
            throws SQLException, InvalidPropertiesFormatException
    {
        if (this.zeroDate == null)
        {
            this.zeroDate = this.getProjectDetails().getInitialRollingDate( this.getFeature() );
        }
        return this.zeroDate;
    }

    private void applyEnsembleConstraint() throws SQLException
    {
        if (this.getDataSourceConfig().getEnsemble() != null &&
            this.getDataSourceConfig().getEnsemble().size() > 0)
        {
            int includeCount = 0;
            int excludeCount = 0;
            StringJoiner
                    include = new StringJoiner( ",", "ANY('{", "}'::integer[])");
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
                this.addLine( "    AND F.ensemble_id = ", include.toString() );
            }

            if (excludeCount > 0)
            {
                this.addLine( "    AND NOT F.ensemble_id = ", exclude.toString() );
            }
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

    private String zeroDate;
}
