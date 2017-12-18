package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.data.caching.Features;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.Time;

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
        this.addLine("WITH forecasts AS");
        this.addLine("(");
        this.addLine("    SELECT F.basis_time,");
        this.addLine("        F.lead,");
        this.addLine("        F.forecasted_value,");
        this.addLine("        F.ensemble_id,");
        this.addLine("        F.measurementunit_id");
        this.addLine("    FROM wres.Forecasts F");
        this.add("    WHERE F.variable_id = ", this.getVariableID());

        Double frequency = Time.unitsToHours( this.getProjectDetails().getAggregationUnit(), this.getProjectDetails().getAggregation().getFrequency() );
        Double halfSpan = Time.unitsToHours( this.getProjectDetails().getAggregationUnit(), this.getProjectDetails().getAggregation().getSpan() ) / 2;

        this.addLine( "       AND F.feature_id = ", Features.getFeatureID(this.getFeature()));
        this.addLine("        AND F.lead > ", this.getProgress() * this.getProjectDetails().getAggregationPeriod());
        this.addLine( "       AND F.lead <= ", (this.getProgress() + 1) * this.getProjectDetails().getAggregationPeriod() );
        this.add( "        AND F.basis_time >= '", this.getInitialRollingDate(), "'::timestep without time zone + (INTERVAL '1 HOUR' * ");
        this.add( frequency);
        this.addLine(") * ", this.getSequenceStep(), ") - INTERVAL '", halfSpan, " HOUR'");
        this.add( "        AND F.basis_time <= ('", this.getInitialRollingDate(), "'" );
        this.add("'::timestamp without time zone + (INTERVAL '1 HOUR' * ", frequency, ") * ", this.getSequenceStep(), ") ");
        this.addLine( "INTERVAL '", halfSpan, " HOUR'");
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
        this.addLine("SELECT F.lead,");
        this.applyValueDate();
        this.addLine("    ARRAY_AGG(F.forecasted_value ORDER BY F.ensemble_id) AS measurements,");
        this.addLine("    F.measurementunit_id");
        this.addLine( "FROM forecasts F" );
        this.addLine( "GROUP BY F.basis_time, F.lead, F.measurementunit_id" );
        this.addLine( "ORDER BY F.basis_time, F.lead;" );

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
