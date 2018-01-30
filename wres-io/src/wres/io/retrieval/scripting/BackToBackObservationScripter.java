package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.TimeHelper;

class BackToBackObservationScripter extends Scripter
{
    protected BackToBackObservationScripter( ProjectDetails projectDetails,
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
        this.addLine("SELECT ARRAY[O.observed_value] AS measurements,");

        this.applyValueDate();

        // EXTRACT(epoch FROM O.observation_time - {zero_date})/3600
        // will yield the number of hours between the observation time and the
        // zero date. If you mod that by the length of the period, you
        // will reveal each member from the aggregate group. If the window
        // period is one, they will all have the same number. Increment that
        // and you'll start getting agg_hours of 0, 1, 2, 3, etc. If the
        // highest agg_hour is 3, you'll know that you want to group values
        // with agg_hours 0, 1, 2, and 3 together. Once you reach the next 0
        // or the end of the result set, you will group those together for
        // aggregation (like finding the average of each) and reset the
        // grouping of values for the next set to aggregate
        this.addLine(
                "    (EXTRACT(epoch FROM O.observation_time - ",
                this.getZeroDate(),
                ")/3600)::int % ",
                this.getWindowPeriod(),
                " AS agg_hour,"
        );

        this.addLine("    O.measurementunit_id");
        this.addLine("FROM wres.Observation O");

        this.applyVariablePositionClause();

        this.applyEarliestDateConstraint();
        this.applyLatestDateConstraint();

        this.applySeasonConstraint();
        this.applyProjectConstraint();
        return this.getScript();
    }

    protected void applyProjectConstraint()
    {
        this.addLine( "    AND EXISTS (" );
        this.addLine( "        SELECT 1" );
        this.addLine( "        FROM wres.ProjectSource PS");
        this.addLine( "        WHERE PS.project_id = ", this.getProjectDetails().getId());
        this.addLine( "            AND PS.member = ", this.getMember());
        this.addLine( "            AND PS.source_id = O.source_id");
        this.addLine( "    )");
    }

    @Override
    protected void applyEarliestDateConstraint() throws SQLException
    {
        this.add( "    AND ", this.getZeroDate(), " <= ", this.getBaseDateName() );
        this.applyTimeShift();
        this.addLine();
    }

    private String getZeroDate() throws SQLException
    {
        if (this.zeroDate == null)
        {
            this.zeroDate = this.getProjectDetails().getZeroDate(
                    this.getDataSourceConfig(),
                    this.getFeature()
            );
        }
        return this.zeroDate;
    }

    private Integer getWindowPeriod() throws InvalidPropertiesFormatException
    {
        if (this.windowPeriod == null)
        {
            this.windowPeriod = TimeHelper.unitsToLeadUnits(
                    this.getProjectDetails().getAggregationUnit(),
                    this.getProjectDetails().getAggregationPeriod()
            ).intValue();
        }
        return this.windowPeriod;
    }

    @Override
    String getBaseDateName()
    {
        return "O.observation_time";
    }

    @Override
    String getValueDate()
    {
        return this.getBaseDateName();
    }

    private String zeroDate;
    private Integer windowPeriod;
}
