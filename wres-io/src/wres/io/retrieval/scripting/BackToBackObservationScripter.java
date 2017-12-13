package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.Collections;
import wres.util.Time;

class BackToBackObservationScripter extends Scripter
{
    protected BackToBackObservationScripter( ProjectDetails projectDetails,
                                             DataSourceConfig dataSourceConfig,
                                             Feature feature,
                                             int progress )
    {
        super( projectDetails, dataSourceConfig, feature, progress );
    }

    @Override
    String formScript() throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        this.addLine("SELECT ARRAY[O.observed_value] AS measurements,");

        this.applyValueDate();

        this.addLine(
                "    (EXTRACT(epoch FROM O.observation_time - ",
                this.getZeroDate(),
                ")/3600):: int % ",
                this.getWindowPeriod(),
                " AS agg_hour,"
        );

        this.addLine("    O.measurementunit_id");
        this.addLine("FROM wres.Observation O");

        this.applyVariablePositionClause();
        this.applySourceConstraint();

        this.applyEarliestDateConstraint();
        this.applyLatestDateConstraint();

        this.applySeasonConstraint();

        return null;
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
            this.windowPeriod = Time.unitsToHours(
                    this.getProjectDetails().getAggregationUnit(),
                    this.getProjectDetails().getAggregationPeriod()
            ).intValue();
        }
        return this.windowPeriod;
    }

    private void applySourceConstraint() throws SQLException
    {
        List<Integer> sourceIds;

        if (this.getProjectDetails().getLeft().equals(this.getDataSourceConfig()))
        {
            sourceIds = this.getProjectDetails().getLeftSources();
        }
        else if (this.getProjectDetails().getRight().equals(this.getDataSourceConfig()))
        {
            sourceIds = this.getProjectDetails().getRightSources();
        }
        else
        {
            sourceIds = this.getProjectDetails().getBaselineSources();
        }

        this.addLine( "    AND O.source_id = ",
                      Collections.formAnyStatement(sourceIds, "int"));
    }

    @Override
    String getBaseDateName()
    {
        return "O.observation_date";
    }

    @Override
    String getValueDate()
    {
        return this.getBaseDateName();
    }

    private String zeroDate;
    private Integer windowPeriod;
}
