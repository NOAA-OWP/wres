package wres.io.retrieval;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.util.Collections;

final class BackToBackMetricInputIterator extends MetricInputIterator
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( BackToBackMetricInputIterator.class );

    @Override
    void createLeftHandCache() throws SQLException, NoDataException
    {
        Integer desiredMeasurementUnitID =
                MeasurementUnits.getMeasurementUnitID( this.getProjectDetails()
                                                           .getDesiredMeasurementUnit());

        DataSourceConfig left = this.getLeft();
        StringBuilder script = new StringBuilder();
        Integer leftVariableID = ConfigHelper.getVariableID(left);

        String earliestDate = this.getProjectDetails().getEarliestDate();
        String latestDate = this.getProjectDetails().getLatestDate();

        if (earliestDate != null)
        {
            earliestDate = "'" + earliestDate + "'";
        }

        if (latestDate != null)
        {
            latestDate = "'" + latestDate + "'";
        }

        Integer timeShift = null;

        String variablepositionClause = ConfigHelper.getVariablePositionClause(this.getFeature(), leftVariableID, "");

        if (left.getTimeShift() != null && left.getTimeShift().getWidth() != 0)
        {
            timeShift = left.getTimeShift().getWidth();
        }

        if (ConfigHelper.isForecast(left))
        {
            List<Integer> forecastIDs = this.getProjectDetails().getLeftForecastIDs();

            if (forecastIDs.size() == 0)
            {
                throw new NoDataException("There is no forecast data that " +
                                          "can be loaded for comparison " +
                                          "purposes. Please supply new " +
                                          "data or adjust the project " +
                                          "specifications.");
            }

            // TODO: This will be a mess if we don't have the ability to select "Assim data" rather than all
            script.append("SELECT ");
            if (left.getExistingTimeAggregation() != null) {
                script.append(left.getExistingTimeAggregation().getFunction());
            }
            else
            {
                // Default is the average - will not change if there is only one value
                script.append("AVG");
            }
            script.append("(FV.forecasted_value) AS left_value,").append(NEWLINE);
            script.append("     TS.measurementunit_id,").append(NEWLINE);
            script.append("     (TS.initialization_date + INTERVAL '1 hour' * FV.lead");

            if (timeShift != null)
            {
                script.append(" + INTERVAL '1 hour' * ").append(timeShift);
            }

            script.append(")::text AS left_date").append(NEWLINE);

            script.append("FROM wres.TimeSeries TS").append(NEWLINE);
            script.append("INNER JOIN wres.ForecastValue FV" ).append(NEWLINE);
            script.append("     ON FV.timeseries_id = TS.timeseries_id").append(NEWLINE);
            script.append("WHERE TS.timeseries_id = ")
                  .append( Collections.formAnyStatement(
                          this.getProjectDetails().getLeftForecastIDs(),
                          "int" ))
                  .append(NEWLINE);

            script.append("GROUP BY TS.initialization_date + INTERVAL '1 hour' * FV.lead");

            if (timeShift != null)
            {
                script.append(" + INTERVAL '1 hour' * ").append(timeShift);
            }

            script.append(", TS.measurementunit_id");
        }
        else
        {
            script.append("SELECT (O.observation_time");

            if (timeShift != null)
            {
                script.append(" + INTERVAL '1 hour' * ").append(timeShift);
            }

            script.append(")::text AS left_date,").append(NEWLINE);
            script.append("     O.observed_value AS left_value,").append(NEWLINE);
            script.append("     O.measurementunit_id").append(NEWLINE);
            script.append("FROM wres.ProjectSource PS").append(NEWLINE);
            script.append("INNER JOIN wres.Observation O").append(NEWLINE);
            script.append("     ON O.source_id = PS.source_id").append(NEWLINE);
            script.append("WHERE PS.project_id = ")
                  .append(this.getProjectDetails().getId())
                  .append(NEWLINE);
            script.append("     AND PS.member = 'left'").append(NEWLINE);
            script.append("     AND ").append(variablepositionClause).append(NEWLINE);

            if (earliestDate != null)
            {
                script.append("     AND O.observation_time");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(" >= ").append(earliestDate).append(NEWLINE);
            }

            if (latestDate != null)
            {
                script.append("     AND O.observation_time");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(" <= ").append(latestDate).append(NEWLINE);
            }
        }

        script.append(";");

        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults(connection, script.toString());

            while(resultSet.next())
            {
                String date = Database.getValue( resultSet, "left_date" );
                Double measurement = Database.getValue( resultSet, "left_value" );

                int unitID = Database.getValue( resultSet, "measurementunit_id" );

                if ( unitID != desiredMeasurementUnitID
                     && measurement != null )
                {
                    measurement = UnitConversions.convert( measurement,
                                                           unitID,
                                                           this.getProjectDetails()
                                                               .getDesiredMeasurementUnit());
                }

                if (measurement == null ||
                    ( measurement >= this.getProjectDetails().getMinimumValue() &&
                      measurement <= this.getProjectDetails().getMaximumValue() ))
                {
                    this.addLeftHandValue( date, measurement );
                }
            }
        }
        finally
        {
            if (resultSet != null)
            {
                resultSet.close();
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }

    @Override
    Logger getLogger()
    {
        return BackToBackMetricInputIterator.LOGGER;
    }

    BackToBackMetricInputIterator( ProjectConfig projectConfig,
                                   Feature feature,
                                   ProjectDetails projectDetails )
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        super( projectConfig, feature, projectDetails );
    }

    @Override
    int calculateWindowCount()
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        int count;
        if ( ConfigHelper.isForecast( this.getRight() ))
        {
            int start = this.getFirstLeadInWindow();
            Integer last = this.getProjectDetails().getLastLead( this.getFeature() );

            if (last == null)
            {
                throw new IllegalArgumentException( "The final lead time for the data set for: " +
                                                    this.getRight()
                                                        .getVariable()
                                                        .getValue() +
                                                    " could not be determined.");
            }
            else if (start >= last)
            {
                throw new NoDataException( "No data can be retrieved because " +
                                           "the first requested lead time " +
                                           "(" + String.valueOf(start) +
                                           ") is greater than or equal to " +
                                           "the largest possible lead time (" +
                                           String.valueOf(last) + ")." );
            }

            double windowWidth = this.getProjectDetails().getWindowWidth();
            double windowSpan = (double)(last - start);

            count = ((Double)Math.ceil( windowSpan / windowWidth)).intValue();
        }
        else
        {
            count = 1;
        }

        return count;
    }
}
