package wres.io.retrieval.left;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.UnitConversions;
import wres.io.data.caching.Variables;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.Collections;
import wres.util.TimeHelper;

class VectorCache implements LeftHandCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger( VectorCache.class );

    VectorCache(final ProjectDetails projectDetails, Feature feature) throws SQLException
    {
        this.projectDetails = projectDetails;
        this.feature = feature;
        this.values = new TreeMap<>(  );

        this.generateLeftHandData();
    }

    private void generateLeftHandData() throws SQLException
    {
        Integer desiredMeasurementUnitID =
            MeasurementUnits.getMeasurementUnitID( this.projectDetails
                                                       .getDesiredMeasurementUnit());

        ScriptBuilder script = new ScriptBuilder(  );

        DataSourceConfig left = this.projectDetails.getLeft();
        Integer leftVariableID = Variables.getVariableID( left);

        String earliestDate = this.projectDetails.getEarliestDate();
        String latestDate = this.projectDetails.getLatestDate();

        if (earliestDate != null)
        {
            earliestDate = "'" + earliestDate + "'";
        }

        if (latestDate != null)
        {
            latestDate = "'" + latestDate + "'";
        }

        String timeShift = null;

        String variablepositionClause = ConfigHelper.getVariablePositionClause( this.feature, leftVariableID, "");

        if (left.getTimeShift() != null)
        {
            timeShift = "'" + left.getTimeShift().getWidth() + " " + left.getTimeShift().getUnit().value() + "'";
        }

        script.add("SELECT (O.observation_time");

        if (timeShift != null)
        {
            script.add(" + ", timeShift);
        }

        script.addLine(") AS left_date,");
        script.addLine("     O.observed_value AS left_value,");
        script.addLine("     O.measurementunit_id");
        script.addLine("FROM wres.ProjectSource PS");
        script.addLine("INNER JOIN wres.Observation O");
        script.addLine("     ON O.source_id = PS.source_id");
        script.addLine("WHERE PS.project_id = ", this.projectDetails.getId());
        script.addLine("     AND PS.member = 'left'");
        script.addLine("     AND ", variablepositionClause);

        if (earliestDate != null)
        {
            script.add("     AND O.observation_time");

            if (timeShift != null)
            {
                script.add(" + INTERVAL '1 hour' * ", timeShift);
            }

            script.addLine(" >= ", earliestDate);
        }

        if (latestDate != null)
        {
            script.add("     AND O.observation_time");

            if (timeShift != null)
            {
                script.add(" + INTERVAL '1 hour' * ", timeShift);
            }

            script.addLine(" <= ", latestDate);
        }

        script.add(";");

        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults(connection, script.toString());

            while(resultSet.next())
            {
                LocalDateTime date = TimeHelper.convertStringToDate(
                        resultSet.getString( "left_date" ),
                        LocalDateTime::from);
                Double measurement = Database.getValue( resultSet, "left_value" );

                int unitID = Database.getValue( resultSet, "measurementunit_id" );

                if ( unitID != desiredMeasurementUnitID
                     && measurement != null )
                {
                    measurement = UnitConversions.convert( measurement,
                                                           unitID,
                                                           this.projectDetails
                                                               .getDesiredMeasurementUnit());
                }

                if (measurement != null && measurement < this.projectDetails.getMinimumValue())
                {
                    measurement = this.projectDetails.getDefaultMinimumValue();
                }
                else if (measurement != null && measurement > this.projectDetails.getMaximumValue())
                {
                    measurement = this.projectDetails.getDefaultMaximumValue();
                }
                else if (measurement == null)
                {
                    measurement = Double.NaN;
                }

                if (measurement != null)
                {
                    this.values.put( date, measurement );
                }
            }
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch (SQLException closeError)
                {
                    LOGGER.debug("The set of retrieved values could not be closed after reading.", closeError);
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }

    private final ProjectDetails projectDetails;
    private final NavigableMap<LocalDateTime, Double> values;
    private final Feature feature;

    @Override
    public List<Double> getLeftValues( Feature feature,
                                       LocalDateTime earliestDate,
                                       LocalDateTime latestDateTime )
    {
        return Collections.getValuesInRange(this.values, earliestDate, latestDateTime);
    }
}
