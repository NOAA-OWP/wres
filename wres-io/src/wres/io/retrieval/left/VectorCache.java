package wres.io.retrieval.left;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
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
import wres.io.project.Project;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.util.Collections;

/**
 * Cache for storing point based observation data used for comparisons
 */
class VectorCache implements LeftHandCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger( VectorCache.class );
    VectorCache( final Project project, Feature feature) throws SQLException
    {
        this.project = project;
        this.feature = feature;
        this.values = new TreeMap<>(  );

        this.generateLeftHandData();
    }

    /**
     * Retrieves the data to store
     * @throws SQLException Thrown if the ID of the measurement unit could not be determined
     * @throws SQLException Thrown if the ID of the variable feature could not be determined
     * @throws SQLException Thrown if the data could not be retrieved from the database
     */
    private void generateLeftHandData() throws SQLException
    {
        Integer desiredMeasurementUnitID =
            MeasurementUnits.getMeasurementUnitID( this.project
                                                       .getDesiredMeasurementUnit());

        DataScripter script = new DataScripter(  );

        // Set it to high priority so that it doesn't conflict with standard database retrieval threads
        script.setHighPriority( true );

        DataSourceConfig left = this.project.getLeft();
        Integer leftVariableID = Variables.getVariableID( left);

        String earliestDate = this.project.getEarliestDate();
        String latestDate = this.project.getLatestDate();

        if (earliestDate != null)
        {
            earliestDate = "'" + earliestDate + "'";
        }

        if (latestDate != null)
        {
            latestDate = "'" + latestDate + "'";
        }

        String timeShift = null;

        String variablepositionClause = ConfigHelper.getVariableFeatureClause( this.feature, leftVariableID, "");

        if (left.getTimeShift() != null)
        {
            timeShift = "INTERVAL '" + ConfigHelper.getTimeShift( left ) + "'";
        }

        script.add("SELECT EXTRACT(epoch FROM (O.observation_time");

        if (timeShift != null)
        {
            script.add(" + ", timeShift);
        }

        script.addLine(")) AS left_date,");
        script.addLine("     O.observed_value AS left_value,");
        script.addLine("     O.measurementunit_id");
        script.addLine("FROM wres.ProjectSource PS");
        script.addLine("INNER JOIN wres.Observation O");
        script.addLine("     ON O.source_id = PS.source_id");
        script.addLine("WHERE PS.project_id = ", this.project.getId());
        script.addLine("     AND PS.member = 'left'");
        script.addLine("     AND ", variablepositionClause);

        if (earliestDate != null)
        {
            script.add("     AND O.observation_time");

            if (timeShift != null)
            {
                script.add(" + ", timeShift);
            }

            script.addLine(" >= ", earliestDate);
        }

        if (latestDate != null)
        {
            script.add("     AND O.observation_time");

            if (timeShift != null)
            {
                // Time shift is measured in hours in the config
                script.add(" + ", timeShift);
            }

            script.addLine(" <= ", latestDate);
        }

        script.add(";");

        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "Now selecting and processing the left hand data for {}.",
                         ConfigHelper.getFeatureDescription( this.feature ) );
        }

        try (DataProvider data = script.buffer())
        {
            while ( data.next() )
            {
                Long seconds = data.getLong( "left_date" );
                Double measurement = data.getValue("left_value");

                int unitID = data.getValue("measurementunit_id");

                if ( unitID != desiredMeasurementUnitID
                     && measurement != null )
                {
                    measurement = UnitConversions.convert( measurement,
                                                           unitID,
                                                           this.project
                                                                   .getDesiredMeasurementUnit() );
                }

                if ( measurement != null && measurement < this.project.getMinimumValue() )
                {
                    measurement = this.project.getDefaultMinimumValue();
                }
                else if ( measurement != null && measurement > this.project.getMaximumValue() )
                {
                    measurement = this.project.getDefaultMaximumValue();
                }
                else if ( measurement == null )
                {
                    measurement = Double.NaN;
                }

                if ( measurement != null )
                {
                    this.values.put( seconds, measurement );
                }
            }
        }

        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "{}: Left hand data has been selected and processed for {}.",
                          LocalDateTime.now(),
                          ConfigHelper.getFeatureDescription( this.feature ) );
        }
    }

    private final Project project;
    private final NavigableMap<Long, Double> values;
    private final Feature feature;

    @Override
    public Collection<Double> getLeftValues( Feature feature,
                                             LocalDateTime earliestDate,
                                             LocalDateTime latestDateTime )
    {
        long earliestSeconds = earliestDate.toInstant( ZoneOffset.UTC ).getEpochSecond();
        long latestSeconds = latestDateTime.toInstant( ZoneOffset.UTC ).getEpochSecond();
        return Collections.getValuesInRange(this.values, earliestSeconds, latestSeconds);
    }
}
