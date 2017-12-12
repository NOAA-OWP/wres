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
