package wres.io.retrieval;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;

public class RollingMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RollingMetricInputIterator.class);

    RollingMetricInputIterator( ProjectConfig projectConfig,
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
        return 0;
    }

    @Override
    protected Integer getWindowCount() throws NoDataException, SQLException,
            InvalidPropertiesFormatException
    {
        return super.getWindowCount();
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}