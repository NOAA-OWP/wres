package wres.io.retrieval;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;

public class RollingMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RollingMetricInputIterator.class);

    RollingMetricInputIterator( Feature feature,
                                ProjectDetails projectDetails )
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        super( feature, projectDetails );
    }

    @Override
    int calculateWindowCount()
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        return this.getProjectDetails().getRollingWindowCount( this.getFeature() );
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}