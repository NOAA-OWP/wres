package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.util.CalculationException;

class PoolingMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PoolingMetricInputIterator.class);

    PoolingMetricInputIterator( Feature feature,
                                ProjectDetails projectDetails )
            throws IOException
    {
        super( feature, projectDetails );
    }

    @Override
    int calculateWindowCount() throws CalculationException
    {
        return this.getProjectDetails().getIssuePoolCount( this.getFeature() );
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}