package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.datamodel.inputs.MetricInput;
import wres.io.concurrency.Executor;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;

public class TimeSeriesMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TimeSeriesMetricInputIterator.class);

    TimeSeriesMetricInputIterator( Feature feature,
                                   ProjectDetails projectDetails )
            throws SQLException, IOException
    {
        super( feature, projectDetails );
    }

    @Override
    int calculateWindowCount() throws SQLException
    {
        return this.getProjectDetails().getIssuePoolCount( this.getFeature() );
    }

    @Override
    protected Future<MetricInput<?>> submitForRetrieval() throws IOException
    {
        return Executor.submit( this.createRetriever() );
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}