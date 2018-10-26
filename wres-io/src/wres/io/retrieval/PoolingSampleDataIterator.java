package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.CalculationException;

class PoolingSampleDataIterator extends SampleDataIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PoolingSampleDataIterator.class);

    PoolingSampleDataIterator( Feature feature,
                               ProjectDetails projectDetails,
                               SharedWriterManager sharedWriterManager,
                               Path outputDirectoryForPairs )
            throws IOException
    {
        super( feature,
               projectDetails,
               sharedWriterManager,
               outputDirectoryForPairs );
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