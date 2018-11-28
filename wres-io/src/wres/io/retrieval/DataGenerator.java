package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Future;

import wres.config.generated.Feature;
import wres.datamodel.sampledata.SampleData;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.data.details.ProjectDetails;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.NotImplementedException;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class DataGenerator implements Iterable<Future<SampleData<?>>>
{
    private final Feature feature;
    private final ProjectDetails projectDetails;
    private final SharedWriterManager sharedWriterManager;
    private final Path outputDirectoryForPairs;
    private final Collection<OrderedSampleMetadata> sampleMetadata;

    public DataGenerator( Feature feature,
                          ProjectDetails projectDetails,
                          SharedWriterManager sharedWriterManager,
                          Path outputDirectoryForPairs )
    {
        this.feature = feature;
        this.projectDetails = projectDetails;
        this.sharedWriterManager = sharedWriterManager;
        this.outputDirectoryForPairs = outputDirectoryForPairs;
        this.sampleMetadata = null;
    }

    public DataGenerator( Feature feature,
                          ProjectDetails projectDetails,
                          SharedWriterManager sharedWriterManager,
                          Path outputDirectoryForPairs,
                          final Collection<OrderedSampleMetadata> sampleMetadata)
    {
        this.feature = feature;
        this.projectDetails = projectDetails;
        this.sharedWriterManager = sharedWriterManager;
        this.outputDirectoryForPairs = outputDirectoryForPairs;
        this.sampleMetadata = sampleMetadata;
    }

    @Override
    public Iterator<Future<SampleData<?>>> iterator()
    {
        Iterator<Future<SampleData<?>>> iterator;
        try
        {
            switch (this.projectDetails.getPairingMode())
            {
                case ROLLING:
                    iterator = new PoolingSampleDataIterator( this.feature,
                                                              this.projectDetails,
                                                              this.sharedWriterManager,
                                                              this.outputDirectoryForPairs,
                                                              this.sampleMetadata);
                    break;
                // TODO: Merge back to back and rolling logic
                case BACK_TO_BACK:
                    iterator =  new BackToBackSampleDataIterator( this.feature,
                                                                  this.projectDetails,
                                                                  this.sharedWriterManager,
                                                                  this.outputDirectoryForPairs,
                                                                  this.sampleMetadata);
                    break;
                case TIME_SERIES:
                    iterator = new TimeSeriesSampleDataIterator( this.feature,
                                                                 this.projectDetails,
                                                                 this.sharedWriterManager,
                                                                 this.outputDirectoryForPairs,
                                                                 this.sampleMetadata);
                    break;
                case BY_TIMESERIES:
                    iterator = new ByForecastSampleDataIterator( this.feature,
                                                                 this.projectDetails,
                                                                 this.sharedWriterManager,
                                                                 this.outputDirectoryForPairs,
                                                                 this.sampleMetadata);
                    break;
                default:
                    throw new NotImplementedException( "The pairing mode of '" +
                                                       this.projectDetails.getPairingMode() +
                                                       "' has not been implemented." );
            }
        }
        catch (IOException e)
        {
            String message = "A MetricInputIterator could not be created for '"
                             + ConfigHelper.getFeatureDescription( this.feature )
                             + "'.";
            throw new IterationFailedException( message, e );
        }

        return iterator;
    }


}
