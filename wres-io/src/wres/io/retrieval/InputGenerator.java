package wres.io.retrieval;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Future;

import wres.config.generated.Feature;
import wres.datamodel.sampledata.SampleData;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.NotImplementedException;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator implements Iterable<Future<SampleData<?>>>
{
    private final Feature feature;
    private final ProjectDetails projectDetails;
    private final SharedWriterManager sharedWriterManager;

    public InputGenerator( Feature feature,
                           ProjectDetails projectDetails,
                           SharedWriterManager sharedWriterManager )
    {
        this.feature = feature;
        this.projectDetails = projectDetails;
        this.sharedWriterManager = sharedWriterManager;
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
                    iterator = new PoolingMetricInputIterator( this.feature,
                                                               this.projectDetails,
                                                               this.sharedWriterManager );
                    break;
                // TODO: Merge back to back and rolling logic
                case BACK_TO_BACK:
                    iterator =  new BackToBackMetricInputIterator( this.feature,
                                                                   this.projectDetails,
                                                                   this.sharedWriterManager );
                    break;
                case TIME_SERIES:
                    iterator = new TimeSeriesMetricInputIterator( this.feature,
                                                                  this.projectDetails,
                                                                  this.sharedWriterManager );
                    break;
                default:
                    throw new NotImplementedException( "The aggregation mode of '" +
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
