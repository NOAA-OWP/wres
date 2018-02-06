package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.Future;

import wres.config.generated.Feature;
import wres.datamodel.inputs.MetricInput;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.util.NotImplementedException;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator implements Iterable<Future<MetricInput<?>>>
{

    public InputGenerator( Feature feature,
                           ProjectDetails projectDetails )
    {
        this.feature = feature;
        this.projectDetails = projectDetails;
    }

    private final Feature feature;
    private final ProjectDetails projectDetails;

    @Override
    public Iterator<Future<MetricInput<?>>> iterator()
    {
        // TODO: Evaluate what kind of MetricInputIterator to return.
        Iterator<Future<MetricInput<?>>> iterator = null;
        try
        {
            switch (this.projectDetails.getPoolingMode())
            {
                case ROLLING:
                    iterator = new PoolingMetricInputIterator( this.feature,
                                                               this.projectDetails );
                    break;
                case BACK_TO_BACK:
                    iterator =  new BackToBackMetricInputIterator( this.feature,
                                                                   this.projectDetails );
                    break;
                default:
                    throw new NotImplementedException( "The aggregation mode of '" +
                                                       this.projectDetails.getPoolingMode() +
                                                       "' has not been implemented." );
            }
        }
        catch (SQLException | IOException e)
        {
            String message = "A MetricInputIterator could not be created for '"
                             + ConfigHelper.getFeatureDescription( this.feature )
                             + "'.";
            throw new IterationFailedException( message, e );
        }
        return iterator;
    }


}
