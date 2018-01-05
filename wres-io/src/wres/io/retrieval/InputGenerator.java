package wres.io.retrieval;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.inputs.MetricInput;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.NotImplementedException;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator implements Iterable<Future<MetricInput<?>>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(InputGenerator.class);

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
            switch (this.projectDetails.getAggregation().getMode())
            {
                case ROLLING:
                    iterator = new RollingMetricInputIterator( this.feature,
                                                               this.projectDetails );
                    break;
                case BACK_TO_BACK:
                    iterator =  new BackToBackMetricInputIterator( this.feature,
                                                                   this.projectDetails );
                    break;
                default:
                    throw new NotImplementedException( "The aggregation mode of '" +
                                                       this.projectDetails.getAggregation()
                                                                                  .getMode() +
                                                       "' has not been implemented." );
            }
        }
        catch (SQLException | NotImplementedException | InvalidPropertiesFormatException e)
        {
            // We don't want to log the new line, we just want the separator
            System.err.println();
            LOGGER.error("A MetricInputIterator could not be created for '{}'.",
                         ConfigHelper.getFeatureDescription( this.feature ));
        }
        catch ( NoDataException e )
        {
            // We don't want to log the new line, we just want the separator
            System.err.println();
            LOGGER.error("A MetricInputIterator could not be created for '{}'. " +
                         "There's no data to iterate over.",
                         ConfigHelper.getFeatureDescription(this.feature));
        }
        return iterator;
    }


}
