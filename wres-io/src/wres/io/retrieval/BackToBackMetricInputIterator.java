package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.CalculationException;

final class BackToBackMetricInputIterator extends MetricInputIterator
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( BackToBackMetricInputIterator.class );

    @Override
    Logger getLogger()
    {
        return BackToBackMetricInputIterator.LOGGER;
    }

    BackToBackMetricInputIterator( Feature feature,
                                   ProjectDetails projectDetails )
            throws IOException
    {
        super( feature, projectDetails );
    }

    @Override
    int calculateWindowCount() throws CalculationException
    {
        int count;
        if ( ConfigHelper.isForecast( this.getRight() ))
        {
            long start = Math.max(1, this.getFirstLeadInWindow());
            Integer last = this.getProjectDetails().getLastLead( this.getFeature() );

            if (last == null)
            {
                throw new CalculationException( "The final lead time for the data set for: " +
                                                    this.getRight()
                                                        .getVariable()
                                                        .getValue() +
                                                    " could not be calculated.");
            }
            else if (start > last)
            {
                throw new CalculationException( "No data can be retrieved because " +
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
