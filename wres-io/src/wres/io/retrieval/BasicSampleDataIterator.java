package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.project.Project;
import wres.util.CalculationException;
import wres.util.IterationFailedException;
import wres.util.TimeHelper;

final class BasicSampleDataIterator extends SampleDataIterator
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( BasicSampleDataIterator.class );

    @Override
    Logger getLogger()
    {
        return BasicSampleDataIterator.LOGGER;
    }

    BasicSampleDataIterator( Feature feature, Project project, Path outputDirectoryForPairs)
            throws IOException
    {
        super( feature, project, outputDirectoryForPairs);
    }


    /**
     * Generates a list of TimeWindows that will generate SampleData objects
     */
    @Override
    protected void calculateSamples() throws CalculationException
    {
        LOGGER.trace( "{} Calculating the sample metadata...");
        int sampleCount = 0;
        OrderedSampleMetadata.Builder metadataBuilder =
                new OrderedSampleMetadata.Builder().setProject( this.getProject() )
                                                   .setFeature( this.getFeature() );


        // If we are basing samples on forecasts, we want to have the option of
        if (ConfigHelper.isForecast( this.getRight() ))
        {
            final Duration lastPossibleLead = Duration.of( this.getProject().getLastLead( this.getFeature() ),
                                                           TimeHelper.LEAD_RESOLUTION );
 
            int leadIteration = 0;

            Pair<Duration, Duration> leadBounds = this.getLeadBounds( leadIteration );
         
            while (TimeHelper.lessThan(leadBounds.getLeft(), lastPossibleLead) &&
                   TimeHelper.lessThanOrEqualTo( leadBounds.getRight(), lastPossibleLead ))
            {
                TimeWindow window = ConfigHelper.getTimeWindow(
                        this.getProject(),
                        leadBounds.getLeft(),
                        leadBounds.getRight(),
                        0
                );

                sampleCount++;

                this.addSample(
                        metadataBuilder.setSampleNumber( sampleCount )
                                       .setTimeWindow( window )
                                       .build()
                );

                leadIteration++;

                leadBounds = this.getLeadBounds( leadIteration );                
            }
        }
        else
        {
            // If we are dealing with observation/simulation data, we only need one window
            TimeWindow window = ConfigHelper.getTimeWindow(
                    this.getProject(),
                    Duration.ZERO,
                    Duration.ZERO,
                    0
            );

            sampleCount++;

            this.addSample(
                    metadataBuilder.setSampleNumber( sampleCount )
                                   .setTimeWindow( window )
                                   .build()
            );
        }

        // We need to throw an exception if no samples to evaluate could be determined
        if ( this.getSampleCount() == 0)
        {
            throw new IterationFailedException( "No windows could be generated for evaluation." );
        }

        LOGGER.trace("Sample metadata has been calculated.");
    }
}
