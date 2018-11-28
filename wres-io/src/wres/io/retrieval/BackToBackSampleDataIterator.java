package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.datamodel.metadata.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.data.details.ProjectDetails;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.CalculationException;
import wres.util.TimeHelper;

final class BackToBackSampleDataIterator extends SampleDataIterator
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( BackToBackSampleDataIterator.class );

    @Override
    Logger getLogger()
    {
        return BackToBackSampleDataIterator.LOGGER;
    }

    BackToBackSampleDataIterator( Feature feature,
                                  ProjectDetails projectDetails,
                                  SharedWriterManager sharedWriterManager,
                                  Path outputDirectoryForPairs,
                                  final Collection<OrderedSampleMetadata> sampleMetadataCollection)
            throws IOException
    {
        super( feature,
               projectDetails,
               sharedWriterManager,
               outputDirectoryForPairs,
               sampleMetadataCollection);
    }


    @Override
    /**
     * Generates a list of TimeWindows that will generate SampleData objects
     */
    protected void calculateSamples() throws CalculationException
    {
        int sampleCount = 0;
        OrderedSampleMetadata.Builder metadataBuilder = new OrderedSampleMetadata.Builder().setProject( this.getProjectDetails() )
                                                                                           .setFeature( this.getFeature() );


        // If we are basing samples on forecasts, we want to have the option of
        if (ConfigHelper.isForecast( this.getRight() ))
        {
            final Duration lastPossibleLead = Duration.of( this.getProjectDetails().getLastLead( this.getFeature() ),
                                                           TimeHelper.LEAD_RESOLUTION );

            int leadIteration = 0;

            Pair<Duration, Duration> leadBounds = this.getLeadBounds( leadIteration );

            while (TimeHelper.lessThan(leadBounds.getLeft(), lastPossibleLead) &&
                   TimeHelper.lessThanOrEqualTo( leadBounds.getRight(), lastPossibleLead ))
            {
                TimeWindow window = ConfigHelper.getTimeWindow(
                        this.getProjectDetails(),
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
                    this.getProjectDetails(),
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
        if (this.amountOfSamplesLeft() == 0)
        {
            throw new IterationFailedException( "No windows could be generated for evaluation." );
        }
    }
}
