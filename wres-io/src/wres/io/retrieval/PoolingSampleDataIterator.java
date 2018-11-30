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

class PoolingSampleDataIterator extends SampleDataIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PoolingSampleDataIterator.class);

    PoolingSampleDataIterator( Feature feature,
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
    protected void calculateSamples() throws CalculationException
    {
        int sampleCount = 0;
        OrderedSampleMetadata.Builder metadataBuilder = new OrderedSampleMetadata.Builder().setProject( this.getProjectDetails() )
                                                                                           .setFeature( this.getFeature() );


        final Duration lastPossibleLead = Duration.of( this.getProjectDetails().getLastLead( this.getFeature() ),
                                                       TimeHelper.LEAD_RESOLUTION );

        final int lastPoolingStep = this.getFinalPoolingStep();

        int leadIteration = 0;

        Pair<Duration, Duration> leadBounds = this.getLeadBounds( leadIteration );

        while (TimeHelper.lessThan(leadBounds.getLeft(), lastPossibleLead) &&
               TimeHelper.lessThanOrEqualTo( leadBounds.getRight(), lastPossibleLead ))
        {
            for ( int issuePoolStep = 0; issuePoolStep < lastPoolingStep; ++issuePoolStep )
            {
                TimeWindow window = ConfigHelper.getTimeWindow(
                        this.getProjectDetails(),
                        leadBounds.getLeft(),
                        leadBounds.getRight(),
                        issuePoolStep
                );

                sampleCount++;

                this.addSample(
                        metadataBuilder.setSampleNumber( sampleCount )
                                       .setTimeWindow( window )
                                       .build()
                );
            }

            leadIteration++;

            leadBounds = this.getLeadBounds( leadIteration );
        }
        
        // We need to throw an exception if no samples to evaluate could be determined
        if (this.amountOfSamplesLeft() == 0)
        {
            throw new IterationFailedException( "No windows could be generated for evaluation." );
        }
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}