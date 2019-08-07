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

class PoolingSampleDataIterator extends SampleDataIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PoolingSampleDataIterator.class);

    PoolingSampleDataIterator( Feature feature, Project project, Path outputDirectoryForPairs)
            throws IOException
    {
        super( feature, project, outputDirectoryForPairs);
    }

    @Override
    protected void calculateSamples() throws CalculationException
    {
        int sampleCount = 0;
        OrderedSampleMetadata.Builder metadataBuilder =
                new OrderedSampleMetadata.Builder().setProject( this.getProject() )
                                                   .setFeature( this.getFeature() );


        final Duration lastPossibleLead = Duration.of( this.getProject().getLastLead( this.getFeature() ),
                                                       TimeHelper.LEAD_RESOLUTION );

        final int lastPoolingStep = this.getFinalPoolingStep();

        int leadIteration = 0;

        Pair<Duration, Duration> leadBounds = this.getLeadBounds( leadIteration );

        while ( leadBounds.getRight().compareTo( lastPossibleLead ) <= 0 )
        {
            for ( int issuePoolStep = 0; issuePoolStep < lastPoolingStep; ++issuePoolStep )
            {
                TimeWindow window = ConfigHelper.getTimeWindow(
                        this.getProject(),
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

            Pair<Duration, Duration> newBounds = this.getLeadBounds( leadIteration );
            
            // If the window is zero wide and centered on one lead duration
            // then it's possible that the next window is the same as the last window,
            // so stop here
            // #66118
            if( newBounds.equals( leadBounds ) ) 
            {
                break;
            }
            
            leadBounds = newBounds;   
        }
        
        // We need to throw an exception if no samples to evaluate could be determined
        // JBr: Demoted from exception to log. The "lead offset" could exceed the last lead duration to 
        // consider, in which case there is no pool with data, and the pre-check is not 
        // sufficiently sophisticated to assert that this is exceptional
        if ( this.getSampleCount() == 0)
        {
            LOGGER.debug( "No windows could be generated for '{}'. First "
                    + "attemped lead duration bounds were '{}'.", this.getFeature(), leadBounds );
        }
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}