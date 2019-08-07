package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.project.Project;
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

    @Override
    protected void calculateSamples()
    {
        LOGGER.trace( "Calculating the sample metadata...");
        int sampleCount = 0;
        OrderedSampleMetadata.Builder metadataBuilder =
                new OrderedSampleMetadata.Builder().setProject( this.getProject() )
                                                   .setFeature( this.getFeature() );


        // One big pool
        if ( BasicSampleDataIterator.requiresOneBigPool( this.getProject().getProjectConfig() ) )
        {
            // If we are dealing with observation/simulation data, we only need one window
            TimeWindow window = this.getOneBigTimeWindow( this.getProject().getProjectConfig() );

            sampleCount++;

            this.addSample( metadataBuilder.setSampleNumber( sampleCount )
                                           .setTimeWindow( window )
                                           .build() );
        }
        
        // Lead duration pools
        else
        {
            final Duration lastPossibleLead = Duration.of( this.getProject().getLastLead( this.getFeature() ),
                                                           TimeHelper.LEAD_RESOLUTION );
 
            int leadIteration = 0;

            Pair<Duration, Duration> leadBounds = this.getLeadBounds( leadIteration );

            // Check that the right bound is <= the last possible lead 
            // No need to check the left bound, as the right bound >= left bound
            // Will ensure that windows of zero width are included on the right bound
            // See #66118
            while ( leadBounds.getRight().compareTo( lastPossibleLead ) <= 0 )
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
        }

        // JBr: Demoted from exception to log. The "lead offset" could exceed the last lead duration to 
        // consider, in which case there is no pool with data, and the pre-check is not 
        // sufficiently sophisticated to assert that this is exceptional
        if ( this.getSampleCount() == 0)
        {
            LOGGER.debug( "No windows could be generated for '"+this.getFeature()+"'." );
        }

        LOGGER.trace("Sample metadata has been calculated.");
    }
    
    /**
     * Returns <code>true</code> if there is one big pool to generate, otherwise <code>false</code>.
     * 
     * @return true if the project declares zero types of pooling window, false if one or more types
     */

    private static boolean requiresOneBigPool( ProjectConfig project )
    {
        return Objects.isNull( project.getPair().getIssuedDatesPoolingWindow() )
               && Objects.isNull( project.getPair().getLeadTimesPoolingWindow() );
    }
    
    /**
     * Creates one big time window that is consistent with the project declaration. TODO: replace with #56213, which
     * will supply the time windows for iteration.
     * 
     * @param projectConfig the project configuration
     * @return a time window
     * @throws NullPointerException if the input is null
     */

    private TimeWindow getOneBigTimeWindow( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, "Cannot determine the time window from null project configuration." );

        PairConfig pairConfig = projectConfig.getPair();

        Instant earliestReferenceTime = Instant.MIN;
        Instant latestReferenceTime = Instant.MAX;
        Instant earliestValidTime = Instant.MIN;
        Instant latestValidTime = Instant.MAX;
        Duration smallestLeadDuration = TimeWindow.DURATION_MIN;
        Duration largestLeadDuration = TimeWindow.DURATION_MAX;

        // Issued datetimes
        if ( Objects.nonNull( pairConfig.getIssuedDates() ) )
        {
            if ( Objects.nonNull( pairConfig.getIssuedDates().getEarliest() ) )
            {
                earliestReferenceTime = Instant.parse( pairConfig.getIssuedDates().getEarliest() );
            }
            if ( Objects.nonNull( pairConfig.getIssuedDates().getLatest() ) )
            {
                latestReferenceTime = Instant.parse( pairConfig.getIssuedDates().getLatest() );
            }
        }

        // Valid datetimes
        if ( Objects.nonNull( pairConfig.getDates() ) )
        {
            if ( Objects.nonNull( pairConfig.getDates().getEarliest() ) )
            {
                earliestValidTime = Instant.parse( pairConfig.getDates().getEarliest() );
            }
            if ( Objects.nonNull( pairConfig.getDates().getLatest() ) )
            {
                latestValidTime = Instant.parse( pairConfig.getDates().getLatest() );
            }
        }

        // Lead durations
        if ( Objects.nonNull( pairConfig.getLeadHours() ) )
        {
            if ( Objects.nonNull( pairConfig.getLeadHours().getMinimum() ) )
            {
                smallestLeadDuration = Duration.ofHours( pairConfig.getLeadHours().getMinimum() );
            }
            if ( Objects.nonNull( pairConfig.getLeadHours().getMaximum() ) )
            {
                largestLeadDuration = Duration.ofHours( pairConfig.getLeadHours().getMaximum() );
            }
        }

        // Adjust the lead durations for any offset
        Pair<Duration, Duration> adjusted =
                this.getAdjustedLeadBounds( Pair.of( smallestLeadDuration, largestLeadDuration ) );
        
        smallestLeadDuration = adjusted.getLeft();
        largestLeadDuration = adjusted.getRight();
        
        return TimeWindow.of( earliestReferenceTime,
                              latestReferenceTime,
                              earliestValidTime,
                              latestValidTime,
                              smallestLeadDuration,
                              largestLeadDuration );
    }    
    
}
