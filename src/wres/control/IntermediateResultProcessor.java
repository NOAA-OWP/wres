package wres.control;

import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.engine.statistics.metric.MetricProcessorByTime;
import wres.io.config.ProjectConfigPlus;

/**
 * A processor that generates outputs while a processing pipeline is ongoing (i.e. has more outputs to generate), rather 
 * than at the end of a pipeline (i.e. once all outputs have been generated). 
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse.bickel@***REMOVED***
 * @since 0.1
 * @version 0.2
 */

public class IntermediateResultProcessor implements Consumer<MetricOutputForProjectByTimeAndThreshold>
{

    /**
     * Logger.
     */
    
    private static final Logger LOGGER = LoggerFactory.getLogger( IntermediateResultProcessor.class );

    /**
     * The project configuration.
     */

    private final ProjectConfigPlus projectConfigPlus;

    /**
     * The feature.
     */

    private final Feature feature;
       
    /**
     * The processor used to determined whether the output is intermediate or being cached.
     */

    private final MetricProcessorByTime<?> processor;
        
    /**
     * Construct.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param processor the metric processor
     */

    IntermediateResultProcessor( final Feature feature,
                                 final ProjectConfigPlus projectConfigPlus,
                                 final MetricProcessorByTime<?> processor )
    {
        Objects.requireNonNull( feature,
                                "Specify a non-null feature for the results processor." );
        Objects.requireNonNull( projectConfigPlus,
                                "Specify a non-null configuration for the results processor." );
        Objects.requireNonNull( processor,
                                "Specify a non-null metric processor for the results processor." );
        this.feature = feature;
        this.projectConfigPlus = projectConfigPlus;
        this.processor = processor;
    }

        @Override
        public void accept(final MetricOutputForProjectByTimeAndThreshold input)
        {
            try
            {
                if ( ProcessorHelper.configNeedsThisTypeOfOutput( projectConfigPlus.getProjectConfig(),
                                                                  DestinationType.GRAPHIC ) )
                {
                    MetricOutputMetadata meta = null;

                    //Multivector output available, not being cached to the end
                    if ( input.hasOutput( MetricOutputGroup.MULTIVECTOR )
                         && !processor.willCacheMetricOutput( MetricOutputGroup.MULTIVECTOR ) )
                    {
                        ProcessorHelper.processMultiVectorCharts( feature,
                                                                  projectConfigPlus,
                                                                  input.getMultiVectorOutput() );
                        meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    }
                    //Box-plot output available, not being cached to the end
                    if ( input.hasOutput( MetricOutputGroup.BOXPLOT )
                         && !processor.willCacheMetricOutput( MetricOutputGroup.BOXPLOT ) )
                    {
                        ProcessorHelper.processBoxPlotCharts( feature,
                                                              projectConfigPlus,
                                                              input.getBoxPlotOutput() );
                        meta = input.getBoxPlotOutput().entrySet().iterator().next().getValue().getMetadata();
                    }
                    if ( Objects.nonNull( meta ) )
                    {
                        LOGGER.debug( "Completed processing of intermediate metrics results for feature '{}' "
                                      + "and time window {}.",
                                      meta.getIdentifier().getGeospatialID(),
                                      meta.getTimeWindow() );
                    }
                    else
                    {
                        LOGGER.debug( "Completed processing of intermediate metrics results for feature '{}' with "
                                      + "unknown time window.",
                                      feature.getLocationId() );
                    }
                }
            }
            catch ( final MetricOutputAccessException e )
            {
                if ( Thread.currentThread().isInterrupted() )
                {
                    LOGGER.warn( "Interrupted while processing intermediate results:", e );
                }
                throw new WresProcessingException( "Error while processing intermediate results:", e );
            }
        }
}
