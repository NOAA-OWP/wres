package wres.control;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.io.config.ProjectConfigPlus;
import wres.io.writing.CommaSeparated;

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
     * The types that should not be processed as intermediate output, because they are being cached until the end
     * of a processing pipeline.
     */
       
    private final Set<MetricOutputGroup> ignoreTheseTypes;
    
    /**
     * Construct.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @throws NullPointerException if any of the inputs are null
     */

    IntermediateResultProcessor( final Feature feature,
                                 final ProjectConfigPlus projectConfigPlus,
                                 final Set<MetricOutputGroup> ignoreTheseTypes )
    {
        Objects.requireNonNull( feature,
                                "Specify a non-null feature for the results processor." );
        Objects.requireNonNull( projectConfigPlus,
                                "Specify a non-null configuration for the results processor." );
        Objects.requireNonNull( projectConfigPlus, 
                                "Specify a non-null set of output types to ignore, such as the empty set." );
        this.feature = feature;
        this.projectConfigPlus = projectConfigPlus;
        this.ignoreTheseTypes = ignoreTheseTypes;
    }

    @Override
    public void accept( final MetricOutputForProjectByTimeAndThreshold input )
    {
        try
        {
            if ( ProcessorHelper.configNeedsThisTypeOfOutput( projectConfigPlus.getProjectConfig(),
                                                              DestinationType.GRAPHIC ) )
            {
                MetricOutputMetadata meta = null;

                //Multivector output available and not being cached to the end
                if ( input.hasOutput( MetricOutputGroup.MULTIVECTOR )
                     && !ignoreTheseTypes.contains( MetricOutputGroup.MULTIVECTOR ) )
                {
                    ProcessorHelper.processMultiVectorCharts( feature,
                                                              projectConfigPlus,
                                                              input.getMultiVectorOutput() );
                    meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    // Write the CSV output
                    CommaSeparated.writeDiagramFiles( projectConfigPlus.getProjectConfig(),
                                                      input.getMultiVectorOutput() );
                }
                //Box-plot output available and not being cached to the end
                if ( input.hasOutput( MetricOutputGroup.BOXPLOT )
                     && !ignoreTheseTypes.contains( MetricOutputGroup.BOXPLOT ) )
                {
                    ProcessorHelper.processBoxPlotCharts( feature,
                                                          projectConfigPlus,
                                                          input.getBoxPlotOutput() );
                    meta = input.getBoxPlotOutput().entrySet().iterator().next().getValue().getMetadata();
                    // Write the CSV output
                    CommaSeparated.writeBoxPlotFiles( projectConfigPlus.getProjectConfig(),
                                                      input.getBoxPlotOutput() );
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
        catch ( final MetricOutputAccessException | IOException e )
        {
            if ( Thread.currentThread().isInterrupted() )
            {
                LOGGER.warn( "Interrupted while processing intermediate results:", e );
            }
            throw new WresProcessingException( "Error while processing intermediate results:", e );
        }
    }
}
