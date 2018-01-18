package wres.control;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.PlotTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.ConfigMapper;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.engine.statistics.metric.MetricProcessorByTime;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.io.writing.ChartWriter;
import wres.io.writing.ChartWriter.ChartWritingException;
import wres.vis.ChartEngineFactory;

public class IntermediateResultProcessor implements Consumer<MetricOutputForProjectByTimeAndThreshold>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( IntermediateResultProcessor.class );

    /**
     * Default data factory.
     */

    private static final DataFactory DATA_FACTORY = DefaultDataFactory.getInstance();


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

    /**
     * Processes a set of charts associated with {@link MultiVectorOutput} across multiple metrics, time windows,
     * and thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param multiVectorResults the metric results
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    static void processMultiVectorCharts( final Feature feature,
                                          final ProjectConfigPlus projectConfigPlus,
                                          final MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> multiVectorResults )
    {
        // Check for results
        if(Objects.isNull(multiVectorResults))
        {
            LOGGER.warn( "No multi-vector outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric 
        for ( final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>> e : multiVectorResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeMultiVectorCharts( feature, projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }
    
    /**
     * Writes a set of charts associated with {@link MultiVectorOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param multiVectorResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeMultiVectorCharts( Feature feature,
                                                ProjectConfigPlus projectConfigPlus,
                                                DestinationConfig destConfig,
                                                MetricConstants metricId,
                                                MetricOutputMapByTimeAndThreshold<MultiVectorOutput> multiVectorResults )
    {
        // Build charts
        try
        {
            ProjectConfig config = projectConfigPlus.getProjectConfig();
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            // Build the chart engine
            MetricConfig nextConfig = getNamedConfigOrAllValid( metricId, config );
            // Default to global type parameter
            PlotTypeSelection plotType = destConfig.getGraphical().getPlotType();
            String templateResourceName = null;
            if ( Objects.nonNull( nextConfig ) )
            {
                // Local type parameter
                if ( nextConfig.getPlotType() != null )
                {
                    plotType = nextConfig.getPlotType();
                }
                templateResourceName = nextConfig.getTemplateResourceName();
            }

            final Map<Object, ChartEngine> engines =
                    ChartEngineFactory.buildMultiVectorOutputChartEngine( multiVectorResults,
                                                                          DATA_FACTORY,
                                                                          plotType,
                                                                          templateResourceName,
                                                                          graphicsString );
            // Build the outputs
            for ( final Map.Entry<Object, ChartEngine> nextEntry : engines.entrySet() )
            {
                // Build the output file name
                // TODO: adopt a more general naming convention as the pipelines expand
                // For now, the only supported temporal pipeline is per lead time
                Object key = nextEntry.getKey();
                if ( key instanceof TimeWindow )
                {
                    key = ( (TimeWindow) key ).getLatestLeadTimeInHours();
                }
                else if ( key instanceof Threshold )
                {
                    key = ( (Threshold) key ).toStringSafe();
                }
                File destDir = ConfigHelper.getDirectoryFromDestinationConfig( destConfig );
                Path outputImage = Paths.get( destDir.toString(),
                                              ConfigHelper.getFeatureDescription( feature )
                                                                  + "_"
                                                                  + metricId.name()
                                                                  + "_"
                                                                  + config.getInputs()
                                                                          .getRight()
                                                                          .getVariable()
                                                                          .getValue()
                                                                  + "_"
                                                                  + key
                                                                  + ".png" );
                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating multi-vector charts:", e );
        }
    }
    
    /**
     * Processes a set of charts associated with {@link BoxPlotOutput} across multiple metrics, time window, and 
     * thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param boxPlotResults the box plot outputs
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    static void processBoxPlotCharts( final Feature feature,
                                      final ProjectConfigPlus projectConfigPlus,
                                      final MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> boxPlotResults )
    {
        // Check for results
        if ( Objects.isNull( boxPlotResults ) )
        {
            LOGGER.warn( "No box-plot outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric 
        for ( final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<BoxPlotOutput>> e : boxPlotResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeBoxPlotCharts( feature, projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }

    /**
     * Writes a set of charts associated with {@link BoxPlotOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param boxPlotResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeBoxPlotCharts( Feature feature,
                                            ProjectConfigPlus projectConfigPlus,
                                            DestinationConfig destConfig,
                                            MetricConstants metricId,
                                            MetricOutputMapByTimeAndThreshold<BoxPlotOutput> boxPlotResults )
    {
        // Build charts
        try
        {
            ProjectConfig config = projectConfigPlus.getProjectConfig();
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            // Build the chart engine
            MetricConfig nextConfig = getNamedConfigOrAllValid( metricId, config );
            String templateResourceName = null;
            if ( Objects.nonNull( nextConfig ) )
            {
                templateResourceName = nextConfig.getTemplateResourceName();
            }

            final Map<Pair<TimeWindow, Threshold>, ChartEngine> engines =
                    ChartEngineFactory.buildBoxPlotChartEngine( boxPlotResults,
                                                                templateResourceName,
                                                                graphicsString );
            // Build the outputs
            for ( final Map.Entry<Pair<TimeWindow, Threshold>, ChartEngine> nextEntry : engines.entrySet() )
            {
                // Build the output file name
                File destDir = ConfigHelper.getDirectoryFromDestinationConfig( destConfig );
                // TODO: adopt a more general naming convention as the pipelines expand
                // For now, the only temporal pipeline is by lead time
                long key = nextEntry.getKey().getLeft().getLatestLeadTimeInHours();
                Path outputImage = Paths.get( destDir.toString(),
                                              ConfigHelper.getFeatureDescription( feature )
                                                                  + "_"
                                                                  + metricId.name()
                                                                  + "_"
                                                                  + config.getInputs()
                                                                          .getRight()
                                                                          .getVariable()
                                                                          .getValue()
                                                                  + "_"
                                                                  + key
                                                                  + ".png" );
                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating box-plot charts:", e );
        }
    }

        @Override
        public void accept(final MetricOutputForProjectByTimeAndThreshold input)
        {
            try
            {
                if ( configNeedsThisTypeOfOutput( projectConfigPlus.getProjectConfig(),
                                                  DestinationType.GRAPHIC ) )
                {
                    MetricOutputMetadata meta = null;

                    //Multivector output available, not being cached to the end
                    if ( input.hasOutput( MetricOutputGroup.MULTIVECTOR )
                         && !processor.willCacheMetricOutput( MetricOutputGroup.MULTIVECTOR ) )
                    {
                        processMultiVectorCharts( feature,
                                                  projectConfigPlus,
                                                  input.getMultiVectorOutput() );
                        meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    }
                    //Box-plot output available, not being cached to the end
                    if ( input.hasOutput( MetricOutputGroup.BOXPLOT )
                         && !processor.willCacheMetricOutput( MetricOutputGroup.BOXPLOT ) )
                    {
                        processBoxPlotCharts( feature,
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


    /**
     * Locates the metric configuration corresponding to the input {@link MetricConstants} or null if no corresponding
     * configuration could be found. If the configuration contains a {@link MetricConfigName#ALL_VALID}, the 
     * prescribed metric identifier is ignored and the configuration is returned for 
     * {@link MetricConfigName#ALL_VALID}.
     * 
     * @param metric the metric
     * @param config the project configuration
     * @return the metric configuration or null
     */

    private static MetricConfig getNamedConfigOrAllValid( final MetricConstants metric, final ProjectConfig config )
    {
        // Deal with MetricConfigName.ALL_VALID first
        MetricConfig allValid = ConfigHelper.getMetricConfigByName( config, MetricConfigName.ALL_VALID );
        if ( allValid != null )
        {
            return allValid;
        }
        // Find the corresponding configuration
        final Optional<MetricConfig> returnMe = config.getOutputs().getMetric().stream().filter( a -> {
            try
            {
                return metric.equals( ConfigMapper.from( a.getName() ) );
            }
            catch ( final MetricConfigurationException e )
            {
                LOGGER.error( "Could not map metric name '{}' to metric configuration.", metric, e );
                return false;
            }
        } ).findFirst();
        return returnMe.isPresent() ? returnMe.get() : null;
    }

    /**
     * Returns true if the given config has one or more of given output type.
     * @param config the config to search
     * @param type the type of output to look for
     * @return true if the output type is present, false otherwise
     */

    private static boolean configNeedsThisTypeOfOutput( ProjectConfig config,
                                                        DestinationType type )
    {
        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return false;
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            if ( d.getType().equals( type ) )
            {
                return true;
            }
        }

        return false;
    }

}
