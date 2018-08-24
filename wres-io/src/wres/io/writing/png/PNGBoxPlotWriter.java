package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.BoxPlotOutput;
import wres.datamodel.statistics.ListOfMetricOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link BoxPlotOutput} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGBoxPlotWriter extends PNGWriter
        implements Consumer<ListOfMetricOutput<BoxPlotOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGBoxPlotWriter of( final ProjectConfigPlus projectConfigPlus )
    {
        return new PNGBoxPlotWriter( projectConfigPlus );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfMetricOutput<BoxPlotOutput> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( projectConfigPlus.getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {
            // Iterate through types
            SortedSet<MetricConstants> metrics = Slicer.discover( output, meta -> meta.getMetadata().getMetricID() );
            for ( MetricConstants next : metrics )
            {
                PNGBoxPlotWriter.writeBoxPlotCharts( projectConfigPlus,
                                                     destinationConfig,
                                                     Slicer.filter( output, next ) );
            }
        }
    }

    /**
     * Writes a set of charts associated with {@link BoxPlotOutput} for a single metric and time window, 
     * stored in a {@link ListOfMetricOutput}.
     *
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results
     * @throws PNGWriteException when an error occurs during writing
     */

    private static void writeBoxPlotCharts( ProjectConfigPlus projectConfigPlus,
                                            DestinationConfig destinationConfig,
                                            ListOfMetricOutput<BoxPlotOutput> output )
    {
        // Build charts
        try
        {
            MetricOutputMetadata meta = output.getData().get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            final Map<Pair<TimeWindow, OneOrTwoThresholds>, ChartEngine> engines =
                    ChartEngineFactory.buildBoxPlotChartEngine( projectConfigPlus.getProjectConfig(),
                                                                output,
                                                                helper.getTemplateResourceName(),
                                                                helper.getGraphicsString() );

            // Build the outputs
            for ( final Entry<Pair<TimeWindow, OneOrTwoThresholds>, ChartEngine> nextEntry : engines.entrySet() )
            {

                Path outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                                      meta,
                                                                      nextEntry.getKey().getLeft() );

                PNGWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
            }

        }
        catch ( ChartEngineException | IOException e )
        {
            throw new PNGWriteException( "Error while generating box plot charts: ", e );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfigPlus the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private PNGBoxPlotWriter( ProjectConfigPlus projectConfigPlus )
    {
        super( projectConfigPlus );
    }

}
