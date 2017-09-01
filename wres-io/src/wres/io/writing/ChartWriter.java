package wres.io.writing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.config.generated.DestinationConfig;
import wres.io.config.SystemSettings;

/**
 * Helps to write a {@link ChartEngine} to a graphical product file, currently only in PNG format.
 * 
 * TODO: unit test somehow
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ChartWriter
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(ChartWriter.class);
    
    /**
     * Writes an output chart to a specified path.
     * 
     * @param outputImage the path to the output image
     * @param engine the chart engine
     * @param dest the destination configuration
     * @throws XYChartDataSourceException if the chart data could not be constructed
     * @throws ChartEngineException if the chart could not be constructed
     * @throws IOException if the chart could not be written
     */

    public static void writeChart(final Path outputImage,
                                   final ChartEngine engine,
                                   final DestinationConfig dest) throws IOException,
                                                                 ChartEngineException,
                                                                 XYChartDataSourceException
    {
        if(LOGGER.isWarnEnabled() && outputImage.toFile().exists())
        {
            LOGGER.warn("File {} already existed and is being overwritten.", outputImage);
        }

        final File outputImageFile = outputImage.toFile();

        int width = SystemSettings.getDefaultChartWidth();
        int height = SystemSettings.getDefaultChartHeight();

        if(dest.getGraphical() != null && dest.getGraphical().getWidth() != null)
        {
            width = dest.getGraphical().getWidth();
        }
        if(dest.getGraphical() != null && dest.getGraphical().getHeight() != null)
        {
            height = dest.getGraphical().getHeight();
        }
        ChartTools.generateOutputImageFile(outputImageFile, engine.buildChart(), width, height);
    }    
    
    /**
     * Prevent construction.
     */
    
    private ChartWriter()
    {       
    }
}
