package wres.io.writing;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import ucar.nc2.NetcdfFile;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.outputs.ScoreOutput;

/**
 * Manages netcdf outputs for a project. Consumes MetricOutputs.
 */

public class NetcdfOutputWriter implements Consumer<MetricOutput<?>>
{
    private final ProjectConfig projectConfig;
    private final List<NetcdfFile> files;

    public NetcdfOutputWriter( ProjectConfig projectConfig )
    {
        this.projectConfig = projectConfig;
        int count = NetcdfOutputWriter.countNetcdfOutputFiles( projectConfig );
        this.files = new ArrayList<>( count );
    }

    @Override
    public void accept( MetricOutput<?> metricOutput )
    {
        if ( metricOutput instanceof BoxPlotOutput )
        {
            BoxPlotOutput output = (BoxPlotOutput) metricOutput;
            // Write something boxplotty
        }
        else if ( metricOutput instanceof MatrixOutput )
        {
            MatrixOutput output = (MatrixOutput) metricOutput;
            // Write something matrixy
        }
        else if ( metricOutput instanceof ScoreOutput )
        {
            ScoreOutput output = (ScoreOutput) metricOutput;
            // Write something scorey
        }
        else if ( metricOutput instanceof MultiVectorOutput )
        {
            MultiVectorOutput output = (MultiVectorOutput) metricOutput;
            // Write something multivectory
        }
        else if ( metricOutput instanceof PairedOutput )
        {
            PairedOutput output = (PairedOutput) metricOutput;
            // Write something pairedy
        }
        else
        {
            throw new UnsupportedOperationException( "Unknown MetricOutput" );
        }
    }

    private static int countNetcdfOutputFiles( ProjectConfig config )
    {
        Objects.requireNonNull( config );

        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            return 0;
        }

        int countOfNetcdfOutputs = 0;

        for ( DestinationConfig destinationConfig : config.getOutputs().getDestination() )
        {
            if ( destinationConfig.getType().equals( DestinationType.NET_CDF ) )
            {
                countOfNetcdfOutputs++;
            }
        }

        return countOfNetcdfOutputs;
    }
}
