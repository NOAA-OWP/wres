package wres.io.writing.netcdf;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import ucar.nc2.NetcdfFile;
import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.io.writing.WriterHelper;

/**
 * Manages netcdf outputs for a project. Consumes a {@link MetricOutputMapByTimeAndThreshold}. Writers of concrete 
 * types should extend this class.
 * 
 * @author jesse.bickel@***REMOVED***
 */

abstract class NetcdfWriter<T extends MetricOutput<?>> implements Consumer<MetricOutputMapByTimeAndThreshold<T>>
{
    /**
     * Project configuration.
     */

    final ProjectConfig projectConfig;

    /**
     * List of output files to write.
     */

    final List<NetcdfFile> files;

    /**
     * Build a writer.
     * 
     * @param projectConfig the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    NetcdfWriter( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        WriterHelper.validateProjectForWriting( projectConfig );
        this.projectConfig = projectConfig;
        int count = NetcdfWriter.countNetcdfOutputFiles( projectConfig );
        this.files = new ArrayList<>( count );
    }

    /**
     * Returns the count of output files required.
     * 
     * @param config the project configuration
     * @return the number of files required
     */

    static int countNetcdfOutputFiles( ProjectConfig config )
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
            if ( destinationConfig.getType().equals( DestinationType.NETCDF ) )
            {
                countOfNetcdfOutputs++;
            }
        }

        return countOfNetcdfOutputs;
    }
}
