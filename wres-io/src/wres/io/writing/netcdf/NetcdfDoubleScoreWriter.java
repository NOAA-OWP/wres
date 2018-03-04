package wres.io.writing.netcdf;

import wres.config.ProjectConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;

/**
 * Consumes {@link DoubleScoreOutput} into an output file in NetCDF format.
 * 
 * @author jesse
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class NetcdfDoubleScoreWriter extends NetcdfWriter<DoubleScoreOutput>
{

    /**
     * Returns a writer of {@link DoubleScoreOutput}.
     * 
     * @param projectConfig the project configuration
     * @return a writer
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static NetcdfDoubleScoreWriter of( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        return new NetcdfDoubleScoreWriter( projectConfig );
    }

    /**
     * Consumes a map of {@link DoubleScoreOutput} into a NetCDF file.
     * 
     * @param output the score output for one metric at several time windows and thresholds
     */

    @Override
    public void accept( MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> output )
    {

        // Right now, the following is the only information you have about the geospatial index
        // to which the output corresponds. Clearly, more information will be required, 
        // such as the geospatial coordinates, (lat,long), or grid cell index, (row,column).
        // This will need to be added to the {@link Metadata}, and set by wres-io
        // Also note that the identifier is currently optional. The geospatial index will
        // probably need to be optional too and hence tested here, exceptionally, before 
        // writing. Potentially, we could replace the string identifier for the 
        // geospatial id with a FeaturePlus. However, I am reluctant to do this with the way
        // FeaturePlus is currently defined, i.e. verbosely, unclearly. Whatever we use, it 
        // must override equals and probably implement Comparable too.

        String myGeospatialIndexToWrite = output.getMetadata().getIdentifier().getGeospatialID();

        // The output is stored by TimeWindow (M) and Threshold (N). In principle, a DoubleScoreOutput 
        // may contain several score components (O), but the system currently only produces scores 
        // with one component. Thus, each call to accept() will mutate MNO layers in the NetCDF output
        // at ONE geospatial index (at least, the way our current process-by-feature processing works.
        //
        // Bear in mind that accept() is currently called for all metrics (P) that produce DoubleScoreOutput.
        // This is a large number of metrics, potentially. Thus, each NetCDF output file will have 
        // a total of MNOP layers of which MNO are mutated on each call of accept().

        // This is the metric to write: see the API for DoubleScoreOutput w/r to component names
        MetricConstants myMetricToWrite = output.getMetadata().getMetricID();


        // TODO: mutate the netcdf file

    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfig the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    private NetcdfDoubleScoreWriter( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        super( projectConfig );
    }

}
