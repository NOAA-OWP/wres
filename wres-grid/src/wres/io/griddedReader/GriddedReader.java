package wres.io.griddedReader;

import thredds.client.catalog.ServiceType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.grid.client.Request;
import wres.grid.client.TimeSeriesResponse;
import wres.system.SystemSettings;
import wres.util.NetCDF;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;

import ucar.nc2.dataset.DatasetUrl;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

public class GriddedReader
{
    private String variable_name;

    private Queue<String> paths;
    private List<Feature> features;

    static {
        NetcdfDataset.initNetcdfFileCache(
                SystemSettings.getMinimumCachedNetcdf(),
                SystemSettings.getMaximumCachedNetcdf(),
                SystemSettings.getHardNetcdfCacheLimit(),
                SystemSettings.getNetcdfCachePeriod()
        );
    }

    private static final Object READER_LOCK = new Object();

    private static final Map<String, GridFileReader> FILE_READERS = new ConcurrentHashMap<>(  );

    private TimeSeriesResponse timeSeriesResponse;

    private final boolean isForecast;

    private static GridFileReader getReader(final String filePath, final String variableName, final boolean isForecast)
            throws IOException
    {
        synchronized ( READER_LOCK )
        {
            GridFileReader reader;

            // If the reader isn't in the collection, add it
            if (!GriddedReader.FILE_READERS.containsKey( filePath ))
            {
                reader = new GridFileReader( filePath, variableName, isForecast );
                GriddedReader.FILE_READERS.put( filePath, reader );
            }
            else
            {
                reader = GriddedReader.FILE_READERS.get(filePath);
            }

            return reader;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger (GriddedReader.class);

    public GriddedReader (Request request)
    {
        this.isForecast = request.getIsForecast();
        this.paths = request.getPaths();
        this.features = request.getFeatures();
        this.variable_name = request.getVariableName();
    }

    public Queue<String> getPaths () {return paths;}

    public List<Feature> getFeatures() {
        return features;
    }

    public String getVariable_name() {
        return variable_name;
    }

    private TimeSeriesResponse getTimeSeriesResponse()
    {
        if (this.timeSeriesResponse == null)
        {
            this.timeSeriesResponse = new TimeSeriesResponse();
            this.timeSeriesResponse.setVariableName( this.getVariable_name() );
        }

        return this.timeSeriesResponse;
    }


    public TimeSeriesResponse getData( ) throws IOException, InvalidRangeException
    {
        GridValue value;

        while (!this.paths.isEmpty())
        {
            String path = this.paths.remove();
            GridFileReader reader = GriddedReader.getReader( path, this.getVariable_name(), this.isForecast );

            if (reader.isLocked())
            {
                this.paths.add(path);
            }
            else
            {
                for ( Feature feature : this.getFeatures() )
                {
                    FeaturePlus featurePlus = FeaturePlus.of( feature );

                    // We'll need to eventually iterate through time, but now is
                    // not the... time!
                    value = reader.read(
                            null,
                            feature.getCoordinate().getLatitude(),
                            feature.getCoordinate().getLongitude()
                    );

                    this.getTimeSeriesResponse().add(
                            featurePlus,
                            value.getIssueTime(),
                            value.getValidTime(),
                            value.getValue(),
                            value.getMeasurementUnit()
                    );
                }
            }
        }

        return this.getTimeSeriesResponse();
    }

    private static class GridValue
    {
        GridValue(
                final double value,
                final String measurementUnit,
                final Instant issueTime,
                final Instant validTime
        )
        {
            this.value = value;
            this.measurementUnit = measurementUnit;
            this.issueTime = issueTime;
            this.validTime = validTime;
        }

        double getValue()
        {
            return value;
        }

        Instant getIssueTime()
        {
            return this.issueTime;
        }

        Instant getValidTime()
        {
            return this.validTime;
        }

        String getMeasurementUnit()
        {
            return this.measurementUnit;
        }

        private final String measurementUnit;
        private final double value;
        private final Instant issueTime;
        private final Instant validTime;
    }

    private static class GridFileReader
    {
        GridFileReader(final String path, final String variableName, final boolean isForecast)
        {
            this.path = path;
            this.isForecast = isForecast;
            this.variableName = variableName;
            this.readLock = new ReentrantLock(  );
        }

        GridValue read(Integer time, final double latitude, final double longitude)
                throws IOException, InvalidRangeException
        {
            if (time == null)
            {
                time = 0;
            }

            this.readLock.lock();

            // This is underlying THREDDS code. It generally expects some semi-remote location for its data, but we're local, so we're using
            DatasetUrl url = new DatasetUrl( ServiceType.File, this.path );
            try (NetcdfDataset dataset = NetcdfDataset.acquireDataset( url, null ); GridDataset gridDataset = new GridDataset( dataset ))
            {
                GridDatatype variable = gridDataset.findGridDatatype( this.variableName );

                // Returns XY from YX parameters
                int[] xIndexYIndex = variable.getCoordinateSystem().findXYindexFromLatLon( latitude, longitude, null );

                // readDataSlice takes (time, z, y, x) as parameters. Since the previous call was XY, we need to flip
                // the two, yielding indexes 1 then 0
                Array data = variable.readDataSlice( time, 0, xIndexYIndex[1], xIndexYIndex[0] );

                double value = data.getDouble( 0 );
                return new GridValue( value, variable.getUnitsString(), this.getIssueTime(), this.getValidTime() );
            }
            finally
            {
                this.readLock.unlock();
            }
        }

        boolean isLocked()
        {
            return this.readLock.isLocked();
        }

        private Instant getValidTime() throws IOException, InvalidRangeException
        {
            if (this.validTime == null)
            {
                DatasetUrl url = new DatasetUrl( ServiceType.File, this.path );
                try (NetcdfFile file = NetcdfDataset.acquireFile( url, null ))
                {
                    this.validTime = NetCDF.getTime( file );
                }
            }

            return this.validTime;
        }

        private Instant getIssueTime() throws IOException, InvalidRangeException
        {
            if (this.issueTime == null && this.isForecast)
            {
                DatasetUrl url = new DatasetUrl( ServiceType.File, this.path );
                try (NetcdfFile file = NetcdfDataset.acquireFile( url, null ))
                {
                    this.issueTime = NetCDF.getReferenceTime( file );
                }
            }
            else if (this.issueTime == null)
            {
                this.issueTime = this.getValidTime();
            }

            return this.issueTime;
        }

        private final String path;
        private Instant issueTime = null;
        private Instant validTime = null;
        private final ReentrantLock readLock;
        private final boolean isForecast;
        private final String variableName;
    }
}