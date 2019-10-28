package wres.grid.reading;

import thredds.client.catalog.ServiceType;
import ucar.nc2.NetcdfFile;
import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.grid.client.Request;
import wres.grid.client.TimeSeriesResponse;
import wres.system.SystemSettings;
import wres.util.NetCDF;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import ucar.ma2.Array;

import ucar.nc2.dataset.DatasetUrl;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

public class GriddedReader
{
    /**
     * Stands-up resources per JVM.
     */
    
    static {
        NetcdfDataset.initNetcdfFileCache(
                SystemSettings.getMinimumCachedNetcdf(),
                SystemSettings.getMaximumCachedNetcdf(),
                SystemSettings.getHardNetcdfCacheLimit(),
                SystemSettings.getNetcdfCachePeriod()
        );
    }
    
    /**
     * The request.
     */
    
    private final Request request;

    private static final Object READER_LOCK = new Object();

    private static final Map<String, GridFileReader> FILE_READERS = new ConcurrentHashMap<>(  );

    private TimeSeriesResponse timeSeriesResponse;

    private static GridFileReader getReader(final String filePath, final boolean isForecast)
    {
        synchronized ( READER_LOCK )
        {
            GridFileReader reader;

            // If the reader isn't in the collection, add it
            if (!GriddedReader.FILE_READERS.containsKey( filePath ))
            {
                reader = new GridFileReader( filePath, isForecast );
                GriddedReader.FILE_READERS.put( filePath, reader );
            }
            else
            {
                reader = GriddedReader.FILE_READERS.get(filePath);
            }

            return reader;
        }
    }

    public GriddedReader( Request request )
    {
        Objects.requireNonNull( request );
        
        this.request = request;
    }

    private TimeSeriesResponse getTimeSeriesResponse()
    {
        if (this.timeSeriesResponse == null)
        {
            this.timeSeriesResponse = new TimeSeriesResponse();
            this.timeSeriesResponse.setVariableName( this.request.getVariableName() );
        }

        return this.timeSeriesResponse;
    }


    public TimeSeriesResponse getData() throws IOException
    {
        GridValue value;

        Queue<String> paths = new LinkedList<>( this.request.getPaths() );

        while (! paths.isEmpty() )
        {
            String path = paths.remove();
            GridFileReader reader = GriddedReader.getReader( path, this.request.isForecast() );

            if (reader.isLocked())
            {
                paths.add( path );
            }
            else
            {
                for ( Feature feature : this.request.getFeatures() )
                {
                    FeaturePlus featurePlus = FeaturePlus.of( feature );

                    // We'll need to eventually iterate through time, but now is
                    // not the... time!
                    value = reader.read(
                            null,
                            this.request.getVariableName(), 
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
        GridFileReader(final String path, final boolean isForecast)
        {
            this.path = path;
            this.isForecast = isForecast;
            this.readLock = new ReentrantLock(  );
        }

        GridValue read(Integer time, String variableName, final double latitude, final double longitude)
                throws IOException
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
                GridDatatype variable = gridDataset.findGridDatatype( variableName );

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

        private Instant getValidTime() throws IOException
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

        private Instant getIssueTime() throws IOException
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
    }
}
