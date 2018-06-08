package wres.io.griddedReader;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.grid.client.Request;
import wres.grid.client.TimeSeriesResponse;
import wres.util.NetCDF;

import java.io.Closeable;
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

import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.dt.GridCoordSystem;

public class GriddedReader
{
    private String variable_name;

    private Queue<String> paths;
    private List<Feature> features;

    private static final short MAX_READER_LIFESPAN = 2000;
    private static final short MAX_OPEN_FILES = 60;
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
                //LOGGER.info("Creating the missing reader for {}", filePath);
                reader = new GridFileReader( filePath, variableName, isForecast );
                GriddedReader.FILE_READERS.put( filePath, reader );
            }
            else
            {
                //LOGGER.info("Getting the preexisting reader for {}", filePath);
                reader = GriddedReader.FILE_READERS.get(filePath);
            }

            /*if (GriddedReader.FILE_READERS.size() < GriddedReader.MAX_OPEN_FILES)
            {
                return reader;
            }

            // Now that we've ensured that we have a reader, we want to look
            // through our current readers and see if we need to expire any.
            // This won't necessarily be 100% accurate due to variances in
            // timing, but it doesn't need to be. If what is marked as the
            // least recently used is somehow currently in use, the close
            // operation will wait until that operation is complete. If a
            // previously closed reader is passed back, the reader will
            // "reopen" for use.

            int openFileCount = 0;

            // Record the current time so it doesn't change throughout evaluation
            long now = Instant.now().toEpochMilli();
            GridFileReader leastRecentlyUsed = null;
            long mostSinceUsed = Long.MAX_VALUE;

            LOGGER.info("Finding readers that need to be closed.");
            // For every file in the collection...
            for (GridFileReader oldReader : GriddedReader.FILE_READERS.values())
            {
                Instant lastUsedInstant = oldReader.getLastUse();
                // Skip over this reader if it hasn't been used or is closed
                // (this will skip over any newly created reader)
                if (reader == oldReader || lastUsedInstant == null || !oldReader.isOpen())
                {
                    continue;
                }

                // Record the last time this reader was used...
                long lastUsed = lastUsedInstant.toEpochMilli();

                // Record the amount of time between now and the last time
                // this reader was used
                long sinceUsed = now - lastUsed;

                // If too much time has passed since this was last used, close it
                if (sinceUsed > GriddedReader.MAX_READER_LIFESPAN)
                {
                    LOGGER.info("Closing the reader for {} because it hasn't been used in a while.", oldReader.path);
                    oldReader.close();
                    LOGGER.info("The reader for {} has been closed.", oldReader.path);
                }
                else
                {
                    // Otherwise, increase the count of open files
                    openFileCount++;

                    // If this reader was least recently used than the last
                    // reader we looked at
                    if (sinceUsed < mostSinceUsed)
                    {
                        // Record this as being the least recently used reader
                        leastRecentlyUsed = oldReader;
                        mostSinceUsed = sinceUsed;
                    }
                }
            }

            LOGGER.info("Done looking through files to close.");

            // If there are too many open files, close the least recently used file
            if (openFileCount > GriddedReader.MAX_OPEN_FILES)
            {
                LOGGER.info("Since too many files are open, we're going to "
                            + "close the reader for {} since we think it has "
                            + "been open the longest without use.", leastRecentlyUsed.path);
                leastRecentlyUsed.close();
            }*/

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


    public TimeSeriesResponse getData( )
            throws IOException, InvalidRangeException
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
                try
                {
                    reader.lock();
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
                finally
                {
                    reader.unlock();
                }
            }
        }
        /*for (String filePath : this.getPaths())
        {
            GridFileReader reader = GriddedReader.getReader( filePath, this.getVariable_name(), this.isForecast );
            try
            {
                reader.lock();
                for ( Feature feature : this.getFeatures() )
                {
                    FeaturePlus featurePlus = FeaturePlus.of( feature );

                    LOGGER.info("{} -> Finding data for ({},{})",
                                Thread.currentThread().getName(),
                                feature.getCoordinate().getLongitude(),
                                feature.getCoordinate().getLatitude());

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
            finally
            {
                reader.unlock();
            }
        }*/

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

    private static class GridFileReader implements Closeable
    {
        GridFileReader(final String path, final String variableName, final boolean isForecast)
        {
            this.path = path;
            this.isForecast = isForecast;
            this.variableName = variableName;
            this.readLock = new ReentrantLock(  );
        }

        GridValue read(Integer time, final double x, final double y)
                throws IOException, InvalidRangeException
        {
            if (time == null)
            {
                time = 0;
            }

            this.readLock.lock();

            try
            {
                int[] xIndexYIndex = this.getCoordinateSystem().findXYindexFromLatLon( x, y, null );

                Array data = this.getVariable().readDataSlice( time, 0, xIndexYIndex[0], xIndexYIndex[1] );

                double value = data.getDouble( 0 );
                //this.lastUse = Instant.now();
                return new GridValue( value, this.getVariable().getUnitsString(), this.getIssueTime(), this.getValidTime() );
            }
            finally
            {
                this.readLock.unlock();
            }
        }

        public boolean isOpen()
        {
            return this.dataset != null;
        }

        public boolean isLocked()
        {
            return this.readLock.isLocked();
        }

        void lock()
        {
            this.readLock.lock();
        }

        void unlock() throws IOException
        {
            this.close();
            this.readLock.unlock();
        }

        @Override
        public void close() throws IOException
        {
            this.readLock.lock();
            try
            {
                this.coordinateSystem = null;
                this.variable = null;
                this.dataset.close();
                this.dataset = null;
            }
            finally
            {
                this.readLock.unlock();
                //LOGGER.info("The reader for {} has been closed.", this.path);
            }
        }

        private GridCoordSystem getCoordinateSystem() throws IOException
        {
            if (this.coordinateSystem == null)
            {
                this.coordinateSystem = this.getVariable().getCoordinateSystem();
            }

            return this.coordinateSystem;
        }

        private GridDataset getDataset() throws IOException
        {
            if (this.dataset == null)
            {
                this.dataset = GridDataset.open( this.path );
            }

            return this.dataset;
        }

        private GridDatatype getVariable() throws IOException
        {
            if (this.variable == null)
            {
                this.variable = this.getDataset().findGridDatatype( this.variableName );
            }

            return this.variable;
        }

        private Instant getValidTime() throws IOException, InvalidRangeException
        {
            if (this.validTime == null)
            {
                try (NetcdfFile file = NetcdfFile.open(this.path))
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
                try (NetcdfFile file = NetcdfFile.open(this.path))
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

        Instant getLastUse()
        {
            this.readLock.lock();

            try
            {
                return this.lastUse;
            }
            finally
            {
                this.readLock.unlock();
            }
        }

        private GridDataset dataset;
        private GridDatatype variable;
        private GridCoordSystem coordinateSystem;

        private final String path;
        private Instant issueTime = null;
        private Instant validTime = null;
        private final ReentrantLock readLock;
        private final boolean isForecast;
        private final String variableName;
        private Instant lastUse = null;
    }
}