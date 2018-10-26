package wres.io.reading.nwm;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.grid.GridDataset;
import ucar.unidata.geoloc.LatLonPoint;

import wres.io.concurrency.WRESRunnable;
import wres.io.utilities.DataBuilder;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.system.SystemSettings;
import wres.util.FutureQueue;
import wres.util.NetCDF;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
class GridManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GridManager.class);

    private static final Object PROJECTION_LOCK = new Object();
    private static final Map<GridMetadata, Integer> ENCOUNTERED_PROJECTIONS = new HashMap<>();

    static int addGrid(final NetcdfFile file) throws SQLException
    {
        GridMetadata metadata = new GridMetadata( file );

        synchronized ( PROJECTION_LOCK )
        {
            if ( GridManager.ENCOUNTERED_PROJECTIONS.containsKey( metadata ))
            {
                LOGGER.trace("The grid in {} has already been encountered. Skipping ingest.", metadata.path);
            }
            else if (metadata.hasLoadCompleted())
            {
                GridManager.ENCOUNTERED_PROJECTIONS.put( metadata, metadata.getGridProjectionId() );
                LOGGER.trace("Data for the grid in {} has already been ingested. Skipping ingest.", metadata.path);
            }
            else
            {
                WRESRunnable ingestor = new CoordinateIngestor( metadata );
                Database.ingest(ingestor);
                GridManager.ENCOUNTERED_PROJECTIONS.put( metadata, metadata.getGridProjectionId() );
            }

            return GridManager.ENCOUNTERED_PROJECTIONS.get(metadata);
        }
    }

    private static class GridMetadata
    {
        private GridMetadata(final NetcdfFile file)
        {
            this.path = file.getLocation();

            Variable xCoordinates = NetCDF.getVariable( file, "x" );
            Variable yCoordinates = NetCDF.getVariable( file, "y" );
            Variable coordinateSystem = NetCDF.getVariable( file, "ProjectionCoordinateSystem" );

            this.srText = coordinateSystem.findAttValueIgnoreCase( "esri_pe_string", "" );
            this.proj4 = coordinateSystem.findAttValueIgnoreCase( "proj4", "" );
            this.projectionMapping =
                    coordinateSystem.findAttValueIgnoreCase( "grid_mapping_name", "lambert_conformal_conic" );


            this.xResolution = xCoordinates.findAttribute( "resolution" ).getNumericValue();
            this.yResolution = yCoordinates.findAttribute( "resolution" ).getNumericValue();
            this.xSize = xCoordinates.getSize();
            this.ySize = yCoordinates.getSize();
            this.xUnit = xCoordinates.findAttValueIgnoreCase( "units", "" );
            this.yUnit = yCoordinates.findAttValueIgnoreCase( "units", "" );
            this.xType = xCoordinates.findAttValueIgnoreCase( "_CoordinateAxisType", "GeoX" );
            this.yType = yCoordinates.findAttValueIgnoreCase( "_CoordinateAxisType", "GeoY" );
        }

        private void loadMetadata() throws SQLException
        {
            DataScripter script = new DataScripter();
            script.addLine( "WITH new_projection AS" );
            script.addLine( "(" );
            script.addTab().addLine( "INSERT INTO wres.GridProjection (" );
            script.addTab( 2 ).addLine( "srtext," );
            script.addTab( 2 ).addLine( "proj4," );
            script.addTab( 2 ).addLine( "projection_mapping," );
            script.addTab( 2 ).addLine( "x_resolution," );
            script.addTab( 2 ).addLine( "y_resolution," );
            script.addTab( 2 ).addLine( "x_unit," );
            script.addTab( 2 ).addLine( "y_unit," );
            script.addTab( 2 ).addLine( "x_type," );
            script.addTab( 2 ).addLine( "y_type," );
            script.addTab( 2 ).addLine( "x_size," );
            script.addTab( 2 ).addLine( "y_size" );
            script.addTab().addLine( ")" );
            script.addTab().addLine( "SELECT ?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?," );
            script.addTab( 2 ).addLine( "?" );
            script.addTab().addLine( "WHERE NOT EXISTS (" );
            script.addTab( 2 ).addLine( "SELECT 1" );
            script.addTab( 2 ).addLine( "FROM wres.GridProjection" );
            script.addTab( 2 ).addLine( "WHERE srtext = ?" );
            script.addTab(  3  ).addLine( "AND proj4 = ?" );
            script.addTab(  3  ).addLine( "AND x_resolution = ?" );
            script.addTab(  3  ).addLine( "AND y_resolution = ?" );
            script.addTab(  3  ).addLine( "AND x_unit = ?" );
            script.addTab(  3  ).addLine( "AND y_unit = ?" );
            script.addTab(  3  ).addLine( "AND x_type = ?" );
            script.addTab(  3  ).addLine( "AND y_type = ?" );
            script.addTab(  3  ).addLine( "AND x_size = ?" );
            script.addTab(  3  ).addLine( "AND y_size = ?" );
            script.addTab().addLine( ")" );
            script.addTab().addLine( "RETURNING gridprojection_id" );
            script.addLine( ")" );
            script.addLine( "SELECT gridprojection_id, false AS load_complete" );
            script.addLine( "FROM new_projection" );
            script.addLine();
            script.addLine( "UNION" );
            script.addLine();
            script.addLine( "SELECT gridprojection_id, load_complete" );
            script.addLine( "FROM wres.GridProjection" );
            script.addLine( "WHERE srtext = ?" );
            script.addTab().addLine( "AND proj4 = ?" );
            script.addTab().addLine( "AND x_resolution = ?" );
            script.addTab().addLine( "AND y_resolution = ?" );
            script.addTab().addLine( "AND x_unit = ?" );
            script.addTab().addLine( "AND y_unit = ?" );
            script.addTab().addLine( "AND x_type = ?" );
            script.addTab().addLine( "AND y_type = ?" );
            script.addTab().addLine( "AND x_size = ?" );
            script.addTab().addLine( "AND y_size = ?;" );

            // "Insert new projection" arguments
            script.addArgument( srText );
            script.addArgument( proj4 );
            script.addArgument( projectionMapping );
            script.addArgument( xResolution );
            script.addArgument( yResolution );
            script.addArgument( xUnit );
            script.addArgument( yUnit );
            script.addArgument( xType );
            script.addArgument( yType );
            script.addArgument( xSize );
            script.addArgument( ySize );

            // "Where Projection doesn't exist" arguments
            script.addArgument( srText );
            script.addArgument( proj4 );
            script.addArgument( xResolution );
            script.addArgument( yResolution );
            script.addArgument( xUnit );
            script.addArgument( yUnit );
            script.addArgument( xType );
            script.addArgument( yType );
            script.addArgument( xSize );
            script.addArgument( ySize );

            // "Select where DOES exist" arguments
            script.addArgument( srText );
            script.addArgument( proj4 );
            script.addArgument( xResolution );
            script.addArgument( yResolution );
            script.addArgument( xUnit );
            script.addArgument( yUnit );
            script.addArgument( xType );
            script.addArgument( yType );
            script.addArgument( xSize );
            script.addArgument( ySize );

            try ( DataProvider data = script.getData())
            {
                gridProjectionId = data.getInt( "gridprojection_id" );
                loadComplete = data.getBoolean( "load_complete" );
            }
        }

        private int getGridProjectionId() throws SQLException
        {
            if (this.gridProjectionId == null)
            {
                this.loadMetadata();
            }
            return this.gridProjectionId;
        }

        private boolean hasLoadCompleted() throws SQLException
        {
            if (this.loadComplete == null)
            {
                this.loadMetadata();
            }

            return this.loadComplete;
        }

        private void completeLoad() throws SQLException
        {
            if (!this.loadComplete)
            {
                DataScripter script = new DataScripter();

                script.addLine( "UPDATE wres.GridProjection" );
                script.addTab().addLine( "SET load_complete = true" );
                script.addLine( "WHERE gridprojection_id = ?;" );

                script.addArgument( this.getGridProjectionId() );

                script.execute();
            }
        }

        @Override
        public int hashCode()
        {
            if (this.hash == null)
            {
                this.hash = Objects.hash(
                        srText,
                        proj4,
                        projectionMapping,
                        xResolution,
                        yResolution,
                        xSize,
                        ySize,
                        xUnit,
                        yUnit,
                        xType,
                        yType
                );
            }

            return this.hash;
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof GridMetadata && obj.hashCode() == this.hashCode();
        }

        @Override
        public String toString()
        {
            return this.proj4;
        }

        private final String path;

        private final String srText;
        private final String proj4;
        private final String projectionMapping;
        private final String xUnit;
        private final String yUnit;
        private final String xType;
        private final String yType;

        private final Number xResolution;
        private final Number yResolution;

        private final Long xSize;
        private final Long ySize;

        private Integer hash;

        private Integer gridProjectionId;
        private Boolean loadComplete;
    }

    private static class CoordinateIngestor extends WRESRunnable
    {
        private CoordinateIngestor(final GridMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        protected void execute() throws SQLException, IOException
        {
            LOGGER.trace("Ingesting {}...", this.metadata);
            this.removePreexistingCoordinates();

            this.addCopyTasks();

            this.finishCopyTasks();

            this.metadata.completeLoad();

            LOGGER.trace("The ingest for {} has completed.", this.metadata);
        }

        private void removePreexistingCoordinates() throws SQLException
        {
            LOGGER.trace("Removing previous entries for the grid: {}", this.metadata);
            DataScripter clearScript = new DataScripter(  );

            clearScript.addLine( "DELETE FROM wres.NetCDFCoordinate" );
            clearScript.addLine( "WHERE gridprojection_id = ?;");

            clearScript.addArgument( this.metadata.getGridProjectionId() );

            clearScript.execute();
        }

        private void addCopyTasks() throws IOException, SQLException
        {
            DataBuilder builder = DataBuilder.with(
                    "gridprojection_id",
                    "x_position",
                    "y_position",
                    "x",
                    "y",
                    "geographic_coordinate"
            );

            try (NetcdfFile file = NetcdfFile.open(this.metadata.path))
            {
                GridDataset grid = new GridDataset( new NetcdfDataset( file ) );
                GridCoordSystem coordinateSystem = grid.getGrids().get( 0 ).getCoordinateSystem();

                Variable xCoordinates = NetCDF.getVariable( file, "x" );
                Variable yCoordinates = NetCDF.getVariable( file, "y" );

                for ( int xIndex = 0; xIndex < xCoordinates.getSize(); ++xIndex )
                {
                    for (int yIndex = 0; yIndex < yCoordinates.getSize(); ++yIndex)
                    {
                        LatLonPoint point = coordinateSystem.getLatLon( xIndex, yIndex );
                        builder.addRow();
                        builder.set( "gridprojection_id", this.metadata.getGridProjectionId() );
                        builder.set( "x_position", xIndex );
                        builder.set( "y_position", yIndex );

                        try
                        {
                            builder.set( "x", xCoordinates.read( new int[]{xIndex}, new int[]{1} ));
                        }
                        catch ( InvalidRangeException e )
                        {
                            throw new IOException(
                                    "The x value at position " +
                                    xIndex +
                                    " could not be loaded from " +
                                    this.metadata.path
                            );
                        }

                        try
                        {
                            builder.set( "y", yCoordinates.read( new int[]{yIndex}, new int[]{1} ));
                        }
                        catch ( InvalidRangeException e )
                        {
                            throw new IOException(
                                    "The y value at position " +
                                    yIndex +
                                    " could not be loaded from " +
                                    this.metadata.path
                            );
                        }

                        builder.set( "geographic_coordinate", "(" + point.getLongitude() + "," + point.getLatitude() + ")");

                        if ( builder.getRowCount() >= SystemSettings.getMaximumCopies())
                        {
                            Future copy = builder.build().copy( "wres.NetcdfCoordinate", true );
                            this.copyQueue.add( copy );
                            //this.copyTasks.add( copy );
                            LOGGER.trace("Job to copy {} coordinates for {} dispatched.",
                                         SystemSettings.getMaximumCopies(),
                                         this.metadata);
                        }
                    }
                }

                if (builder.getRowCount() > 0)
                {
                    Future copy = builder.build().copy( "wres.NetcdfCoordinate", true );
                    this.copyQueue.add( copy );
                    //this.copyTasks.add( copy );
                    LOGGER.trace("Job to copy the last coordinates for {} dispatched.", this.metadata);
                }
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "Grid data could not be read and saved.", e );
            }
        }

        private void finishCopyTasks() throws IOException
        {
            LOGGER.trace("Waiting for coordinate copy operations for {} to finish.", this.metadata);

            try
            {
                this.copyQueue.loop();
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "Grid projection copy operation failed.", e );
            }
            LOGGER.trace("All copy operations for the coordinates belonging to {} completed.", this.metadata);
        }

        @Override
        protected Logger getLogger()
        {
            return LOGGER;
        }

        private final GridMetadata metadata;
        //private final Queue<Future> copyTasks = new LinkedList<>();
        private final FutureQueue copyQueue = new FutureQueue(  );
    }
}
