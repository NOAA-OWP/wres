package wres.io.griddedReader;

import wres.config.generated.*;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;

import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.GridDataset;
import ucar.nc2.dt.GridCoordSystem;

public class GriddedReader
{
    private GriddedPath filePath;
    private int x_index;
    private int y_index;
    private GriddedCoordinate coordinate;
    private String variable_name;
    private String fileName;

    private static final Logger LOGGER = LoggerFactory.getLogger (GriddedReader.class);

    public GriddedReader ( GriddedPath filePath, int x_index, int y_index, GriddedCoordinate coordinate, String variable_name, String fileName)
    {
        this.filePath= filePath;
        this.x_index = x_index;
        this.y_index= y_index;
        this.coordinate= coordinate;
        this.variable_name= variable_name;

        if (fileName == null || fileName.isEmpty())
        {
            throw new IllegalArgumentException("Invalid Filename");
        }
        else
            this.fileName= fileName;
    }

    public GriddedPath getFilePath() {
        return filePath;
    }

    public int getX_index()
    {
        return x_index;
    }

    public int getY_index() { return y_index; }

    public GriddedCoordinate getCoordinate() {
        return coordinate;
    }

    public String getFileName()
    {
        return fileName;
    }

    public String getVariable_name() {
        return variable_name;
    }

    public void getData(String fileName )
    {
        GridDataset gds= null;
        try
        {
            gds = ucar.nc2.dt.grid.GridDataset.open( fileName );
        }
        catch (IOException ioe)
        {
            LOGGER.info(ioe.toString());
        }

        // -----------------------------------------------------------------------
// GridCoordSystem to find the value of a grid a a specific lat, lon point

        GridDatatype grid = gds.findGridDatatype( variable_name );
        GridCoordSystem gcs = null;
        gcs= grid.getCoordinateSystem();

        // find the x,y index for a specific lat/lon position
        int[] xy = gcs.findXYindexFromLatLon( x_index, y_index, null ); // xy[0] = x, xy[1] = y

// read the data at that lat, lon and the first time and z level (if any)
        Array data= null;
        try
        {
            data = grid.readDataSlice(0, 0, xy[1], xy[0]); // note order is t, z, y, x
        }
        catch (IOException ioe)
        {
            LOGGER.info(ioe.toString());
        }

        double val = data.getDouble(0 ); // we know its a scalar
        LOGGER.info ( "Value at %f %f == %f%n", x_index, y_index, val );


    }


}