package wres.io.griddedReader;

import org.apache.commons.lang3.tuple.Pair;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import wres.config.generated.*;
import wres.grid.client.*;
import wres.config.FeaturePlus;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;

import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.GridDataset;
import ucar.nc2.dt.GridCoordSystem;

public class GriddedReader
{

    private int x_index;
    private int y_index;
    private GriddedCoordinate coordinate;
    private String variable_name;
    private String fileName;

    private List<Pair<Integer, Integer>> indices = new ArrayList<>();
    private List<String> paths = new ArrayList<>();
    private List<Feature> features = new ArrayList<>();

    private TimeSeriesResponse timeSeriesResponse;



    private static final Logger LOGGER = LoggerFactory.getLogger (GriddedReader.class);

    public GriddedReader (Request request)
    {

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

    public List<String> getPaths () {return paths;}

    public List<Pair<Integer, Integer>> getIndices() {
        return indices;
    }

    public List<Feature> getFeatures() {
        return features;
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

    NetcdfFile ncfile = null;
    Attribute timeAtt1, timeAtt2;
    String issuenceTime, validTime;

    public TimeSeriesResponse getData( List<String> paths )
    {
        for (String filePath:paths)
        {

            try
            {
                ncfile = NetcdfFile.open(filePath);
            }
            catch (IOException ioe)
            {
                LOGGER.info(ioe.toString());
            }

            timeAtt1 = ncfile.findGlobalAttribute("model_initialization_time");
            issuenceTime = timeAtt1.getStringValue();
            //timeSeriesResponse.issuedDate.add (issuenceTime);

            timeAtt2 = ncfile.findGlobalAttribute("model_output_valid_time");
            validTime = timeAtt2.getStringValue();
            //timeSeriesResponse.lastLead.add (validTime);

            Double value = 0.0;

            for (Feature feature : this.getFeatures())
            {

                FeaturePlus plus = FeaturePlus.of( feature );

                // value = the value from the feature in the grid

                timeSeriesResponse.add( plus,
                                        Instant.parse( issuenceTime ),
                                        Instant.parse( validTime ),
                                        value );
            }

            try {
                ncfile.close();
            }

            catch (IOException ioe)
            {
                LOGGER.info(ioe.toString());
            }

            GridDataset gds = null;
            try
            {
                gds = ucar.nc2.dt.grid.GridDataset.open(filePath);
            }
            catch (IOException ioe)
            {
                LOGGER.info(ioe.toString());
            }

            // -----------------------------------------------------------------------
// GridCoordSystem to find the value of a grid a a specific lat, lon point

            GridDatatype grid = gds.findGridDatatype(variable_name);
            GridCoordSystem gcs = null;
            gcs = grid.getCoordinateSystem();

            // find the x,y index for a specific lat/lon position

            Array data = null;

            int[] xy = gcs.findXYindexFromLatLon(x_index, y_index, null); // xy[0] = x, xy[1] = y


// read the data at that lat, lon and the first time and z level (if any)

            try
            {
                data = grid.readDataSlice(0, 0, xy[1], xy[0]); // note order is t, z, y, x
            }
            catch (IOException ioe)
            {
                LOGGER.info(ioe.toString());
            }

            double val = data.getDouble(0); // we know its a scalar
            //timeSeriesResponse.value.add ( val );

            //LOGGER.info("Value at %f %f == %f%n", x_index, y_index, val);
        }

        return timeSeriesResponse;
    }


}