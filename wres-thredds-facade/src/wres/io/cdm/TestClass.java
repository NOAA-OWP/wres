/*
This is a test class to check CDM grid feature type of 
NetCDFDataset
*/

package wres.io.cdm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.lang.Object;
import java.util.Formatter;
import java.lang.Comparable;

import ucar.ma2.Array;
import ucar.ma2.Range;

import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dt.grid.*;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.GridDataset;
import ucar.nc2.dt.GridCoordSystem;


public class TestClass 
{

    public static void main( String[] args ) throws IOException
    {


// Opening a Gridset        

//    GridDataset gds = ucar.nc2.dt.grid.GridDataset.open("localhost:8080//thredds/fileServer/short_range/nwm.t00z.short_range.terrain_rt.f001.conus.nc");       
 
    String filename= "/home/sanian.gaffar/short_range/20170821/nwm.t23z.short_range.terrain_rt.f018.conus.nc";
    GridDataset gds= null;
    try 
    {
        gds = ucar.nc2.dt.grid.GridDataset.open(filename);
    } 
    catch (IOException ioe) 
    {
        System.out.println(ioe.toString());
    }

// -----------------------------------------------------------------------
// GridCoordSystem to find the value of a grid a a specific lat, lon point

    GridDatatype grid = gds.findGridDatatype( "sfcheadsubrt");
    GridCoordSystem gcs = null; 
    gcs= grid.getCoordinateSystem();

    double lat = 8.0;
    double lon = 21.0;

// find the x,y index for a specific lat/lon position
    int[] xy = gcs.findXYindexFromLatLon(lat, lon, null); // xy[0] = x, xy[1] = y
  
// read the data at that lat, lon and the first time and z level (if any) 
    Array data  = grid.readDataSlice(0, 0, xy[1], xy[0]); // note order is t, z, y, x
    double val = data.getDouble(0); // we know its a scalar
    System.out.printf("Value at %f %f == %f%n", lat, lon, val);


//------------------------------------------------------------------------------
// Create a logical subset of a GeoGrid using index Ranges

//    GridDatatype subset = grid.makeSubset(rt_range, ens_range, null, t_range, z_range, y_range, x_range);


//-----------------------------------------------------------------------------------
// Writing a GridDataset to a Netcdf-3 file

/*
* @param location    write to this location on disk
* @param gds         A gridded dataset
* @param gridList    the list of grid names to be written, must not be empty. Full name (not short).
* @param llbb        optional lat/lon bounding box
* @param range       optional time range
* @param addLatLon   should 2D lat/lon variables be added, if its a projection coordainte system?
* @param horizStride x,y stride
* @param stride_z    not implemented yet
* @param stride_time not implemented yet
* @throws IOException           if write or read error
* @throws InvalidRangeException if subset is illegal

*/
    
//    NetcdfCFWriter writer = new NetcdfCFWriter();
//    writer.makeFile(filename, gds, gridList, boundingBox, timeRange, addLatLon, horizStride, vertStride, timeStride);


    }      
}      
