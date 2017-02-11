package owp.util;

//EVS dependencies
import evs.data.*;
import evs.io.*;
import evs.io.netcdf.*;
import evs.io.nws.*;
import evs.io.xml.pixml.*;
import evs.utilities.*;
import evs.utilities.matrix.*;

//Java dependencies
import java.net.*;
import java.io.*;
import java.util.*;

//JUnit dependencies
import org.junit.*;

/**
 * Test class for EVS IO.
 * 
 * @author evs@hydrosolved.com
 */

public class EVSIOTest {
    
    /**
     * Reads a test dataset of observations in PI-XML format and prints them to
     * standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readPIObservations() throws IOException {
        //Locate the test data
        String resource = "CBNK1_MAP.xml";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("CBNK1", "MAP");
        v.add(getMe);
        //Construct the reader
        PublishedInterfaceIO reader = new PublishedInterfaceIO(true);
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), false, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, data,"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }
    
    /**
     * Reads a test dataset of ensemble forecasts in PI-XML format and prints 
     * them to standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readPIForecasts() throws IOException {
        //Locate the test data
        String resource = "1988010112_WALN6DEL_hefs_export.xml";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("WALN6TOT", "SQIN");
        v.add(getMe);
        //Construct the reader
        PublishedInterfaceIO reader = new PublishedInterfaceIO(true);
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), true, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, data,"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }    
    
    /**
     * Reads a test dataset of observations in ASCII format and prints them to
     * standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readASCIIObservations() throws IOException {
        //Locate the test data
        String resource = "01608500_TEST.OBS";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("01608500", "MAP");
        v.add(getMe);
        //Construct the reader
        ASCIIFileIO reader = new ASCIIFileIO("yyyyMMddHH", " ");
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), false, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, data,"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }
    
    /**
     * Reads a test dataset of ensemble forecasts in ASCII format and prints 
     * them to standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readASCIIForecasts() throws IOException {
        //Locate the test data
        String resource = "01608500_TEST.FCST";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("01608500", "MAP");
        v.add(getMe);
        //Construct the reader
        ASCIIFileIO reader = new ASCIIFileIO("yyyyMMddHH", " ");
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), true, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, data,"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }    
    
    /**
     * Reads a test dataset of observations in NetCDF format and prints them to
     * standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readNetCDFObservations() throws IOException {
        //Locate the test data
        String resource = "2011010312_observed_discharge_timeseries.nc";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("JNSV2", "QINE");
        v.add(getMe);
        //Construct the reader
        NetCDFFileIO reader = new NetCDFFileIO(new ObservedNetCDFReader(
                new NetCDFObservedHandler()),
                false);
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), false, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, (DoubleMatrix2D)data.
                getSubmatrixByRow(0, 10),"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }
    
    /**
     * Reads a test dataset of ensemble forecasts in NetCDF format and prints 
     * them to standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readNetCDFForecasts() throws IOException {
        //Locate the test data
        String resource = "1990110112_AEnKF_discharge_timeseries.nc";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("JNSV2", "QINE");
        v.add(getMe);
        //Construct the reader
        NetCDFFileIO reader = new NetCDFFileIO(new EnsembleNetCDFReader(
                new NetCDFEnsembleHandler()),
                false);
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), true, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, (DoubleMatrix2D)data.
                getSubmatrixByRow(0, 10),"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }    
    
    /**
     * Reads a test dataset of observations in NWS Datacard format and prints 
     * them to standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readDatacardObservations() throws IOException {
        //Locate the test data
        String resource = "QUAO2.MAT.OBS";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("QUAO2", "MAT");
        v.add(getMe);
        //Construct the reader
        OHDFileIO reader = new OHDFileIO(InputDataIO.NWSCARD);
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), false, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, data,"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }
    
    /**
     * Reads a test dataset of ensemble forecasts in NWS Datacard format and
     * prints them to standard out.
     * 
     * @throws IOException
     */
    @Test
    public void readDatacardForecasts() throws IOException {
        //Locate the test data
        String resource = "20030701QUAO2.MAT";
        URL readMe = EVSIOTest.class.getClassLoader().getResource(resource);
        File f = null; 
        try { 
            f = new File(readMe.getFile());
        } catch(Exception e) {
            throw new IOException("Could not locate test data '"+resource+"'. "
                    + "Check that the containing directory is in the test "
                    + "classpath: "
                    + e.getMessage());
        }
        //Identify the precise data to read
        ArrayList<VUIdentifier> v = new ArrayList<>();
        VUIdentifier getMe = new VUIdentifier("QUAO2", "MAT");
        v.add(getMe);
        //Construct the reader
        OHDFileIO reader = new OHDFileIO(InputDataIO.NWSCARD);
        //Read the data into the store
        reader.read(new File[]{f}, v,
                TimeZone.getTimeZone("UTC"), true, null, -999,new IOState());
        //Obtain the data from the store
        DoubleMatrix2D data = reader.getData(getMe).get(0);
        //Print some of the data to standard out
        StringUtilities.printWithDates(0, data,"yyyyMMddHH");
        //Get the double[][]
        //double[][] d = data.toArray();
    }     
    
    
    
}
