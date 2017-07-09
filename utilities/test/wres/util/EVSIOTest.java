package wres.util;

/**
 * Test class for EVS IO.
 * 
 * @author evs@hydrosolved.com
 */

public class EVSIOTest
{

//    /**
//     * Reads a test dataset of observations in PI-XML format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test1ReadPIObservations() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/test1.xml";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("CBNK1", "MAP");
//        v.add(getMe);
//        //Construct the reader
//        final PublishedInterfaceIO reader = new PublishedInterfaceIO(true);
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), false, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, data, "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();  
//    }
//
//    /**
//     * Reads a test dataset of ensemble forecasts in PI-XML format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test2ReadPIForecasts() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/test2.xml";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("WALN6TOT", "SQIN");
//        v.add(getMe);
//        //Construct the reader
//        final PublishedInterfaceIO reader = new PublishedInterfaceIO(true);
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), true, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, data, "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();
//    }
//
//    /**
//     * Reads a test dataset of observations in ASCII format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test3ReadASCIIObservations() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/test3.obs";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("01608500", "MAP");
//        v.add(getMe);
//        //Construct the reader
//        final ASCIIFileIO reader = new ASCIIFileIO("yyyyMMddHH", " ");
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), false, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, data, "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();
//    }
//
//    /**
//     * Reads a test dataset of ensemble forecasts in ASCII format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test4ReadASCIIForecasts() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/test4.fcst";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("01608500", "MAP");
//        v.add(getMe);
//        //Construct the reader
//        final ASCIIFileIO reader = new ASCIIFileIO("yyyyMMddHH", " ");
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), true, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, data, "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();
//    }
//
//    /**
//     * Reads a test dataset of observations in NetCDF format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test5ReadNetCDFObservations() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/test5.nc";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("JNSV2", "QINE");
//        v.add(getMe);
//        //Construct the reader
//        final NetCDFFileIO reader = new NetCDFFileIO(new ObservedNetCDFReader(new NetCDFObservedHandler()), false);
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), false, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, (DoubleMatrix2D)data.getSubmatrixByRow(0, 10), "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();
//    }
//
//    /**
//     * Reads a test dataset of ensemble forecasts in NetCDF format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test6ReadNetCDFForecasts() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/test6.nc";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("JNSV2", "QINE");
//        v.add(getMe);
//        //Construct the reader
//        final NetCDFFileIO reader = new NetCDFFileIO(new EnsembleNetCDFReader(new NetCDFEnsembleHandler()), false);
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), true, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, (DoubleMatrix2D)data.getSubmatrixByRow(0, 10), "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();
//    }
//
//    /**
//     * Reads a test dataset of observations in NWS Datacard format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test7ReadDatacardObservations() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/test7.obs";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("QUAO2", "MAT");
//        v.add(getMe);
//        //Construct the reader
//        final OHDFileIO reader = new OHDFileIO(InputDataIO.NWSCARD);
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), false, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, data, "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();
//    }
//
//    /**
//     * Reads a test dataset of ensemble forecasts in NWS Datacard format and prints them to standard out.
//     * 
//     * @throws IOException
//     */
//    @Test
//    public void test8ReadDatacardForecasts() throws IOException
//    {
//        //Locate the test data
//        final String resource = "testinput/evsIOTest/20030701.test8.mat";
//        final File f = new File(resource);
//        if(!f.exists())
//        {
//            throw new IOException("Could not locate test data '" + f + "'.");
//        }
//        //Identify the precise data to read
//        final ArrayList<VUIdentifier> v = new ArrayList<>();
//        final VUIdentifier getMe = new VUIdentifier("QUAO2", "MAT");
//        v.add(getMe);
//        //Construct the reader
//        final OHDFileIO reader = new OHDFileIO(InputDataIO.NWSCARD);
//        //Read the data into the store
//        reader.read(new File[]{f}, v, TimeZone.getTimeZone("UTC"), true, null, -999, new IOState());
//        //Obtain the data from the store
//        final DoubleMatrix2D data = reader.getData(getMe).get(0);
//        //Print some of the data to standard out
//        //StringUtilities.printWithDates(0, data, "yyyyMMddHH");
//        //Get the double[][]
//        //double[][] d = data.toArray();
//    }

}
