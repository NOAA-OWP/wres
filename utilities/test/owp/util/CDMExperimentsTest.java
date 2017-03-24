package owp.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import evs.data.VUIdentifier;
import evs.io.IOState;
import evs.io.xml.pixml.PublishedInterfaceIO;
import evs.utilities.matrix.DoubleMatrix2D;
import jersey.repackaged.com.google.common.collect.Lists;
import junit.framework.TestCase;
import ucar.ma2.Array;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayInt;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

public class CDMExperimentsTest extends TestCase
{

    public void testPITimeSeriesReadIntoCDM()
    {
        //Read in FEWS time series.
        final File tsFile = new File("testinput/evsIOTest/test2.xml");

        //Identify the precise data to read
        final ArrayList<VUIdentifier> v = new ArrayList();
        final VUIdentifier getMe = new VUIdentifier("WALN6TOT", "SQIN");
        v.add(getMe);

        //Construct the reader
        final PublishedInterfaceIO reader = new PublishedInterfaceIO(true);

        NetcdfFileWriter netCDFWriter = null;
        NetcdfFile netCDFFile = null;

        //Read the data into the store
        try
        {
            reader.read(new File[]{tsFile},
                        v,
                        TimeZone.getTimeZone("UTC"),
                        true,
                        null,
                        -999,
                        new IOState());

            //Data stores the valid time at index 0, lead time (hrs) and index 1, and ensemble members of the data read in order at index 2 and later.
            //Convert date to millis by multiplying by 1000*60*60.
            final DoubleMatrix2D data = reader.getData(getMe).get(0);

            //How many valid times?  
            //I'm assuming that, by the time I turn the data into NetCDF-based stuff, I'll know the number of valid times.  Hence, 
            //I must track that as data is read in.  In fact, if I knew the number of valid times a priori, I could use a Unidata Array 
            //directly, but I don't for a PI-timeseries file.  Hence, I'm assuming whatever reader is written will count it.  This 
            //loop mimicks that.
            final Set<Double> validTimes = new HashSet();
            for(int i = 0; i < data.getRowCount(); i++)
            {
                validTimes.add(data.get(i, 0));
            }

            //===========================================================
            //Follow example I found to create a CDM data storage object.
            //===========================================================

            //Dimensions
            //Valid times
            final Dimension validTimeLeadTimeDim =
                                                 new Dimension("validTimeLeadTime",
                                                               data.getRowCount()); //0 -> Unlimited
            final ArrayDouble.D2 validTimeLeadTimeArray =
                                                        new ArrayDouble.D2(validTimeLeadTimeDim.getLength(),
                                                                           2);

            //Ensemble member indices
            final Dimension ensembleMemberDim = new Dimension("ensembleMember",
                                                              data.getColumnCount()
                                                                  - 2);
            final ArrayInt.D1 ensembleMemberArray =
                                                  new ArrayInt.D1(ensembleMemberDim.getLength());

            //Populate the arrays.  First the members...
            for(int i = 0; i < ensembleMemberDim.getLength(); i++)
            {
                ensembleMemberArray.set(i, 1950 + i);
            }

            //Now the valid time lead time dimension
            for(int i = 0; i < data.getRowCount(); i++)
            {
                validTimeLeadTimeArray.set(i, 0, (long)data.get(i, 0));
                validTimeLeadTimeArray.set(i, 1, (long)data.get(i, 1));
            }

            //Metadata.  In practice, this would all be translated from the PI-timseries file.  However, the EVS tool above
            //loses that information.
            final Attribute locationIdAttr = new Attribute("locationId",
                                                           "WALN6TOT");
            final Attribute parameterIdAttr = new Attribute("parameterId",
                                                            "SQIN");
            final Attribute ensembleIdAttr =
                                           new Attribute("ensembleId", "HEFS");

            //Now for the data which stores ensemble member values for each valid time and lead time
            final ArrayDouble.D2 dataInCDM =
                                           new ArrayDouble.D2(validTimeLeadTimeDim.getLength(),
                                                              ensembleMemberDim.getLength());
            for(int i = 0; i < validTimeLeadTimeDim.getLength(); i++)
            {
                for(int j = 0; j < ensembleMemberDim.getLength(); j++)
                {
                    dataInCDM.set(i, j, data.get(i, j + 2));
                }
            }

            //===========================================================
            //WRITE THE FILE
            //===========================================================
            //Wrap the data in variables and insert them into the net cdf writer.  Write the data to the file.
            netCDFWriter =
                         NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3,
                                                    "testoutput/CDMExperiments/test1.ncdf",
                                                    null);
            final Dimension dim1 =
                                 netCDFWriter.addDimension(null,
                                                           "validTimeLeadTimeRows",
                                                           validTimeLeadTimeDim.getLength());
            final Dimension dim2 = netCDFWriter.addDimension(null,
                                                             "validTimeLeadTimeColumns",
                                                             2);
            final Dimension dim3 =
                                 netCDFWriter.addDimension(null,
                                                           "ensembleMemberRows",
                                                           ensembleMemberDim.getLength());
            final Variable dataVariable =
                                        netCDFWriter.addVariable(null,
                                                                 "data",
                                                                 DataType.DOUBLE,
                                                                 Lists.newArrayList(dim1,
                                                                                    dim3));
            final Variable validTimeLeadTimeVar =
                                                netCDFWriter.addVariable(null,
                                                                         "validTimeLeadTimeDimension",
                                                                         DataType.DOUBLE,
                                                                         Lists.newArrayList(dim1,
                                                                                            dim2));
            final Variable ensembleMemberVar =
                                             netCDFWriter.addVariable(null,
                                                                      "ensembleMembers",
                                                                      DataType.INT,
                                                                      Lists.newArrayList(dim3));
            netCDFWriter.addGroupAttribute(null, locationIdAttr);
            netCDFWriter.addGroupAttribute(null, parameterIdAttr);
            netCDFWriter.addGroupAttribute(null, ensembleIdAttr);
            netCDFWriter.create();
            netCDFWriter.write(validTimeLeadTimeVar, validTimeLeadTimeArray);
            netCDFWriter.write(ensembleMemberVar, ensembleMemberArray);
            netCDFWriter.write(dataVariable, dataInCDM);
            netCDFWriter.close();
            netCDFWriter = null;

            //===========================================================
            //READ IN THE FILE AND CHECK THE DATA!
            //===========================================================
            netCDFFile =
                       NetcdfFile.open("testoutput/CDMExperiments/test1.ncdf");
            final Variable foundData = netCDFFile.findVariable("data");
            final Attribute foundAttr = netCDFFile.findAttribute("@locationId");
            System.err.println("####>> found attr - "
                + foundAttr.getStringValue());
            final Array array = foundData.read();
            System.err.println("####>> array stuff - "
                + Arrays.toString(array.getShape()));

            //TODO Need to either add benchmark comparisons or remove this test since its just an experiment.
        }
        catch(final Exception e)
        {
            e.printStackTrace();
            fail("Unexpected exception.");
        }
        finally
        {
            if(netCDFWriter != null)
            {
                try
                {
                    netCDFWriter.close();
                }
                catch(final IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

}
