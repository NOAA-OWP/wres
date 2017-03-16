package gov.noaa.wres;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import com.google.common.base.Stopwatch;

import junit.framework.TestCase;
import ucar.ma2.ArrayDouble;

/**
 * Test class for performance of population and computation for different schemes of storing observed and forecast data.
 * 
 * @author Hank.Herr
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DSMIdeasPerformanceTest extends TestCase
{
    //All sizes are communicated by these constants!!!
    private final static int NUMBER_OF_ISSUETIMES = 1000;
    private final static int NUMBER_OF_LEADTIMES = 1400; //About one year of 6-hour forecasts
    private final static int NUMBER_OF_ENSEMBLE_MEMBERS = 50;

    private final static long RANDOM_SEED = 0L;

    public Random numberGen = new Random();

    /**
     * Call to reset the seed to a starting point.
     * 
     * @param seed
     */
    public void resetNumberGen()
    {
        numberGen.setSeed(RANDOM_SEED);
    }

    /**
     * I think this is Brownian motion, when you move according to a Guassian deviate at each time step. The times
     * generated start at 0 and count forward by 6-hours.
     * 
     * @param dataStore Where the data will be placed, with lead time and member assumed to be 0.
     */
    public void generateWhiteNoiseObservations(final TestDataReceiver dataStore)
    {
        double value = 0.0;
        for(int index = 0; index < NUMBER_OF_ISSUETIMES
            + NUMBER_OF_LEADTIMES; index++)
        {
            value = value + numberGen.nextGaussian();
            dataStore.putValue(index, 0, 0, value);
        }

    }

    /**
     * Populates the provided {@link TestDataStore} with Brownian motion time series generated for each issuance time
     * (T0) starting with the observed value at the corresponding index from the provided {@link TestDataProvider}.
     * 
     * @param observedData A {@link TestDataProvider} for which it is assumed the observed data is provided for each
     *            issuance time index with the the lead time and member indices both 0.
     * @param dataStore The data store to record the fake forecasts.
     */
    public void generateWhiteNoiseForecast(final TestDataProvider observedData,
                                           final TestDataReceiver dataStore)
    {

        //Index tracks the current T0.
        for(int issuanceTimeIndex =
                                  0; issuanceTimeIndex < NUMBER_OF_ISSUETIMES; issuanceTimeIndex++)
        {
            //Each member is generated indepedently.
            for(int memberIndex =
                                0; memberIndex < NUMBER_OF_ENSEMBLE_MEMBERS; memberIndex++)
            {
                //Initialize value to observed value at T0.
                double value = observedData.getValue(issuanceTimeIndex, 0, 0);

                //Then we loop through the lead times, starting at the obs value.
                //Time series is generated one member at a time.
                for(int leadTimeIndex =
                                      0; leadTimeIndex < NUMBER_OF_LEADTIMES; leadTimeIndex++)
                {
                    value = value + numberGen.nextGaussian();
                    dataStore.putValue(issuanceTimeIndex,
                                       leadTimeIndex,
                                       memberIndex,
                                       value);
                }
            }
        }
    }

    /**
     * Compute the mean error cell by cell. Indices of the data must line up. Since this is faked data, that's not a
     * problem.
     * 
     * @param observedData The observed data (lead time and member index serve no purpose).
     * @param forecastData The forecast data.
     */
    public double computeMeanErrorBruteForce(final TestDataProvider observedData,
                                             final TestDataProvider forecastData)
    {
        final int sampleSize = NUMBER_OF_ISSUETIMES * NUMBER_OF_LEADTIMES
            * NUMBER_OF_ENSEMBLE_MEMBERS;
        System.out.println("####>> Sample size of mean error: " + sampleSize);
        double meanError = 0.0;
//        int count = 0;

        //Index tracks the current T0.
        for(int issuanceTimeIndex =
                                  0; issuanceTimeIndex < NUMBER_OF_ISSUETIMES; issuanceTimeIndex++)
        {
            //Then we loop through the lead times, starting at the obs value.
            //Time series is generated one member at a time.
            for(int leadTimeIndex =
                                  0; leadTimeIndex < NUMBER_OF_LEADTIMES; leadTimeIndex++)
            {
                //Initialize value to observed value at T0.
                final double observedValue =
                                           observedData.getValue(issuanceTimeIndex,
                                                                 leadTimeIndex,
                                                                 0);

                //Each member is generated indepedently.
                for(int memberIndex =
                                    0; memberIndex < NUMBER_OF_ENSEMBLE_MEMBERS; memberIndex++)
                {
//                    count++;
                    meanError +=
                              (forecastData.getValue(issuanceTimeIndex,
                                                     leadTimeIndex,
                                                     memberIndex)
                                  - observedValue) / sampleSize;
                }
            }
        }
//        System.err.println("####>> Count of computations = " + (double)count);
        return meanError;
    }

    /**
     * Run the performance tests.
     * 
     * @param observedValuesStore The observed data. Only the time index matters, so it need only be a one dimensional
     *            array. However, when called during statistic computation, it will be provided with both an issue time
     *            index and a lead time index, and its up to the store to determine the overall time index to return
     *            (should be issue time index + lead time index);
     * @param forecastValuesStore The forecast data.
     */
    private void runComputationalTests(final TestDataStore observedValuesStore,
                                       final TestDataStore forecastValuesStore)
    {
        try
        {
            //Generate observed and forecast data
            final Stopwatch watch = Stopwatch.createStarted();
            generateWhiteNoiseObservations(observedValuesStore);
            System.out.println("time to create Brownian motion observed data: "
                + watch.stop().elapsed(TimeUnit.MILLISECONDS) + " millis.");
            watch.reset().start();
            generateWhiteNoiseForecast(observedValuesStore,
                                       forecastValuesStore);
            System.out.println("time to create Brownian motion ensemble forecast data: "
                + watch.stop().elapsed(TimeUnit.MILLISECONDS) + " millis");

            watch.reset().start();
            final double meanError =
                                   computeMeanErrorBruteForce(observedValuesStore,
                                                              forecastValuesStore);
            System.out.println("time to compute mean error (brute force): "
                + watch.stop().elapsed(TimeUnit.MILLISECONDS) + " millis");
            System.out.println("mean error = " + meanError);
        }
        catch(final Throwable t)
        {
            t.printStackTrace();
            System.err.println("Problem Encountered!");
        }
    }

    public void test01BasicPrimitiveArrays()
    {
        System.out.println("\nTest of Basic Primitive Arrays (Peak Performance) ==================================================");
        resetNumberGen();

        //THe NUMBER_OF_LEADTIMES accounts for the pairing that must be done for the final forecast series.
        final double[] observedValues = new double[NUMBER_OF_ISSUETIMES
            + NUMBER_OF_LEADTIMES];
        final double[][][] forecastValues =
                                          new double[NUMBER_OF_ISSUETIMES][NUMBER_OF_LEADTIMES][NUMBER_OF_ENSEMBLE_MEMBERS];

        //Wrappers
        final TestDataStore observedValuesStore = new TestDataStore()
        {
            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                observedValues[issueTimeIndex + leadTimeIndex] = value;
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {
                return observedValues[issueTimeIndex + leadTimeIndex];
            }
        };
        final TestDataStore forecastValuesStore = new TestDataStore()
        {

            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                forecastValues[issueTimeIndex][leadTimeIndex][memberIndex] =
                                                                           value;
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {
                return forecastValues[issueTimeIndex][leadTimeIndex][memberIndex];
            }
        };

        //Generate observed and forecast data
        runComputationalTests(observedValuesStore, forecastValuesStore);

        System.out.println("CONCLUSION: Its doubtful performance of computations will ever better this brute force options.");
    }

    public void test02UnidataCDMArrays()
    {
        System.out.println("\nTest of Unidata CDM Array ==================================================");
        resetNumberGen();

        //THe NUMBER_OF_LEADTIMES accounts for the pairing that must be done for the final forecast series.
        final ArrayDouble.D1 observedValues =
                                            new ArrayDouble.D1(NUMBER_OF_ISSUETIMES
                                                + NUMBER_OF_LEADTIMES);
        final ArrayDouble.D3 forecastValues =
                                            new ArrayDouble.D3(NUMBER_OF_ISSUETIMES,
                                                               NUMBER_OF_LEADTIMES,
                                                               NUMBER_OF_ENSEMBLE_MEMBERS);

        //Wrappers
        final TestDataStore observedValuesStore = new TestDataStore()
        {
            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                observedValues.set(issueTimeIndex + leadTimeIndex, value);
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {
                return observedValues.get(issueTimeIndex + leadTimeIndex);
            }
        };
        final TestDataStore forecastValuesStore = new TestDataStore()
        {

            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                forecastValues.set(issueTimeIndex,
                                   leadTimeIndex,
                                   memberIndex,
                                   value);
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {
                return forecastValues.get(issueTimeIndex,
                                          leadTimeIndex,
                                          memberIndex);
            }
        };

        //Generate observed and forecast data
        runComputationalTests(observedValuesStore, forecastValuesStore);

        System.out.println("CONCLUSION: Pretty close to the performance of raw primitive arrays.");
    }

    public void test03AsTest1ButUsingJavaWrappersForPrimitives()
    {
        System.out.println("\nTest of Java Wrapped Primitive Arrays ==================================================");
        resetNumberGen();

        //THe NUMBER_OF_LEADTIMES accounts for the pairing that must be done for the final forecast series.
        final Double[] observedValues = new Double[NUMBER_OF_ISSUETIMES
            + NUMBER_OF_LEADTIMES];
        final Double[][][] forecastValues =
                                          new Double[NUMBER_OF_ISSUETIMES][NUMBER_OF_LEADTIMES][NUMBER_OF_ENSEMBLE_MEMBERS];

        //Wrappers
        final TestDataStore observedValuesStore = new TestDataStore()
        {
            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                observedValues[issueTimeIndex + leadTimeIndex] = value;
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {
                return observedValues[issueTimeIndex + leadTimeIndex];
            }
        };
        final TestDataStore forecastValuesStore = new TestDataStore()
        {

            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                forecastValues[issueTimeIndex][leadTimeIndex][memberIndex] =
                                                                           value;
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {
                return forecastValues[issueTimeIndex][leadTimeIndex][memberIndex];
            }
        };

        //Generate observed and forecast data
        runComputationalTests(observedValuesStore, forecastValuesStore);

        System.out.println("CONCLUSION: Very slow to populate data (takes three times longer for forecast data).  Takes significantly longer than primitive arrays to compute error.");
    }

    public void test04MapByIssuetimeArrayForRest()
    {
        System.out.println("\nTest of Map By Issuance Time To Primitive Array ==================================================");
        resetNumberGen();

        //THe NUMBER_OF_LEADTIMES accounts for the pairing that must be done for the final forecast series.
        final double[] observedValues = new double[NUMBER_OF_ISSUETIMES
            + NUMBER_OF_LEADTIMES];
        final Map<Integer, double[][]> forecastValues = new HashMap();

        //Wrappers
        final TestDataStore observedValuesStore = new TestDataStore()
        {
            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                observedValues[issueTimeIndex + leadTimeIndex] = value;
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {

                return observedValues[issueTimeIndex + leadTimeIndex];
            }
        };
        final TestDataStore forecastValuesStore = new TestDataStore()
        {

            @Override
            public void putValue(final int issueTimeIndex,
                                 final int leadTimeIndex,
                                 final int memberIndex,
                                 final double value)
            {
                double[][] values = forecastValues.get(issueTimeIndex);
                if(values == null)
                {
                    values =
                           new double[NUMBER_OF_LEADTIMES][NUMBER_OF_ENSEMBLE_MEMBERS];
                    forecastValues.put(issueTimeIndex, values);
                }
                values[leadTimeIndex][memberIndex] = value;
            }

            @Override
            public double getValue(final int issueTimeIndex,
                                   final int leadTimeIndex,
                                   final int memberIndex)
            {
                return forecastValues.get(issueTimeIndex)[leadTimeIndex][memberIndex];
            }
        };

        //Generate observed and forecast data
        runComputationalTests(observedValuesStore, forecastValuesStore);
        System.out.println("CONCLUSION: Slower for both population and computation, but particularly computation which takes twice as long as primitive arrays.");
    }

    /**
     * Interface that implements both.
     * 
     * @author Hank.Herr
     */
    public interface TestDataStore extends TestDataReceiver, TestDataProvider
    {

    }

    /**
     * Will be called when the white noise data is being generated.
     * 
     * @author Hank.Herr
     */
    public interface TestDataReceiver
    {
        public void putValue(int issueTimeIndex,
                             int leadTimeIndex,
                             int memberIndex,
                             double value);

    }

    /**
     * Will be called to get white noise data for verification.
     * 
     * @author Hank.Herr
     */
    public interface TestDataProvider
    {
        public double getValue(int issueTimeIndex,
                               int leadTimeIndex,
                               int memberIndex);
    }

}
