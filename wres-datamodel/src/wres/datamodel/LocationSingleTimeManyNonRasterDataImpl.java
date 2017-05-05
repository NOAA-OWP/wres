package wres.datamodel;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Has observations and (optionally) several forecasts
 *
 * Intended to be thread safe.
 *
 * @author jesse
 *
 */
public class LocationSingleTimeManyNonRasterDataImpl
implements LocationSingleTimeManyNonRasterData
{
    private static Logger LOGGER = LoggerFactory.getLogger(LocationSingleTimeManyNonRasterDataImpl.class);

    private final WresPoint wresPoint;
    private final int[] times;
    private final double[] observations; // could be mutable unidata arrays but
    private final double[][] forecasts;  // at the cost of thread safety.
    private final String[] ensembleIds;

    /**
     * This is where the io is requested.
     * 
     * The pairing is essentially complete by the time this object is fully
     * created.
     * 
     * If there is an error during construction or the data doesn't make sense,
     * an exception is thrown.
     * 
     * @param observedVariableName the identifier of the observation data
     * @param forecastVariableName the identifier of the forecast data
     * @param conditions whatever needed to condition the data, dates, etc.
     */
    private LocationSingleTimeManyNonRasterDataImpl(String observedVariableName,
                                                    String forecastVariableName,
                                                    List<Predicate> conditions)
    //throws DataConsistencyException, DataRetrievalException
    {
        this.wresPoint = WresPointFactory.of(0);

        List<Integer> times = new ArrayList<>();
        times.add(TimeConversion.internalTimeOf(LocalDateTime.of(2017, 5, 5, 6, 00))); // TODO remove dummy data
        // this would really be reading from io module
        times.add(TimeConversion.internalTimeOf(LocalDateTime.of(2017, 5, 5, 7, 00))); // TODO remove dummy data
        this.times = times.stream().mapToInt(i -> i).toArray();

        List<Double> observations = new ArrayList<>();
        observations.add(500.0); // TODO remove dummy data
        // this would really be reading from io module
        observations.add(20.0); // TODO remove dummy data
        this.observations = observations.stream().mapToDouble(d -> d).toArray();

        List<List<Double>> forecasts = new ArrayList<>();
        List<String> ensembleIds = new ArrayList<>();
        // get all the forecast data...
        int i = 0;
        while (i < 2)
        {
            List<Double> anEnsembleTrace = new ArrayList<>();
            anEnsembleTrace.add(45.0*(i+1)); // TODO remove dummy data
            // this would really be reading from io module
            anEnsembleTrace.add(30.5*(i+1)); // TODO remove dummy data
            
            forecasts.add(anEnsembleTrace);
            
            ensembleIds.add(Integer.toString(i)); // TODO remove dummy data
            i++;
        }
        // Create 2d array from array of arrays
        this.forecasts = forecasts.stream()
                                  .map(l -> l.stream()
                                             .mapToDouble(d -> d)
                                             .toArray())
                                  .toArray(double[][]::new);

        this.ensembleIds = ensembleIds.stream().toArray(String[]::new);
    }

    public static LocationSingleTimeManyNonRasterDataImpl of(String observedVariable,
                                                             String forecastVariable,
                                                             List<Predicate> conditions)
    {
        return new LocationSingleTimeManyNonRasterDataImpl(observedVariable,
                                                           forecastVariable,
                                                           conditions);
    }

    @Override
    public WresPoint getWresPoint()
    {
        return wresPoint;
    }

    @Override
    public LocalDateTime[] getDateTimes()
    {
        return Arrays.stream(times)
                     .mapToObj(TimeConversion::localDateTimeOf)
                     .toArray(LocalDateTime[]::new);
    }

    @Override
    public double[] getObservationValues()
    {
        return observations;
    }

    @Override
    public double[] getForecastValues(String ensembleId)
    {
        for (int i = 0; i < ensembleIds.length; i++)
        {
            if (ensembleIds[i].equals(ensembleId))
            {
                // assumes internal consistency
                return forecasts[i];
            }
        }
        return new double[] {};
    }

    @Override
    public double[][] getObservationAndForecastValues(String ensembleId)
    {
        double[] observations = this.observations;
        double[] forecasts = getForecastValues(ensembleId);
        double[][] result = new double[forecasts.length][2];
        for (int i = 0; i < forecasts.length && i < observations.length; i++)
        {
            result[i][0] = observations[i];
            result[i][1] = forecasts[i];
        }
        return result;
    }
}
