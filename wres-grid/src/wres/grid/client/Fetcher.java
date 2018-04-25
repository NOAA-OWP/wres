package wres.grid.client;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.stream.DoubleStream;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;

/**
 * Stand in module for integration development between IO and the Grid module
 */
public class Fetcher
{
    public static Response getData(Request request) throws IOException
    {
        TimeSeriesResponse response = Fetcher.fakeOutResponseData(request);


        return response;
    }

    /**
     * Stand-in function for creating fake response data
     * @param request
     * @return A response object with fake data
     */
    private static TimeSeriesResponse fakeOutResponseData(Request request)
    {
        TimeSeriesResponse response = new TimeSeriesResponse();

        // We want 1 feature, 5 time series

        // Each time series, 6 hours apart from the earliest date

        // each time series has 18 entries, with 1 value each, for leads 1 through 18

        // The values from each need to be a function of their feature,
        // their issue time, their path, and their lead

        response.setVariableName( request.getVariableName() );
        response.setMeasurementUnit( "MM" );

        for (Feature feature : request.getFeatures())
        {
            FeaturePlus featurePlus = FeaturePlus.of(feature);

            int featureSeed = 0;

            if (feature.getLocationId() != null)
            {
                for (int i = 0; i < feature.getLocationId().length(); ++i)
                {
                    featureSeed += Character.getNumericValue( feature.getLocationId().charAt( i ) );
                }
            }

            if (feature.getCoordinate() != null)
            {
                featureSeed += feature.getCoordinate().getLatitude();
                featureSeed += feature.getCoordinate().getLongitude();
            }

            if (feature.getWkt() != null)
            {
                for (int i = 0; i < feature.getWkt().length(); ++i)
                {
                    featureSeed += Character.getNumericValue( feature.getWkt().charAt( i ) );
                }
            }

            if (feature.getHuc() != null)
            {
                for (int i = 0; i < feature.getHuc().length(); ++i)
                {
                    featureSeed += Character.getNumericValue( feature.getHuc().charAt( i ) );
                }
            }

            if (feature.getRfc() != null)
            {
                for (int i = 0; i < feature.getRfc().length(); ++i)
                {
                    featureSeed += Character.getNumericValue( feature.getRfc().charAt( i ) );
                }
            }

            int variableNameSeed = 0;

            for (int i = 0; i < request.getVariableName().length(); ++i)
            {
                featureSeed += Character.getNumericValue( request.getVariableName().charAt( i ) );
            }


            Duration offset = Duration.of( 1, ChronoUnit.HOURS );

            Instant earliestDate = LocalDateTime.of( 2018, 4, 1, 12, 0 )
                                                .toInstant( ZoneOffset.UTC );

            if ( request.getIsForecast()
                 && request.getEarliestIssueTime() != null )
            {
                earliestDate = request.getEarliestIssueTime();
            }
            else if ( request.getEarliestValidTime() != null )
            {
                earliestDate = request.getEarliestValidTime();
            }

            Instant latestDate = null;
            int observationCount = 6 * 6 + 18;

            if ( request.getLatestValidTime() != null )
            {
                latestDate = request.getLatestValidTime();
            }
            else
            {
                latestDate = earliestDate.plus( Duration.of( observationCount,
                                                             ChronoUnit.HOURS ) );
            }

            long earliestIssueSeed = 0;
            long latestIssueSeed = 0;

            if (request.getIsForecast() && request.getEarliestIssueTime() != null)
            {
                earliestIssueSeed = request.getEarliestIssueTime().getEpochSecond();
            }

            if (request.getIsForecast() && request.getLatestIssueTime() != null)
            {
                latestIssueSeed = request.getLatestIssueTime().getEpochSecond();
            }

            Random generator = new Random(earliestDate.getEpochSecond() +
                                          latestDate.getEpochSecond() +
                                          earliestIssueSeed +
                                          latestIssueSeed +
                                          variableNameSeed +
                                          featureSeed);

            if ( request.getIsForecast() )
            {
                for ( int timeSeriesIndex = 0;
                      timeSeriesIndex < request.getPaths().size() / 6; // This is fake; we're acting like each forecast has 6 hours of data
                      ++timeSeriesIndex )
                {
                    Instant currentDateTime = Instant.from( earliestDate );

                    for (long leadIndex = request.getEarliestLead().plusHours( 1 ).toHours();
                         leadIndex <= request.getLatestLead().toHours();
                         ++leadIndex)
                    {
                        currentDateTime = currentDateTime.plus( Duration.of(leadIndex, ChronoUnit.HOURS) );
                        response.add( featurePlus, earliestDate, currentDateTime, generator.nextDouble() * 2.5 );
                    }

                    earliestDate = earliestDate.plus( offset );
                }
            }
            else
            {
                Instant currentDateTime = Instant.from( earliestDate ).plus( Duration.of( 1, ChronoUnit.MINUTES ) );

                while ( currentDateTime.isBefore( latestDate ) || currentDateTime.equals( latestDate ) )
                {
                    response.add( featurePlus, earliestDate, currentDateTime, generator.nextDouble() * 2.5 );

                    currentDateTime = currentDateTime.plus( Duration.of( 1,
                                                                         ChronoUnit.HOURS ) );
                }

            }
        }

        return response;
    }

    public static Integer getValueCount(Request request) throws IOException
    {
        Response timeSeriesSet = Fetcher.getData( request );
        return timeSeriesSet.getValueCount();
    }

    public static Duration getLastLead(Request request) throws IOException
    {
        Response timeSeriesSet = Fetcher.getData( request );
        return timeSeriesSet.getLastLead();
    }

    public static Request prepareRequest()
    {
        return new GridDataRequest();
    }
}
