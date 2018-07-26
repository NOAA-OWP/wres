package wres.io.reading.s3;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.util.TimeHelper;

class NWCALReader extends S3Reader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NWCALReader.class );
    private static final int MAX_KEY_COUNT = 9000;
    private static final String ENDPOINT_URL = "http://***REMOVED***rgw.***REMOVED***.***REMOVED***:8080";
    private static final DateTimeFormatter DATE_PREFIX_FORMAT = DateTimeFormatter.ofPattern( "yyyyMMdd" );

    NWCALReader( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

    @Override
    String getKeyURL( String key )
    {
        return NWCALReader.ENDPOINT_URL + "/" + this.getBucketName() + "/" + key;
    }

    @Override
    int getMaxKeyCount()
    {
        return NWCALReader.MAX_KEY_COUNT;
    }

    @Override
    Iterable<PrefixPattern> getPrefixPatterns()
    {
        // The NWM bucket is set up as:
        //   "nwm.YYYYmmDD/range/filename.nc"
        // We need to find the very basic prefix, which will be "nwm.yyyyMMdd" with
        // each date between the earliest of the issue or valid dates and the latest of the issue and valid dates
        LocalDateTime earliestIssue = this.getEarliestIssueDate();
        LocalDateTime latestIssue = this.getLatestIssueDate();
        LocalDateTime earliestValidDate = this.getEarliestValidDate();
        LocalDateTime latestValidDate = this.getLatestValidDate();

        LocalDate earliest;
        LocalDate latest;

        // Get the earliest date to get data from
        if (earliestIssue.isBefore( earliestValidDate ))
        {
            earliest = earliestIssue.toLocalDate();
        }
        else
        {
            earliest = earliestValidDate.toLocalDate();
        }

        // Get the latest date to get data from
        if (latestIssue == null && latestValidDate == null)
        {
            latest = LocalDate.now();
        }
        else if (latestValidDate == null || latestIssue != null && latestIssue.isAfter( latestValidDate ) )
        {
            Duration specifiedLead = this.getLatestSpecifiedLead();

            // If we're rolling with an issue date and the user specifies that they want
            // leads that extend beyond the issuing date, the date needs to be at least
            // that far out. If the user specifies a max lead of 120 hours, we need to
            // ensure that the user has data for at least 5 days after that issue date
            if (!specifiedLead.isNegative())
            {
                latestIssue = latestIssue.plus( specifiedLead );
            }

            latest = latestIssue.toLocalDate();
        }
        else
        {
            latest = latestValidDate.toLocalDate();
        }

        // Get the number of days to iterate through. We add the extra day to make it inclusive
        final int numberOfDays = (int)earliest.until( latest, ChronoUnit.DAYS ) + 1;

        // Get every full day between [earliest day, latest day]
        List<LocalDate> datesToRetrieve = Stream.iterate( earliest, d -> d.plusDays( 1 ) )
                                                .limit( numberOfDays )
                                                .collect( Collectors.toList());

        List<PrefixPattern> prefixPatterns = new ArrayList<>();

        // For every calculated day...
        for (LocalDate date : datesToRetrieve)
        {
            // Add the prefix of "nwm." + date and the source's pattern to the collection
            String prefix = "nwm.";
            prefix += date.format( NWCALReader.DATE_PREFIX_FORMAT );
            prefixPatterns.add( new PrefixPattern( prefix, this.getSourceConfig().getPattern() ) );
        }

        return prefixPatterns;
    }

    private LocalDateTime getEarliestValidDate()
    {
        LocalDateTime date = LocalDateTime.MAX;

        if (this.getProjectConfig().getPair().getDates() != null &&
            this.getProjectConfig().getPair().getDates().getEarliest() != null)
        {
            date = LocalDateTime.ofInstant(
                    Instant.parse( this.getProjectConfig().getPair().getDates().getEarliest()),
                    ZoneId.of( "UTC")
            );
        }
        return date;
    }

    private LocalDateTime getLatestValidDate()
    {
        LocalDateTime date = null;

        if (this.getProjectConfig().getPair().getDates() != null &&
            this.getProjectConfig().getPair().getDates().getLatest() != null)
        {
            date = LocalDateTime.ofInstant(
                    Instant.parse( this.getProjectConfig().getPair().getDates().getLatest()),
                    ZoneId.of( "UTC")
            );
        }
        return date;
    }

    private LocalDateTime getEarliestIssueDate()
    {
        LocalDateTime date = LocalDateTime.MAX;

        if (this.getProjectConfig().getPair().getIssuedDates() != null &&
            this.getProjectConfig().getPair().getIssuedDates().getEarliest() != null)
        {
            date = LocalDateTime.ofInstant(
                    Instant.parse( this.getProjectConfig().getPair().getIssuedDates().getEarliest()),
                    ZoneId.of( "UTC")
            );
        }
        return date;
    }

    private LocalDateTime getLatestIssueDate()
    {
        LocalDateTime date = null;

        if (this.getProjectConfig().getPair().getIssuedDates() != null &&
            this.getProjectConfig().getPair().getIssuedDates().getLatest() != null)
        {
            date = LocalDateTime.ofInstant(
                    Instant.parse( this.getProjectConfig().getPair().getIssuedDates().getLatest()),
                    ZoneId.of( "UTC")
            );
        }
        return date;
    }

    private Duration getLatestSpecifiedLead()
    {
        Duration specifiedLead = Duration.ofDays(-1);

        if (this.getProjectConfig().getPair().getLeadHours() != null)
        {

            if (this.getProjectConfig().getPair().getLeadHours().getMaximum() != null)
            {
                specifiedLead = Duration.of(
                        this.getProjectConfig().getPair().getLeadHours().getMaximum(),
                        TimeHelper.LEAD_RESOLUTION
                );
            }

            if (this.getProjectConfig().getPair().getLeadHours().getMinimum() != null)
            {
                specifiedLead = Duration.of(
                        this.getProjectConfig().getPair().getLeadHours().getMinimum(),
                        TimeHelper.LEAD_RESOLUTION
                );
            }
        }

        return specifiedLead;
    }

    @Override
    AwsClientBuilder.EndpointConfiguration getEndpoint()
    {
        return new AwsClientBuilder.EndpointConfiguration(
                NWCALReader.ENDPOINT_URL,
                Regions.US_EAST_1.getName()
        );
    }

    @Override
    String getBucketName()
    {
        // The reader will be used to access National Water Model data that is
        // stored within the "nwm" bucket.
        return "nwm";
    }

    @Override
    AWSCredentialsProvider getCredentials()
    {
        // No credentials are needed to read objects from the NWCAL object store
        return new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() );
    }

    @Override
    boolean shouldUsePathStyle()
    {
        // The NWCAL Object store uses the pattern
        // "example.com/${BUCKET_NAME}/whatever/content.ext"
        return true;
    }

    @Override
    protected Logger getLogger()
    {
        return NWCALReader.LOGGER;
    }
}
