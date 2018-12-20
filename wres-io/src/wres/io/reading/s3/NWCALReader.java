package wres.io.reading.s3;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.util.TimeHelper;

/**
 * An S3 reader specifically for the NWCAL object store
 */
class NWCALReader extends S3Reader
{
    private enum NWMFileType
    {
        ALL(0),
        ANALYSIS_ASSIM(1),
        FORCING_ANALYSIS_ASSIM(2),
        FORCING_MEDIUM_RANGE(3),
        FORCING_SHORT_RANGE(4),
        MEDIUM_RANGE(5),
        SHORT_RANGE(6),
        LONG_RANGE_MEM1(7),
        LONG_RANGE_MEM2(8),
        LONG_RANGE_MEM3(9),
        LONG_RANGE_MEM4(10);

        private final int fileType;

        public int getFileType()
        {
            return this.fileType;
        }

        NWMFileType(final int fileType)
        {
            this.fileType = fileType;
        }

        static boolean typeExists(final String name)
        {
            for (NWMFileType fileType : NWMFileType.values())
            {
                if (fileType.name().equals( name ))
                {
                    return true;
                }
            }
            return false;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( NWCALReader.class );

    /**
     * This is the maximum number of S3 file entries that may be retrieved at once
     */
    private static final int MAX_KEY_COUNT = 200;

    /**
     * The number of times to attempt to access the Object store before giving up
     */
    private static final int RETRY_COUNT = 5;

    /**
     * This is the URL of the NWCAL object store
     * TODO: Do we want to make this configurable?
     */
    private static final String ENDPOINT_URL = "http://***REMOVED***rgw.***REMOVED***.***REMOVED***:8080";

    /*Here's the fun: we need to hit the file index API like with NWIS. We no longer need to seperate days, but we still need
    to determine the bookends.  Users will need to indicate first the file type (enum above), then a pattern, like "*land*" or something.
    We need a query like:
        "http://***REMOVED***.***REMOVED***.***REMOVED***/api/v1/nwm-file-index/6/?"
        + "start-date=20181120&"
        + "end-date=20181121&"
        + "fields=name"
        + "%2C%20source_file"
        + "%2C%20destination_file"
        + "%2C%20data_datetime"
        + "%2C%20appearance_datetime"
        + "%2C%20acquired_datetime"
        + "%2C%20complete_datetime"
        + "%2C%20file_size_in_bytes"
        + "%2C%20file_modification_datetime"
        + "%2C%20cached_datetime"
    The hash will be based on those numbers since we wont have enough information otherwise without downloading these files every time.

    that will give us a response like:
    {
        "next": null,
            "previous": null,
            "results": [
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t23z.short_range.land.f007.conus.nc",
                "name": "NWM Land Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t23z.short_range.land.f007.conus.nc",
                "data_datetime": "2018-11-21T23:00:00Z",
                "appearance_datetime": "2018-11-22T00:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T06:20:21.750273Z",
                "complete_datetime": "2018-11-27T06:20:25.791354Z",
                "failed_datetime": "2018-11-22T00:50:07.835130Z",
                "file_modification_datetime": "2018-11-22T00:38:53Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-22T00:50:07.835130Z",
                "cached_file": null,
                "file_size_in_bytes": 42783462,
                "extra_classes": {
            "forecast_hour": 7
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t22z.short_range.channel_rt.f005.conus.nc",
                "name": "NWM Channel Routing Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t22z.short_range.channel_rt.f005.conus.nc",
                "data_datetime": "2018-11-21T22:00:00Z",
                "appearance_datetime": "2018-11-21T23:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T16:32:17.508714Z",
                "complete_datetime": "2018-11-27T16:32:28.960181Z",
                "failed_datetime": "2018-11-21T23:47:58.342677Z",
                "file_modification_datetime": "2018-11-21T23:38:15Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T23:47:58.342677Z",
                "cached_file": null,
                "file_size_in_bytes": 11870523,
                "extra_classes": {
            "forecast_hour": 5
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t20z.short_range.reservoir.f008.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t20z.short_range.reservoir.f008.conus.nc",
                "data_datetime": "2018-11-21T20:00:00Z",
                "appearance_datetime": "2018-11-21T21:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T00:15:48.916723Z",
                "complete_datetime": "2018-11-27T00:15:49.778110Z",
                "failed_datetime": "2018-11-21T21:46:40.102993Z",
                "file_modification_datetime": "2018-11-21T21:41:35Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T21:46:40.102993Z",
                "cached_file": null,
                "file_size_in_bytes": 54134,
                "extra_classes": {
            "forecast_hour": 8
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t19z.short_range.land.f013.conus.nc",
                "name": "NWM Land Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t19z.short_range.land.f013.conus.nc",
                "data_datetime": "2018-11-21T19:00:00Z",
                "appearance_datetime": "2018-11-21T20:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T08:46:33.466118Z",
                "complete_datetime": "2018-11-27T08:46:37.052928Z",
                "failed_datetime": "2018-11-21T20:48:00.841238Z",
                "file_modification_datetime": "2018-11-21T20:38:14Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T20:48:00.841238Z",
                "cached_file": null,
                "file_size_in_bytes": 44461932,
                "extra_classes": {
            "forecast_hour": 13
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t17z.short_range.reservoir.f017.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t17z.short_range.reservoir.f017.conus.nc",
                "data_datetime": "2018-11-21T17:00:00Z",
                "appearance_datetime": "2018-11-21T18:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T08:16:11.787374Z",
                "complete_datetime": "2018-11-27T08:16:13.366895Z",
                "failed_datetime": "2018-11-21T18:50:46.414884Z",
                "file_modification_datetime": "2018-11-21T18:38:52Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T18:50:46.414884Z",
                "cached_file": null,
                "file_size_in_bytes": 54152,
                "extra_classes": {
            "forecast_hour": 17
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t16z.short_range.channel_rt.f003.conus.nc",
                "name": "NWM Channel Routing Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t16z.short_range.channel_rt.f003.conus.nc",
                "data_datetime": "2018-11-21T16:00:00Z",
                "appearance_datetime": "2018-11-21T17:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T07:15:44.096289Z",
                "complete_datetime": "2018-11-27T07:15:49.225957Z",
                "failed_datetime": "2018-11-21T17:49:34.477543Z",
                "file_modification_datetime": "2018-11-21T17:39:32Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T17:49:34.477543Z",
                "cached_file": null,
                "file_size_in_bytes": 11845140,
                "extra_classes": {
            "forecast_hour": 3
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t15z.short_range.channel_rt.f011.conus.nc",
                "name": "NWM Channel Routing Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t15z.short_range.channel_rt.f011.conus.nc",
                "data_datetime": "2018-11-21T15:00:00Z",
                "appearance_datetime": "2018-11-21T16:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T07:35:58.803857Z",
                "complete_datetime": "2018-11-27T07:36:02.797267Z",
                "failed_datetime": "2018-11-21T16:48:01.448075Z",
                "file_modification_datetime": "2018-11-21T16:39:59Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T16:48:01.448075Z",
                "cached_file": null,
                "file_size_in_bytes": 11854617,
                "extra_classes": {
            "forecast_hour": 11
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t12z.short_range.land.f002.conus.nc",
                "name": "NWM Land Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t12z.short_range.land.f002.conus.nc",
                "data_datetime": "2018-11-21T12:00:00Z",
                "appearance_datetime": "2018-11-21T13:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T21:33:56.527433Z",
                "complete_datetime": "2018-11-27T21:36:41.531597Z",
                "failed_datetime": "2018-11-21T14:16:54.880908Z",
                "file_modification_datetime": "2018-11-21T14:06:13Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T14:16:54.880908Z",
                "cached_file": null,
                "file_size_in_bytes": 40692301,
                "extra_classes": {
            "forecast_hour": 2
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t07z.short_range.terrain_rt.f014.conus.nc",
                "name": "NWM Terrain Routing Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t07z.short_range.terrain_rt.f014.conus.nc",
                "data_datetime": "2018-11-21T07:00:00Z",
                "appearance_datetime": "2018-11-21T08:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T08:46:33.553701Z",
                "complete_datetime": "2018-11-27T08:46:37.720757Z",
                "failed_datetime": "2018-11-21T08:49:44.065954Z",
                "file_modification_datetime": "2018-11-21T08:38:32Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T08:49:44.065954Z",
                "cached_file": null,
                "file_size_in_bytes": 40760291,
                "extra_classes": {
            "forecast_hour": 14
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t07z.short_range.terrain_rt.f005.conus.nc",
                "name": "NWM Terrain Routing Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t07z.short_range.terrain_rt.f005.conus.nc",
                "data_datetime": "2018-11-21T07:00:00Z",
                "appearance_datetime": "2018-11-21T08:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T06:38:22.943026Z",
                "complete_datetime": "2018-11-27T06:38:38.836947Z",
                "failed_datetime": "2018-11-21T08:47:17.071426Z",
                "file_modification_datetime": "2018-11-21T08:38:39Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T08:47:17.071426Z",
                "cached_file": null,
                "file_size_in_bytes": 40098543,
                "extra_classes": {
            "forecast_hour": 5
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t06z.short_range.terrain_rt.f012.conus.nc",
                "name": "NWM Terrain Routing Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t06z.short_range.terrain_rt.f012.conus.nc",
                "data_datetime": "2018-11-21T06:00:00Z",
                "appearance_datetime": "2018-11-21T07:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T08:04:08.119754Z",
                "complete_datetime": "2018-11-27T08:04:11.110312Z",
                "failed_datetime": "2018-11-21T08:13:01.487987Z",
                "file_modification_datetime": "2018-11-21T08:06:12Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T08:13:01.487987Z",
                "cached_file": null,
                "file_size_in_bytes": 40589214,
                "extra_classes": {
            "forecast_hour": 12
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181121/short_range/nwm.t04z.short_range.reservoir.f017.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181121/short_range/nwm.t04z.short_range.reservoir.f017.conus.nc",
                "data_datetime": "2018-11-21T04:00:00Z",
                "appearance_datetime": "2018-11-21T05:45:00Z",
                "removal_datetime": "2018-11-23T00:00:00Z",
                "acquired_datetime": "2018-11-27T07:34:51.769615Z",
                "complete_datetime": "2018-11-27T07:34:52.436108Z",
                "failed_datetime": "2018-11-26T03:24:24.120461Z",
                "file_modification_datetime": "2018-11-21T05:37:46Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-26T03:24:24.120461Z",
                "cached_file": null,
                "file_size_in_bytes": 54114,
                "extra_classes": {
            "forecast_hour": 17
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t23z.short_range.reservoir.f018.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t23z.short_range.reservoir.f018.conus.nc",
                "data_datetime": "2018-11-20T23:00:00Z",
                "appearance_datetime": "2018-11-21T00:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T07:45:54.557858Z",
                "complete_datetime": "2018-11-27T07:45:56.914155Z",
                "failed_datetime": "2018-11-21T00:48:08.626265Z",
                "file_modification_datetime": "2018-11-21T00:39:21Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-21T00:48:08.626265Z",
                "cached_file": null,
                "file_size_in_bytes": 54109,
                "extra_classes": {
            "forecast_hour": 18
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t22z.short_range.terrain_rt.f005.conus.nc",
                "name": "NWM Terrain Routing Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t22z.short_range.terrain_rt.f005.conus.nc",
                "data_datetime": "2018-11-20T22:00:00Z",
                "appearance_datetime": "2018-11-20T23:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T00:01:36.177571Z",
                "complete_datetime": "2018-11-27T00:01:40.086654Z",
                "failed_datetime": "2018-11-22T09:42:50.791917Z",
                "file_modification_datetime": "2018-11-20T23:38:22Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-22T09:42:50.791917Z",
                "cached_file": null,
                "file_size_in_bytes": 40379422,
                "extra_classes": {
            "forecast_hour": 5
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t22z.short_range.reservoir.f003.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t22z.short_range.reservoir.f003.conus.nc",
                "data_datetime": "2018-11-20T22:00:00Z",
                "appearance_datetime": "2018-11-20T23:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-28T19:30:33.381505Z",
                "complete_datetime": "2018-11-28T19:30:50.433396Z",
                "failed_datetime": "2018-11-23T10:24:14.018841Z",
                "file_modification_datetime": "2018-11-20T23:38:04Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-23T10:24:14.018841Z",
                "cached_file": null,
                "file_size_in_bytes": 54122,
                "extra_classes": {
            "forecast_hour": 3
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t19z.short_range.reservoir.f003.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t19z.short_range.reservoir.f003.conus.nc",
                "data_datetime": "2018-11-20T19:00:00Z",
                "appearance_datetime": "2018-11-20T20:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T07:15:41.467123Z",
                "complete_datetime": "2018-11-27T07:15:44.089097Z",
                "failed_datetime": "2018-11-20T20:49:06.689340Z",
                "file_modification_datetime": "2018-11-20T20:36:34Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-20T20:49:06.689340Z",
                "cached_file": null,
                "file_size_in_bytes": 54143,
                "extra_classes": {
            "forecast_hour": 3
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t19z.short_range.land.f010.conus.nc",
                "name": "NWM Land Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t19z.short_range.land.f010.conus.nc",
                "data_datetime": "2018-11-20T19:00:00Z",
                "appearance_datetime": "2018-11-20T20:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T07:18:40.447625Z",
                "complete_datetime": "2018-11-27T07:18:43.477484Z",
                "failed_datetime": "2018-11-20T20:47:35.452905Z",
                "file_modification_datetime": "2018-11-20T20:36:33Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-20T20:47:35.452905Z",
                "cached_file": null,
                "file_size_in_bytes": 43422011,
                "extra_classes": {
            "forecast_hour": 10
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t19z.short_range.channel_rt.f010.conus.nc",
                "name": "NWM Channel Routing Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t19z.short_range.channel_rt.f010.conus.nc",
                "data_datetime": "2018-11-20T19:00:00Z",
                "appearance_datetime": "2018-11-20T20:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T06:33:18.210130Z",
                "complete_datetime": "2018-11-27T06:33:31.665699Z",
                "failed_datetime": "2018-11-20T20:47:05.431570Z",
                "file_modification_datetime": "2018-11-20T20:36:28Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-20T20:47:05.431570Z",
                "cached_file": null,
                "file_size_in_bytes": 11862919,
                "extra_classes": {
            "forecast_hour": 10
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t16z.short_range.reservoir.f012.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t16z.short_range.reservoir.f012.conus.nc",
                "data_datetime": "2018-11-20T16:00:00Z",
                "appearance_datetime": "2018-11-20T17:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T13:47:49.990681Z",
                "complete_datetime": "2018-11-27T13:47:50.859096Z",
                "failed_datetime": "2018-11-20T17:51:14.597557Z",
                "file_modification_datetime": "2018-11-20T17:36:57Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-20T17:51:14.597557Z",
                "cached_file": null,
                "file_size_in_bytes": 54115,
                "extra_classes": {
            "forecast_hour": 12
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t13z.short_range.reservoir.f018.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t13z.short_range.reservoir.f018.conus.nc",
                "data_datetime": "2018-11-20T13:00:00Z",
                "appearance_datetime": "2018-11-20T14:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T06:38:21.059507Z",
                "complete_datetime": "2018-11-27T06:38:22.938196Z",
                "failed_datetime": "2018-11-20T14:46:46.804564Z",
                "file_modification_datetime": "2018-11-20T14:36:56Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-20T14:46:46.804564Z",
                "cached_file": null,
                "file_size_in_bytes": 54109,
                "extra_classes": {
            "forecast_hour": 18
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t12z.short_range.land.f013.conus.nc",
                "name": "NWM Land Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t12z.short_range.land.f013.conus.nc",
                "data_datetime": "2018-11-20T12:00:00Z",
                "appearance_datetime": "2018-11-20T13:45:00Z",
                "removal_datetime": "2018-11-22T00:00:00Z",
                "acquired_datetime": "2018-11-27T06:38:20.841913Z",
                "complete_datetime": "2018-11-27T06:38:24.540413Z",
                "failed_datetime": "2018-11-20T14:13:14.830001Z",
                "file_modification_datetime": "2018-11-20T14:05:16Z",
                "expire_datetime": null,
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-20T14:13:14.830001Z",
                "cached_file": null,
                "file_size_in_bytes": 44071679,
                "extra_classes": {
            "forecast_hour": 13
        }
        },
        {
            "source_file": "/pub/data/nccf/com/nwm/prod/nwm.20181120/short_range/nwm.t07z.short_range.reservoir.f014.conus.nc",
                "name": "NWM Reservoir Short Range",
                "destination_file": "nwm.20181120/short_range/nwm.t07z.short_range.reservoir.f014.conus.nc",
                "data_datetime": "2018-11-20T07:00:00Z",
                "appearance_datetime": "2018-11-20T08:45:00Z",
                "acquired_datetime": "2018-11-27T06:38:20.887084Z",
                "complete_datetime": "2018-11-27T06:38:22.309366Z",
                "file_modification_datetime": "2018-11-20T08:37:41Z",
                "cleaned_datetime": null,
                "cached_datetime": "2018-11-26T15:30:23.890346Z",
                "file_size_in_bytes": 54106
        }
  ]
    }

    we will then use the information in the "destination_file" field to create the link: ENDPOINT + "/" + BUCKET + "/" = destination_file
    That link will get us our data. We should decide on what destination_file s to download by globbing on the pattern. The user should add in
    the file type in the path. Ultimately, this probably needs to be a wholly different reader and we should probably have a different tag for this source
    since so many different attributes are needed.l*/

    /**
     * Since the format for immediate sub-buckets in the NWCAL object store is "nwm.yyyyMMdd", we
     * want to have a formatter that will allow us to find objects in the right sub-buckets
     */
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
        if ((ConfigHelper.isForecast( this.dataSourceConfig ) && earliestIssue.isBefore( LocalDateTime.MAX)) ||
            earliestIssue.isBefore( earliestValidDate ))
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
        else if (ConfigHelper.isForecast( this.getDataSourceConfig() ) && latestIssue != null)
        {
            latest = latestIssue.toLocalDate();
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

    @Override
    protected Collection<ETagKey> getIngestableObjects()
    {
        AmazonS3 connection = this.getConnection();
        List<ETagKey> ingestableObjects = new ArrayList<>(  );
        for (PrefixPattern prefixPattern : this.getPrefixPatterns())
        {
            ListObjectsRequest request = new ListObjectsRequest(  );
            request.setPrefix( prefixPattern.getPrefix() );
            request.setBucketName( this.getBucketName() );
            request.setMaxKeys( this.getMaxKeyCount() );

            final PathMatcher matcher = FileSystems.getDefault()
                                                   .getPathMatcher( "glob:" + prefixPattern.getPattern() );

            ObjectListing s3Objects = null;

            int listAttempt = 0;

            do
            {
                try
                {
                    s3Objects = connection.listObjects( request );
                }
                catch (SdkClientException clientException)
                {
                    listAttempt++;

                    // If we've exceeded our amount of allowable retries, rethrow the exception.
                    if (listAttempt > this.getRetryCount())
                    {
                        throw clientException;
                    }

                    this.getLogger().debug( "S3 exception encountered while "
                                            + "trying to get a list of objects to download. "
                                            + "Trying again...", clientException );
                }
            } while (s3Objects == null);

            do
            {
                s3Objects.getObjectSummaries().forEach( summary -> {
                    if (matcher.matches( Paths.get( summary.getKey()) ))
                    {
                        ingestableObjects.add(new ETagKey( summary.getETag(), summary.getKey() ));
                    }
                } );

                listAttempt = 0;

                do
                {
                    try
                    {
                        s3Objects = connection.listNextBatchOfObjects( s3Objects );
                        break;
                    }
                    catch ( SdkClientException clientException )
                    {
                        listAttempt++;

                        if (listAttempt > this.getRetryCount())
                        {
                            throw clientException;
                        }

                        this.getLogger().debug( "S3 exception encountered while retrieving the next "
                                                + "set of results. Trying again...", clientException );
                    }
                } while (true);
            } while ( !s3Objects.getObjectSummaries().isEmpty() );
        }

        return ingestableObjects;
    }

    /**
     * @return The earliest day to retrieve data for based off of the configured valid times.
     * Returns LocalDateTime.MAX if none were configured.
     */
    private LocalDateTime getEarliestValidDate()
    {
        LocalDateTime date = LocalDateTime.MAX;

        // If the earliest valid date is configured, get the LocalDateTime representation of it
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

    /**
     * @return The latest day to retrieve data for based off of the configured valid times
     * Returns null if no latest valid time were configured.
     */
    private LocalDateTime getLatestValidDate()
    {
        LocalDateTime date = null;

        // If the latest valid date is configured, get the LocalDateTime representation of it
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

    /**
     * Creates an object representing the earliest day to retrieve data for
     * based off of the configured issue dates. If an earliest issue date wasn't
     * configured, the MAX date is returned
     * @return The earliest day to retrieve data for when considering the configured issue date
     */
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

    /**
     * Creates an object representing the latest day to retrieve data for based
     * off of the configured issued dates. If a latest issue date wasn't configured,
     * null is returned
     * @return The latest day to retrieve data for when considering the configured issue date
     */
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
        DataSourceConfig.Source source = this.getSourceConfig();

        if (source != null && source.getBucket() != null)
        {
            return source.getBucket();
        }

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
    int getRetryCount()
    {
        return NWCALReader.RETRY_COUNT;
    }

    @Override
    protected Logger getLogger()
    {
        return NWCALReader.LOGGER;
    }
}
