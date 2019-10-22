package wres.io.reading.nwm;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import wres.datamodel.Ensemble;
import wres.datamodel.time.TimeSeries;

public class NWMTimeSeriesTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NWMTimeSeriesTest.class );

    @Test
    public void generateFakeNwmForecastNames()
    {
        NWMProfile nwmProfile = new NWMProfile( 5,
                                                1,
                                                Duration.ofHours( 3 ),
                                                true,
                                                "fake_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );
        Set<URI> actual = NWMTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T08:00:00Z" ),
                                                       URI.create( "https://test/" ) );

        Set<URI> expected = Set.of( URI.create( "https://test/nwm.20191006/fake_range/nwm.t08z.fake_range.channel_rt.f003.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/fake_range/nwm.t08z.fake_range.channel_rt.f006.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/fake_range/nwm.t08z.fake_range.channel_rt.f009.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/fake_range/nwm.t08z.fake_range.channel_rt.f012.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/fake_range/nwm.t08z.fake_range.channel_rt.f015.conus.nc" ) );
        assertEquals( expected, actual );
    }

    @Test
    public void generateShortRangeForecastNames()
    {
        NWMProfile nwmProfile = new NWMProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                "short_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );
        Set<URI> actual = NWMTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T08:00:00Z" ),
                                                       URI.create( "https://test/" ) );

        Set<URI> expected = Set.of( URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f001.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f002.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f003.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f004.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f005.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f006.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f007.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f008.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f009.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f010.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f011.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f012.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f013.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f014.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f015.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f016.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f017.conus.nc" ),
                                    URI.create( "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f018.conus.nc" ) );
        assertEquals( expected, actual );
    }


    @Test
    public void generateFakeMediumRangeLandEnsembleForecastNames()
    {
        NWMProfile nwmProfile = new NWMProfile( 7,
                                                3,
                                                Duration.ofHours( 3 ),
                                                false,
                                                "medium_range",
                                                "land",
                                                NWMProfile.TimeLabel.f );
        Set<URI> actual = NWMTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T18:00:00Z" ),
                                                       URI.create( "file:///test/" ) );

        Set<URI> expected = Set.of( URI.create( "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f003.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f006.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f009.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f012.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f015.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f018.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f021.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f003.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f006.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f009.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f012.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f015.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f018.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f021.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f003.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f006.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f009.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f012.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f015.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f018.conus.nc" ),
                                    URI.create( "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f021.conus.nc" ) );
        assertEquals( expected, actual );
    }


    @Test
    public void generateAnalysisAssimNames()
    {
        NWMProfile nwmProfile = new NWMProfile( 3,
                                                1,
                                                Duration.ofHours( 1 ),
                                                false,
                                                "analysis_assim",
                                                "reservoir",
                                                NWMProfile.TimeLabel.tm );
        Set<URI> actual = NWMTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse(
                                                             "2019-10-06T02:00:00Z" ),
                                                       URI.create( "file:///test/" ) );

        Set<URI> expected = Set.of( URI.create(
                "file:///test/nwm.20191006/analysis_assim/nwm.t02z.analysis_assim.reservoir.tm00.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/analysis_assim/nwm.t02z.analysis_assim.reservoir.tm01.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/analysis_assim/nwm.t02z.analysis_assim.reservoir.tm02.conus.nc" ) );
        assertEquals( expected, actual );
    }

    @Test
    // To try this against the real service, use a new date, remove @Ignore
    @Ignore
    public void openForecastsFromNomads()
    {
        // To see it fail to find a file, change blobCount to 4
        NWMProfile nwmProfile = new NWMProfile( 3,
                                                1,
                                                Duration.ofHours( 1 ),
                                                false,
                                                "analysis_assim",
                                                "reservoir",
                                                NWMProfile.TimeLabel.tm );

        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2019-10-22T02:00:00Z" ),
                                                               URI.create( "https://nomads.ncep.***REMOVED***/pub/data/nccf/com/nwm/prod/" ) ) )
        {
            assertEquals( nwmProfile, nwmTimeSeries.getProfile() );
            assertNotNull( nwmProfile );
            assertEquals( 3, nwmTimeSeries.countOfNetcdfFiles() );
        }
    }

    @Test
    // To try this against the real service, use a new date, remove @Ignore
    @Ignore
    public void readForecastFromNomads()
    {
        // To see it fail to find a file, change blobCount to 25
        NWMProfile nwmProfile = new NWMProfile( 2,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                "short_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );

        LOGGER.info( "Opening a forecast based on {}", nwmProfile );

        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2019-10-22T02:00:00Z" ),
                                                               URI.create( "https://nomads.ncep.***REMOVED***/pub/data/nccf/com/nwm/prod/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Double> timeSeries = nwmTimeSeries.readTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore
    public void readNWM20ShortRangeForecastFromDstore()
    {
        // To see it fail to find a file, change blobCount to 25
        NWMProfile nwmProfile = new NWMProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                "short_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );

        LOGGER.info( "Opening a forecast based on {}", nwmProfile );

        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2019-10-06T02:00:00Z" ),
                                                               URI.create( "https://dstore-fqdn/nwm/2.0/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Double> timeSeries = nwmTimeSeries.readTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }

    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore
    public void readNWM12ShortRangeForecastFromDstore()
    {
        // To see it fail to find a file, change blobCount to 25
        NWMProfile nwmProfile = new NWMProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                "short_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );

        LOGGER.info( "Opening a forecast based on {}", nwmProfile );

        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2018-05-06T04:00:00Z" ),
                                                               URI.create( "https://dstore-fqdn/nwm/1.2/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Double> timeSeries = nwmTimeSeries.readTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore
    public void readNWM11ShortRangeForecastFromDstore()
    {
        // To see it fail to find a file, change blobCount to 25
        NWMProfile nwmProfile = new NWMProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                "short_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );

        LOGGER.info( "Opening a forecast based on {}", nwmProfile );
        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2017-10-06T17:00:00Z" ),
                                                               URI.create( "https://dstore-fqdn/nwm/1.1/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Double> timeSeries = nwmTimeSeries.readTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    @Ignore // If you want to try this against real service, remove @Ignore.
    // Fails because NWM 1.0 had .nc.gz extension.
    public void readNWM10ShortRangeForecastFromDstore()
    {
        // To see it fail to find a file, change blobCount to 25
        NWMProfile nwmProfile = new NWMProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                "short_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );
        LOGGER.info( "Opening a forecast based on {}", nwmProfile );

        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2016-10-18T17:00:00Z" ),
                                                               URI.create( "https://dstore-fqdn/nwm/1.0/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Double> timeSeries = nwmTimeSeries.readTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }

    @Test
    @Ignore // If you want to try this against real volume, remove @Ignore.
    public void readForecastFromFilesystem()
    {
        // To see it fail to find a file, change blobCount to 25
        NWMProfile nwmProfile = new NWMProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                "short_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );

        LOGGER.info( "Opening a forecast based on {}", nwmProfile );
        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2019-10-06T02:00:00Z" ),
                                                               URI.create( "H:/netcdf_data/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Double> timeSeries = nwmTimeSeries.readTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore
    public void readNWM20MediumRangeForecastFromDstore()
    {
        // To see it fail to find a file, change blobCount to 70
        // To see it run OutOfMemoryError, change blobCount to full 68
        NWMProfile nwmProfile = new NWMProfile( 68,
                                                7,
                                                Duration.ofHours( 3 ),
                                                true,
                                                "medium_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );

        LOGGER.info( "Opening a forecast based on {}", nwmProfile );

        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2019-10-21T06:00:00Z" ),
                                                               URI.create( "https://dstore-fqdn/nwm/2.0/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Ensemble> timeSeries = nwmTimeSeries.readEnsembleTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            TimeSeries<Ensemble> timeSeries2 = nwmTimeSeries.readEnsembleTimeSeries( 18696047, "streamflow" );
            LOGGER.info( "Here is timeseries 2: {}", timeSeries2 );
            TimeSeries<Ensemble> timeSeries3 = nwmTimeSeries.readEnsembleTimeSeries( 942030011, "streamflow" );
            LOGGER.info( "Here is timeseries 3: {}", timeSeries3 );
            assertNotNull( timeSeries );
            assertNotNull( timeSeries2 );
            assertNotNull( timeSeries3 );
            assertNotEquals( 0, timeSeries.getEvents().size() );
            assertNotEquals( 0, timeSeries2.getEvents().size() );
            assertNotEquals( 0, timeSeries3.getEvents().size() );
        }
    }


    @Test
    // To try this against real filesystem, download data, set URI, remove @Ignore.
    @Ignore
    public void readNWM20MediumRangeForecastFromFilesystem()
    {
        // To see it fail to find a file, change blobCount to 70
        // To see it run OutOfMemoryError, change blobCount to full 68
        NWMProfile nwmProfile = new NWMProfile( 68,
                                                7,
                                                Duration.ofHours( 3 ),
                                                true,
                                                "medium_range",
                                                "channel_rt",
                                                NWMProfile.TimeLabel.f );

        LOGGER.info( "Opening a forecast based on {}", nwmProfile );

        try ( NWMTimeSeries nwmTimeSeries = new NWMTimeSeries( nwmProfile,
                                                               Instant.parse( "2019-10-21T06:00:00Z" ),
                                                               URI.create( "C:/nwm_data/" ) ) )
        {
            LOGGER.info( "Finished opening forecast files, now reading..." );
            TimeSeries<Ensemble> timeSeries = nwmTimeSeries.readEnsembleTimeSeries( 18384141, "streamflow" );
            LOGGER.info( "Here is the timeseries: {}", timeSeries );
            TimeSeries<Ensemble> timeSeries2 = nwmTimeSeries.readEnsembleTimeSeries( 18696047, "streamflow" );
            LOGGER.info( "Here is timeseries 2: {}", timeSeries2 );
            TimeSeries<Ensemble> timeSeries3 = nwmTimeSeries.readEnsembleTimeSeries( 942030011, "streamflow" );
            LOGGER.info( "Here is timeseries 3: {}", timeSeries3 );
            assertNotNull( timeSeries );
            assertNotNull( timeSeries2 );
            assertNotNull( timeSeries3 );
            assertNotEquals( 0, timeSeries.getEvents().size() );
            assertNotEquals( 0, timeSeries2.getEvents().size() );
            assertNotEquals( 0, timeSeries3.getEvents().size() );
        }
    }
}
