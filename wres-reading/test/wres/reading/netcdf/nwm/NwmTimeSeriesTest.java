package wres.reading.netcdf.nwm;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import wres.config.yaml.components.SourceInterface;
import wres.datamodel.types.Ensemble;
import wres.datamodel.time.TimeSeries;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

public class NwmTimeSeriesTest
{
    private static final String FINISHED_OPENING_FORECAST_FILES_NOW_READING =
            "Finished opening forecast files, now reading...";

    private static final String T2019_10_06T02_00_00Z = "2019-10-06T02:00:00Z";

    private static final String HERE_IS_THE_TIMESERIES = "Here is the timeseries: {}";

    private static final String OPENING_A_FORECAST_BASED_ON = "Opening a forecast based on {}";

    private static final String ANALYSIS_ASSIM = "analysis_assim";

    private static final String LAND = "land";

    private static final String MEDIUM_RANGE = "medium_range";

    private static final String CONUS = "conus";

    private static final String CHANNEL_RT = "channel_rt";

    private static final String SHORT_RANGE = "short_range";

    private static final String CMS = "CMS";

    private static final String STREAMFLOW = "streamflow";

    private static final Logger LOGGER = LoggerFactory.getLogger( NwmTimeSeriesTest.class );

    @Test
    public void generateFakeNwmForecastNames()
    {
        NwmProfile nwmProfile = new NwmProfile( 5,
                                                1,
                                                Duration.ofHours( 3 ),
                                                true,
                                                "fake_range",
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                "fake_dir_prefix",
                                                "fake_location_label",
                                                Duration.ofHours( 9001 ),
                                                false,
                                                Duration.ZERO );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T08:00:00Z" ),
                                                       URI.create( "https://test/" ) );

        Set<URI> expected = Set.of( URI.create(
                                            "https://test/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f003.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f006.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f009.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f012.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f015.fake_location_label.nc" ) );
        assertEquals( expected, actual );
    }

    /**
     * Issue #110561.
     */

    @Test
    public void generateFakeNwmForecastNamesWhenBaseUriIsMissingLastSlash()
    {
        NwmProfile nwmProfile = new NwmProfile( 5,
                                                1,
                                                Duration.ofHours( 3 ),
                                                true,
                                                "fake_range",
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                "fake_dir_prefix",
                                                "fake_location_label",
                                                Duration.ofHours( 9001 ),
                                                false,
                                                Duration.ZERO );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T08:00:00Z" ),
                                                       URI.create( "https://test/2.0" ) );

        Set<URI> expected = Set.of( URI.create(
                                            "https://test/2.0/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f003.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/2.0/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f006.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/2.0/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f009.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/2.0/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f012.fake_location_label.nc" ),
                                    URI.create(
                                            "https://test/2.0/nwm.20191006/fake_dir_prefix/nwm.t08z.fake_range.channel_rt.f015.fake_location_label.nc" ) );
        assertEquals( expected, actual );
    }

    @Test
    public void generateShortRangeForecastNames()
    {
        NwmProfile nwmProfile = new NwmProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T08:00:00Z" ),
                                                       URI.create( "https://test/" ) );

        Set<URI> expected = Set.of( URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f001.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f002.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f003.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f004.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f005.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f006.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f007.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f008.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f009.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f010.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f011.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f012.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f013.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f014.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f015.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f016.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f017.conus.nc" ),
                                    URI.create(
                                            "https://test/nwm.20191006/short_range/nwm.t08z.short_range.channel_rt.f018.conus.nc" ) );
        assertEquals( expected, actual );
    }

    @Test
    public void generateShortRangeForecastNamesWithS3Interface()
    {
        // GitHub #75
        NwmProfile nwmProfile = new NwmProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T08:00:00Z" ),
                                                       URI.create( "cdms3://test/" ) );

        Set<URI> expected = Set.of( URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f001.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f002.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f003.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f004.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f005.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f006.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f007.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f008.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f009.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f010.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f011.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f012.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f013.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f014.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f015.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f016.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f017.conus.nc" ),
                                    URI.create(
                                            "cdms3://test/nwm.20191006/short_range?nwm.t08z.short_range.channel_rt.f018.conus.nc" ) );
        assertEquals( expected, actual );
    }

    @Test
    public void generateFakeMediumRangeLandEnsembleForecastNames()
    {
        NwmProfile nwmProfile = new NwmProfile( 7,
                                                3,
                                                Duration.ofHours( 3 ),
                                                false,
                                                MEDIUM_RANGE,
                                                LAND,
                                                NwmProfile.TimeLabel.F,
                                                MEDIUM_RANGE,
                                                CONUS,
                                                Duration.ofHours( 6 ),
                                                true,
                                                Duration.ZERO );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T18:00:00Z" ),
                                                       URI.create( "file:///test/" ) );

        Set<URI> expected = Set.of( URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f003.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f006.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f009.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f012.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f015.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f018.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem1/nwm.t18z.medium_range.land_1.f021.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f003.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f006.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f009.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f012.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f015.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f018.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem2/nwm.t18z.medium_range.land_2.f021.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f003.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f006.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f009.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f012.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f015.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f018.conus.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/medium_range_mem3/nwm.t18z.medium_range.land_3.f021.conus.nc" ) );
        assertEquals( expected, actual );
    }

    /**
     * #110992
     */

    @Test
    public void generateFakeMediumRangeDeterministicForecastNamesForLegacyNwm1_2()
    {
        NwmProfile nwmProfile =
                NwmProfiles.getProfileFromShortHand( SourceInterface.NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2019-10-06T18:00:00Z" ),
                                                       URI.create( "file:///test/1.2/" ) );

        String template = "file:/test/1.2/nwm.20191006/medium_range/nwm.t18z.medium_range.channel_rt.fqux.conus.nc";
        Set<URI> expected = new TreeSet<>();
        for ( int i = 1; i < 81; i++ )
        {
            String nextTime = String.format( "%03d", i * 3 );
            String nextUriString = template.replaceAll( "qux", nextTime );
            URI nextUri = URI.create( nextUriString );
            expected.add( nextUri );
        }

        assertEquals( expected, actual );
    }

    @Test
    public void generateAnalysisAssimNames()
    {
        NwmProfile nwmProfile = new NwmProfile( 3,
                                                1,
                                                Duration.ofHours( 1 ),
                                                false,
                                                ANALYSIS_ASSIM,
                                                "reservoir",
                                                NwmProfile.TimeLabel.TM,
                                                "analysis_assim_hawaii",
                                                "hawaii",
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse(
                                                               T2019_10_06T02_00_00Z ),
                                                       URI.create( "file:///test/" ) );

        Set<URI> expected = Set.of( URI.create(
                                            "file:///test/nwm.20191006/analysis_assim_hawaii/nwm.t02z.analysis_assim.reservoir.tm00.hawaii.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/analysis_assim_hawaii/nwm.t02z.analysis_assim.reservoir.tm01.hawaii.nc" ),
                                    URI.create(
                                            "file:///test/nwm.20191006/analysis_assim_hawaii/nwm.t02z.analysis_assim.reservoir.tm02.hawaii.nc" ) );
        assertEquals( expected, actual );
    }


    @Test
    public void generateAnalysisAssimHawaiiNames()
    {
        NwmProfile nwmProfile =
                NwmProfiles.getProfileFromShortHand( SourceInterface.NWM_ANALYSIS_ASSIM_CHANNEL_RT_HAWAII );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2020-08-05T04:00:00Z" ),
                                                       URI.create( "file:///test/" ) );

        Set<URI> expected = Set.of(
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0000.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0015.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0030.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0045.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0100.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0115.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0130.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0145.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0200.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0215.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0230.hawaii.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_hawaii/nwm.t04z.analysis_assim.channel_rt.tm0245.hawaii.nc" ) );

        assertEquals( expected, actual );
    }


    @Test
    public void generateAnalysisAssimNoDaPuertoRicoNames()
    {
        NwmProfile nwmProfile =
                NwmProfiles.getProfileFromShortHand( SourceInterface.NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_PUERTORICO );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2020-08-05T05:00:00Z" ),
                                                       URI.create( "file:///test/" ) );

        Set<URI> expected = Set.of(
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_puertorico_no_da/nwm.t05z.analysis_assim_no_da.channel_rt.tm00.puertorico.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_puertorico_no_da/nwm.t05z.analysis_assim_no_da.channel_rt.tm01.puertorico.nc" ),
                URI.create(
                        "file:///test/nwm.20200805/analysis_assim_puertorico_no_da/nwm.t05z.analysis_assim_no_da.channel_rt.tm02.puertorico.nc" ) );
        assertEquals( expected, actual );
    }

    @Test
    public void generateAnalysisAssimExtendNoDaConusNames()
    {
        NwmProfile nwmProfile =
                NwmProfiles.getProfileFromShortHand( SourceInterface.NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_CONUS );
        Set<URI> actual = NwmTimeSeries.getNetcdfUris( nwmProfile,
                                                       Instant.parse( "2020-08-04T16:00:00Z" ),
                                                       URI.create( "file:///test/" ) );

        Set<URI> expected = Set.of(
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm00.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm01.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm02.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm03.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm04.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm05.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm06.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm07.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm08.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm09.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm10.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm11.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm12.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm13.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm14.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm15.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm16.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm17.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm18.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm19.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm20.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm21.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm22.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm23.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm24.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm25.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm26.conus.nc" ),
                URI.create(
                        "file:///test/nwm.20200804/analysis_assim_extend_no_da/nwm.t16z.analysis_assim_extend_no_da.channel_rt.tm27.conus.nc" ) );

        assertEquals( expected, actual );
    }


    @Test
    // To try this against the real service, use a new date, remove @Ignore
    @Ignore( "Live testing example, ignore for automated/build testing" )
    public void openForecastsFromNomads()
    {
        // To see it fail to find a file, change blobCount to 4
        NwmProfile nwmProfile = new NwmProfile( 3,
                                                1,
                                                Duration.ofHours( 1 ),
                                                false,
                                                ANALYSIS_ASSIM,
                                                "reservoir",
                                                NwmProfile.TimeLabel.TM,
                                                ANALYSIS_ASSIM,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );

        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               new long[] {},
                                                               Instant.parse( "2019-10-23T02:00:00Z" ),
                                                               ReferenceTimeType.ANALYSIS_START_TIME,
                                                               URI.create(
                                                                       "https://example.gov/pub/data/nccf/com/nwm/prod/" ) ) )
        {
            assertEquals( nwmProfile, nwmTimeSeries.getProfile() );
            assertNotNull( nwmProfile );
            assertEquals( 3, nwmTimeSeries.countOfNetcdfFiles() );
        }
    }

    @Test
    // To try this against the real service, use a new date, remove @Ignore
    @Ignore( "Live testing example, ignore for automated/build testing" )
    public void readForecastFromNomads()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 25
        NwmProfile nwmProfile = new NwmProfile( 2,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );

        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long featureId = 18384141;
        try ( NwmTimeSeries nwmTimeSeries =
                      new NwmTimeSeries( nwmProfile,
                                         new long[] { featureId },
                                         Instant.parse( "2019-10-22T02:00:00Z" ),
                                         ReferenceTimeType.T0,
                                         URI.create( "https://example.gov/pub/data/nccf/com/nwm/prod/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            TimeSeries<?> timeSeries =
                    nwmTimeSeries.readSingleValuedTimeSerieses( STREAMFLOW, new long[] { featureId }, CMS )
                                 .get( featureId );
            LOGGER.info( HERE_IS_THE_TIMESERIES, timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore( "Live testing example, ignore for automated/build testing" )
    public void readNWM20ShortRangeForecastFromDstore()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 25
        NwmProfile nwmProfile = new NwmProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );

        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long featureId = 18384141;
        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               new long[] { featureId },
                                                               Instant.parse( T2019_10_06T02_00_00Z ),
                                                               ReferenceTimeType.T0,
                                                               URI.create( "https://dstore-fqdn/nwm/2.0/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            TimeSeries<?> timeSeries =
                    nwmTimeSeries.readSingleValuedTimeSerieses( STREAMFLOW, new long[] { featureId }, CMS )
                                 .get( featureId );
            LOGGER.info( HERE_IS_THE_TIMESERIES, timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }

    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore( "Live testing example, ignore for automated/build testing" )
    public void readNWM12ShortRangeForecastFromDstore()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 25
        NwmProfile nwmProfile = new NwmProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );

        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long featureId = 18384141;
        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               new long[] { featureId },
                                                               Instant.parse( "2018-05-06T04:00:00Z" ),
                                                               ReferenceTimeType.T0,
                                                               URI.create( "https://dstore-fqdn/nwm/1.2/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            TimeSeries<?> timeSeries =
                    nwmTimeSeries.readSingleValuedTimeSerieses( STREAMFLOW, new long[] { featureId }, CMS )
                                 .get( featureId );
            LOGGER.info( HERE_IS_THE_TIMESERIES, timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore( "Live testing example, ignore for automated/build testing" )
    public void readNWM11ShortRangeForecastFromDstore()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 25
        NwmProfile nwmProfile = new NwmProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );

        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long featureId = 18384141;

        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               new long[] { featureId },
                                                               Instant.parse( "2017-10-06T17:00:00Z" ),
                                                               ReferenceTimeType.T0,
                                                               URI.create( "https://dstore-fqdn/nwm/1.1/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            TimeSeries<?> timeSeries =
                    nwmTimeSeries.readSingleValuedTimeSerieses( STREAMFLOW, new long[] { featureId }, CMS )
                                 .get( featureId );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    @Ignore( "Live testing example, ignore for automated/build testing" )
    // If you want to try this against real service, remove @Ignore.
    // Fails because NWM 1.0 had .nc.gz extension.
    public void readNWM10ShortRangeForecastFromDstore()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 25
        NwmProfile nwmProfile = new NwmProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );
        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long featureId = 18384141;

        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               new long[] { featureId },
                                                               Instant.parse( "2016-10-18T17:00:00Z" ),
                                                               ReferenceTimeType.T0,
                                                               URI.create( "https://dstore-fqdn/nwm/1.0/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            TimeSeries<?> timeSeries =
                    nwmTimeSeries.readSingleValuedTimeSerieses( STREAMFLOW, new long[] { featureId }, CMS )
                                 .get( featureId );
            LOGGER.info( HERE_IS_THE_TIMESERIES, timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }

    @Test
    @Ignore( "Live testing example, ignore for automated/build testing" )
    // If you want to try this against real volume, remove @Ignore.
    public void readForecastFromFilesystem()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 25
        NwmProfile nwmProfile = new NwmProfile( 18,
                                                1,
                                                Duration.ofHours( 1 ),
                                                true,
                                                SHORT_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                SHORT_RANGE,
                                                CONUS,
                                                Duration.ofHours( 1 ),
                                                false,
                                                Duration.ZERO );

        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long featureId = 18384141;
        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               new long[] { featureId },
                                                               Instant.parse( T2019_10_06T02_00_00Z ),
                                                               ReferenceTimeType.T0,
                                                               URI.create( "H:/netcdf_data/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            TimeSeries<?> timeSeries =
                    nwmTimeSeries.readSingleValuedTimeSerieses( STREAMFLOW, new long[] { featureId }, CMS )
                                 .get( featureId );
            LOGGER.info( HERE_IS_THE_TIMESERIES, timeSeries );
            assertNotNull( timeSeries );
            assertNotEquals( 0, timeSeries.getEvents().size() );
        }
    }


    @Test
    // If you want to try this against real service, set FQDN, remove @Ignore.
    @Ignore( "Live testing example, ignore for automated/build testing" )
    public void readNWM20MediumRangeForecastFromDstore()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 70
        // To see it run OutOfMemoryError, change blobCount to full 68
        NwmProfile nwmProfile = new NwmProfile( 68,
                                                7,
                                                Duration.ofHours( 3 ),
                                                true,
                                                MEDIUM_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                MEDIUM_RANGE,
                                                CONUS,
                                                Duration.ofHours( 6 ),
                                                true,
                                                Duration.ZERO );

        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long[] featureIds = new long[] { 18384141, 18696047, 942030011 };

        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               featureIds,
                                                               Instant.parse( "2019-10-21T06:00:00Z" ),
                                                               ReferenceTimeType.T0,
                                                               URI.create( "https://dstore-fqdn/nwm/2.0/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            Map<Long, TimeSeries<Ensemble>> timeSerieses = nwmTimeSeries.readEnsembleTimeSerieses( STREAMFLOW,
                                                                                                   featureIds,
                                                                                                   CMS );
            TimeSeries<Ensemble> timeSeries1 = timeSerieses.get( featureIds[0] );
            LOGGER.info( "Here is timeSeries 1: {}", timeSeries1 );
            TimeSeries<Ensemble> timeSeries2 = timeSerieses.get( featureIds[1] );
            LOGGER.info( "Here is timeseries 2: {}", timeSeries2 );
            TimeSeries<Ensemble> timeSeries3 = timeSerieses.get( featureIds[2] );
            LOGGER.info( "Here is timeseries 3: {}", timeSeries3 );
            assertNotNull( timeSeries1 );
            assertNotNull( timeSeries2 );
            assertNotNull( timeSeries3 );
            assertNotEquals( 0, timeSeries1.getEvents().size() );
            assertNotEquals( 0, timeSeries2.getEvents().size() );
            assertNotEquals( 0, timeSeries3.getEvents().size() );
        }
    }


    @Test
    // To try this against real filesystem, download data, set URI, remove @Ignore.
    @Ignore( "Live testing example, ignore for automated/build testing" )
    public void readNWM20MediumRangeForecastFromFilesystem()
            throws InterruptedException, ExecutionException
    {
        // To see it fail to find a file, change blobCount to 70
        // To see it run OutOfMemoryError, change blobCount to full 68
        NwmProfile nwmProfile = new NwmProfile( 68,
                                                7,
                                                Duration.ofHours( 3 ),
                                                true,
                                                MEDIUM_RANGE,
                                                CHANNEL_RT,
                                                NwmProfile.TimeLabel.F,
                                                MEDIUM_RANGE,
                                                CONUS,
                                                Duration.ofHours( 6 ),
                                                true,
                                                Duration.ZERO );

        LOGGER.info( OPENING_A_FORECAST_BASED_ON, nwmProfile );

        long[] featureIds = new long[] { 18384141, 18696047, 942030011 };

        try ( NwmTimeSeries nwmTimeSeries = new NwmTimeSeries( nwmProfile,
                                                               featureIds,
                                                               Instant.parse( "2019-10-21T06:00:00Z" ),
                                                               ReferenceTimeType.T0,
                                                               URI.create( "C:/nwm_data/" ) ) )
        {
            LOGGER.info( FINISHED_OPENING_FORECAST_FILES_NOW_READING );
            Map<Long, TimeSeries<Ensemble>> timeSerieses = nwmTimeSeries.readEnsembleTimeSerieses( STREAMFLOW,
                                                                                                   featureIds,
                                                                                                   CMS );
            TimeSeries<Ensemble> timeSeries1 = timeSerieses.get( featureIds[0] );
            LOGGER.info( "Here is timeSeries 1: {}", timeSeries1 );
            TimeSeries<Ensemble> timeSeries2 = timeSerieses.get( featureIds[1] );
            LOGGER.info( "Here is timeseries 2: {}", timeSeries2 );
            TimeSeries<Ensemble> timeSeries3 = timeSerieses.get( featureIds[2] );
            LOGGER.info( "Here is timeseries 3: {}", timeSeries3 );
            assertNotNull( timeSeries1 );
            assertNotNull( timeSeries2 );
            assertNotNull( timeSeries3 );
            assertNotEquals( 0, timeSeries1.getEvents().size() );
            assertNotEquals( 0, timeSeries2.getEvents().size() );
            assertNotEquals( 0, timeSeries3.getEvents().size() );
        }
    }
}