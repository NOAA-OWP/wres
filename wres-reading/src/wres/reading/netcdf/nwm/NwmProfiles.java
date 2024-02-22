package wres.reading.netcdf.nwm;

import java.time.Duration;

import wres.config.yaml.components.SourceInterface;

/**
 * <p>Static helper class to generate NWM profiles for NWM versions.
 *
 * <p>Maps short-hand declarations to the ten parameters required to describe an
 * NWM dataset sufficient for successful retrieval.
 *
 * <p>Survey of the directories on 2019-10-23:
 *
 * <p>Index of /pub/data/nccf/com/nwm/prod/nwm.20191023
 *
 * <p>Name                           Last modified      Size  Parent Directory                                    -
 * analysis_assim/                23-Oct-2019 17:48    -
 * analysis_assim_hawaii/         23-Oct-2019 17:58    -
 * analysis_assim_long/           23-Oct-2019 12:50    -
 * forcing_analysis_assim/        23-Oct-2019 17:35    -
 * forcing_analysis_assim_extend/ 23-Oct-2019 18:23    -
 * forcing_analysis_assim_hawaii/ 23-Oct-2019 14:28    -
 * forcing_medium_range/          23-Oct-2019 16:40    -
 * forcing_short_range/           23-Oct-2019 18:28    -
 * forcing_short_range_hawaii/    23-Oct-2019 14:28    -
 * long_range_mem1/               23-Oct-2019 17:28    -
 * long_range_mem2/               23-Oct-2019 15:41    -
 * long_range_mem3/               23-Oct-2019 15:36    -
 * long_range_mem4/               23-Oct-2019 15:35    -
 * medium_range_mem1/             23-Oct-2019 17:53    -
 * medium_range_mem2/             23-Oct-2019 17:42    -
 * medium_range_mem3/             23-Oct-2019 17:43    -
 * medium_range_mem4/             23-Oct-2019 17:38    -
 * medium_range_mem5/             23-Oct-2019 17:40    -
 * medium_range_mem6/             23-Oct-2019 17:40    -
 * medium_range_mem7/             23-Oct-2019 17:40    -
 * short_range/                   23-Oct-2019 17:41    -
 * short_range_hawaii/            23-Oct-2019 14:58    -
 * usgs_timeslices/               23-Oct-2019 18:31    -
 *
 * <p>Survey of directories on para 2020-08-03
 * Index of /pub/data/nccf/com/nwm/para/nwm.20200802
 *
 * <p>Name                               Last modified      Size  Parent Directory                                        -
 * analysis_assim/                    02-Aug-2020 23:48    -
 * analysis_assim_extend/             03-Aug-2020 21:01    -
 * analysis_assim_extend_no_da/       03-Aug-2020 21:00    -
 * analysis_assim_hawaii/             03-Aug-2020 21:01    -
 * analysis_assim_hawaii_no_da/       03-Aug-2020 00:40    -
 * analysis_assim_long/               03-Aug-2020 21:02    -
 * analysis_assim_long_no_da/         03-Aug-2020 21:00    -
 * analysis_assim_no_da/              03-Aug-2020 21:00    -
 * analysis_assim_puertorico/         03-Aug-2020 21:02    -
 * analysis_assim_puertorico_no_da/   02-Aug-2020 23:58    -
 * forcing_analysis_assim/            02-Aug-2020 23:33    -
 * forcing_analysis_assim_extend/     02-Aug-2020 18:22    -
 * forcing_analysis_assim_hawaii/     03-Aug-2020 00:25    -
 * forcing_analysis_assim_puertorico/ 02-Aug-2020 23:45    -
 * forcing_medium_range/              03-Aug-2020 21:17    -
 * forcing_short_range/               03-Aug-2020 21:17    -
 * forcing_short_range_hawaii/        02-Aug-2020 15:02    -
 * forcing_short_range_puertorico/    03-Aug-2020 21:17    -
 * long_range_mem1/                   03-Aug-2020 14:27    -
 * long_range_mem2/                   03-Aug-2020 04:26    -
 * long_range_mem3/                   03-Aug-2020 21:00    -
 * long_range_mem4/                   03-Aug-2020 04:27    -
 * medium_range_mem1/                 03-Aug-2020 21:17    -
 * medium_range_mem2/                 03-Aug-2020 21:02    -
 * medium_range_mem3/                 03-Aug-2020 01:40    -
 * medium_range_mem4/                 03-Aug-2020 21:17    -
 * medium_range_mem5/                 03-Aug-2020 21:03    -
 * medium_range_mem6/                 03-Aug-2020 21:17    -
 * medium_range_mem7/                 03-Aug-2020 21:17    -
 * medium_range_no_da/                03-Aug-2020 02:39    -
 * short_range/                       03-Aug-2020 00:59    -
 * short_range_hawaii/                02-Aug-2020 15:37    -
 * short_range_hawaii_no_da/          02-Aug-2020 15:37    -
 * short_range_puertorico/            02-Aug-2020 21:05    -
 * short_range_puertorico_no_da/      02-Aug-2020 21:11    -
 * usgs_timeslices/                   03-Aug-2020 20:40    -
 *
 * <p>Survey of directories in Nomads on 2023-12-08
 * Index of /pub/data/nccf/com/nwm/v3.0/nwm.20231208
 * (not all are availalbe on D-Store)
 * 
 * <p>Name                                Last modified      Size
 * Parent Directory                                         -   
 * analysis_assim/                     08-Dec-2023 15:42    -   
 * analysis_assim_alaska/              08-Dec-2023 15:24    -   
 * analysis_assim_alaska_no_da/        08-Dec-2023 15:24    -   
 * analysis_assim_coastal_atlgulf/     08-Dec-2023 16:00    -   
 * analysis_assim_coastal_hawaii/      08-Dec-2023 15:32    -   
 * analysis_assim_coastal_pacific/     08-Dec-2023 15:50    -   
 * analysis_assim_coastal_puertorico/  08-Dec-2023 15:36    -   
 * analysis_assim_hawaii/              08-Dec-2023 15:30    -   
 * analysis_assim_hawaii_no_da/        08-Dec-2023 15:30    -   
 * analysis_assim_long/                08-Dec-2023 12:44    -   
 * analysis_assim_long_no_da/          08-Dec-2023 12:46    -   
 * analysis_assim_no_da/               08-Dec-2023 15:44    -   
 * analysis_assim_puertorico/          08-Dec-2023 15:26    -   
 * analysis_assim_puertorico_no_da/    08-Dec-2023 15:27    -   
 * forcing_analysis_assim/             08-Dec-2023 15:33    -   
 * forcing_analysis_assim_alaska/      08-Dec-2023 15:21    -   
 * forcing_analysis_assim_hawaii/      08-Dec-2023 15:25    -   
 * forcing_analysis_assim_puertorico/  08-Dec-2023 15:25    -   
 * forcing_medium_range/               08-Dec-2023 10:53    -   
 * forcing_medium_range_alaska/        08-Dec-2023 10:44    -   
 * forcing_medium_range_blend/         08-Dec-2023 10:57    -   
 * forcing_medium_range_blend_alaska/  08-Dec-2023 10:46    -   
 * forcing_short_range/                08-Dec-2023 15:29    -   
 * forcing_short_range_alaska/         08-Dec-2023 15:12    -   
 * forcing_short_range_hawaii/         08-Dec-2023 14:51    -   
 * forcing_short_range_puertorico/     08-Dec-2023 08:41    -   
 * long_range_mem1/                    08-Dec-2023 14:59    -   
 * long_range_mem2/                    08-Dec-2023 15:01    -   
 * long_range_mem3/                    08-Dec-2023 14:58    -   
 * long_range_mem4/                    08-Dec-2023 15:02    -   
 * medium_range_alaska_mem1/           08-Dec-2023 11:02    -   
 * medium_range_alaska_mem2/           08-Dec-2023 10:59    -   
 * medium_range_alaska_mem3/           08-Dec-2023 11:00    -   
 * medium_range_alaska_mem4/           08-Dec-2023 11:00    -   
 * medium_range_alaska_mem5/           08-Dec-2023 10:59    -   
 * medium_range_alaska_mem6/           08-Dec-2023 11:00    -   
 * medium_range_alaska_no_da/          08-Dec-2023 11:04    -   
 * medium_range_blend/                 08-Dec-2023 12:18    -   
 * medium_range_blend_alaska/          08-Dec-2023 11:02    -   
 * medium_range_blend_coastal_atlgulf/ 08-Dec-2023 13:03    -   
 * medium_range_blend_coastal_pacific/ 08-Dec-2023 15:34    -   
 * medium_range_coastal_atlgulf_mem1/  08-Dec-2023 13:00    -   
 * medium_range_coastal_pacific_mem1/  08-Dec-2023 15:36    -   
 * medium_range_mem1/                  08-Dec-2023 12:15    -   
 * medium_range_mem2/                  08-Dec-2023 12:07    -   
 * medium_range_mem3/                  08-Dec-2023 12:03    -   
 * medium_range_mem4/                  08-Dec-2023 12:06    -   
 * medium_range_mem5/                  08-Dec-2023 12:04    -   
 * medium_range_mem6/                  08-Dec-2023 12:05    -   
 * medium_range_no_da/                 08-Dec-2023 12:28    -   
 * short_range/                        08-Dec-2023 15:42    -   
 * short_range_alaska/                 08-Dec-2023 15:34    -   
 * short_range_coastal_atlgulf/        08-Dec-2023 16:07    -   
 * short_range_coastal_hawaii/         08-Dec-2023 15:15    -   
 * short_range_coastal_pacific/        08-Dec-2023 15:57    -   
 * short_range_coastal_puertorico/     08-Dec-2023 09:26    -   
 * short_range_hawaii/                 08-Dec-2023 15:05    -   
 * short_range_hawaii_no_da/           08-Dec-2023 15:05    -   
 * short_range_puertorico/             08-Dec-2023 08:49    -   
 * short_range_puertorico_no_da/       08-Dec-2023 08:49    -   
 * usgs_timeslices/                    08-Dec-2023 16:01    -   
 */

class NwmProfiles
{
    private static final String ANALYSIS_ASSIM_NO_DA = "analysis_assim_no_da";
    private static final String HAWAII = "hawaii";
    private static final String PUERTORICO = "puertorico";
    private static final String ANALYSIS_ASSIM = "analysis_assim";
    private static final String MEDIUM_RANGE = "medium_range";
    private static final String SHORT_RANGE = "short_range";
    private static final String CHANNEL_RT = "channel_rt";
    private static final String CONUS = "conus";
    private static final String ALASKA = "alaska";
    private static final String MEDIUM_RANGE_NO_DA = "medium_range_no_da";
    private static final String ANALYSIS_ASSIM_EXTEND = "analysis_assim_extend";
    private static final String ANALYSIS_ASSIM_EXTEND_NO_DA = "analysis_assim_extend_no_da";

    static NwmProfile getProfileFromShortHand( SourceInterface shortHand )
    {
        return switch( shortHand )
        {
            case NWM_SHORT_RANGE_CHANNEL_RT_CONUS -> NwmProfiles.getShortRangeChannelRtConus();
            case NWM_SHORT_RANGE_CHANNEL_RT_HAWAII -> NwmProfiles.getShortRangeChannelRtHawaii();
            case NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_HAWAII -> NwmProfiles.getShortRangeNoDaChannelRtHawaii();
            case NWM_SHORT_RANGE_CHANNEL_RT_PUERTORICO -> NwmProfiles.getShortRangeChannelRtPuertoRico();
            case NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_PUERTORICO -> NwmProfiles.getShortRangeNoDaChannelRtPuertoRico();
            case NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS -> NwmProfiles.getMediumRangeEnsembleChannelRtConus();
            case NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS -> NwmProfiles.getMediumRangeDeterministicChannelRtConus();
            case NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS_HOURLY -> NwmProfiles.getMediumRangeEnsembleChannelRtConusHourly();
            case NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS_HOURLY -> NwmProfiles.getMediumRangeDeterministicChannelRtConusHourly();
            case NWM_MEDIUM_RANGE_NO_DA_DETERMINISTIC_CHANNEL_RT_CONUS -> NwmProfiles.getMediumRangeNoDaDeterministicChannelRtConus();
            case NWM_ANALYSIS_ASSIM_CHANNEL_RT_CONUS -> NwmProfiles.getAnalysisAssimChannelRtConus();
            case NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_CONUS -> NwmProfiles.getAnalysisAssimNoDaChannelRtConus();
            case NWM_ANALYSIS_ASSIM_EXTEND_CHANNEL_RT_CONUS -> NwmProfiles.getAnalysisAssimExtendChannelRtConus();
            case NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_CONUS -> NwmProfiles.getAnalysisAssimExtendNoDaChannelRtConus();
            case NWM_ANALYSIS_ASSIM_CHANNEL_RT_HAWAII -> NwmProfiles.getAnalysisAssimChannelRtHawaii();
            case NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_HAWAII -> NwmProfiles.getAnalysisAssimNoDaChannelRtHawaii();
            case NWM_ANALYSIS_ASSIM_CHANNEL_RT_PUERTORICO -> NwmProfiles.getAnalysisAssimChannelRtPuertoRico();
            case NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_PUERTORICO -> NwmProfiles.getAnalysisAssimNoDaChannelRtPuertoRico();
            case NWM_LONG_RANGE_CHANNEL_RT_CONUS -> NwmProfiles.getLongRangeChannelRtConus();
            
            // Alaska
            case NWM_SHORT_RANGE_CHANNEL_RT_CONUS_ALASKA -> NwmProfiles.getShortRangeChannelRtAlaska();
            case NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_ALASKA -> NwmProfiles.getMediumRangeEnsembleChannelRtAlaska();
            case NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_ALASKA -> NwmProfiles.getMediumRangeDeterministicChannelRtAlaska();
            case NWM_MEDIUM_RANGE_NO_DA_DETERMINISTIC_CHANNEL_RT_ALASKA -> NwmProfiles.getMediumRangeNoDaDeterministicChannelRtAlaska();
            case NWM_ANALYSIS_ASSIM_CHANNEL_RT_ALASKA -> NwmProfiles.getAnalysisAssimChannelRtAlaska();
            case NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_ALASKA -> NwmProfiles.getAnalysisAssimNoDaChannelRtAlaska();
            case NWM_ANALYSIS_ASSIM_EXTEND_CHANNEL_RT_ALASKA -> NwmProfiles.getAnalysisAssimExtendChannelRtAlaska();
            case NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_ALASKA -> NwmProfiles.getAnalysisAssimExtendNoDaChannelRtAlaska();
            
            default -> throw new UnsupportedOperationException( "No NwmProfile known to WRES for "
                                                                + shortHand );
        };
    }

    private static NwmProfile getShortRangeChannelRtConus()
    {
        return new NwmProfile( 18,
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
    }


    private static NwmProfile getMediumRangeEnsembleChannelRtConus()
    {
        return new NwmProfile( 68,
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
    }

    private static NwmProfile getMediumRangeDeterministicChannelRtConus()
    {
        return new NwmProfile( 80,
                               1,
                               Duration.ofHours( 3 ),
                               true,
                               MEDIUM_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               MEDIUM_RANGE,
                               CONUS,
                               Duration.ofHours( 6 ),
                               true, // For 2.0 and higher, the path has an ensemble-like structure: #110992
                               Duration.ZERO );
    }

    private static NwmProfile getMediumRangeEnsembleChannelRtConusHourly()
    {
        return new NwmProfile( 68 * 3,
                               7,
                               Duration.ofHours( 1 ),
                               true,
                               MEDIUM_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               MEDIUM_RANGE,
                               CONUS,
                               Duration.ofHours( 6 ),
                               true,
                               Duration.ZERO );
    }

    private static NwmProfile getMediumRangeDeterministicChannelRtConusHourly()
    {
        return new NwmProfile( 80 * 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               MEDIUM_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               MEDIUM_RANGE,
                               CONUS,
                               Duration.ofHours( 6 ),
                               true, // For 2.0 and higher, the path has an ensemble-like structure: #110992
                               Duration.ZERO );
    }

    private static NwmProfile getMediumRangeNoDaDeterministicChannelRtConus()
    {
        return new NwmProfile( 80,
                               1,
                               Duration.ofHours( 3 ),
                               true,
                               MEDIUM_RANGE_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               MEDIUM_RANGE_NO_DA,
                               CONUS,
                               Duration.ofHours( 6 ),
                               false, // Available for 2.1 and above, no ensemble-like structure: #110992
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimChannelRtConus()
    {
        return new NwmProfile( 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               ANALYSIS_ASSIM,
                               CONUS,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getLongRangeChannelRtConus()
    {
        return new NwmProfile( 120,
                               4,
                               Duration.ofHours( 6 ),
                               true,
                               "long_range",
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "long_range",
                               CONUS,
                               Duration.ofHours( 6 ),
                               true,
                               Duration.ZERO );
    }


    private static NwmProfile getShortRangeChannelRtPuertoRico()
    {
        return new NwmProfile( 48,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               SHORT_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "short_range_puertorico",
                               PUERTORICO,
                               Duration.ofHours( 12 ),
                               false,
                               Duration.ofHours( 6 ) );
    }


    private static NwmProfile getShortRangeNoDaChannelRtPuertoRico()
    {
        return new NwmProfile( 48,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               "short_range_no_da",
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "short_range_puertorico_no_da",
                               PUERTORICO,
                               Duration.ofHours( 12 ),
                               false,
                               Duration.ofHours( 6 ) );
    }


    private static NwmProfile getShortRangeChannelRtHawaii()
    {
        return new NwmProfile( 192,
                               1,
                               Duration.ofMinutes( 15 ),
                               true,
                               SHORT_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "short_range_hawaii",
                               HAWAII,
                               Duration.ofHours( 12 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getShortRangeNoDaChannelRtHawaii()
    {
        return new NwmProfile( 192,
                               1,
                               Duration.ofMinutes( 15 ),
                               true,
                               "short_range_no_da",
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "short_range_hawaii_no_da",
                               HAWAII,
                               Duration.ofHours( 12 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimChannelRtHawaii()
    {
        return new NwmProfile( 12,
                               1,
                               Duration.ofMinutes( 15 ),
                               true,
                               ANALYSIS_ASSIM,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_hawaii",
                               HAWAII,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimNoDaChannelRtHawaii()
    {
        return new NwmProfile( 12,
                               1,
                               Duration.ofMinutes( 15 ),
                               true,
                               ANALYSIS_ASSIM_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_hawaii_no_da",
                               HAWAII,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimChannelRtPuertoRico()
    {
        return new NwmProfile( 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_puertorico",
                               PUERTORICO,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimNoDaChannelRtPuertoRico()
    {
        return new NwmProfile( 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_puertorico_no_da",
                               PUERTORICO,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimNoDaChannelRtConus()
    {
        return new NwmProfile( 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               ANALYSIS_ASSIM_NO_DA,
                               CONUS,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimExtendChannelRtConus()
    {
        return new NwmProfile( 28,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM_EXTEND,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               ANALYSIS_ASSIM_EXTEND,
                               CONUS,
                               Duration.ofDays( 1 ),
                               false,
                               Duration.ofHours( 16 ) );
    }

    private static NwmProfile getAnalysisAssimExtendNoDaChannelRtConus()
    {
        return new NwmProfile( 28,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM_EXTEND_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               ANALYSIS_ASSIM_EXTEND_NO_DA,
                               CONUS,
                               Duration.ofDays( 1 ),
                               false,
                               Duration.ofHours( 16 ) );
    }
    

    private static NwmProfile getShortRangeChannelRtAlaska()
    {
        return new NwmProfile( 45,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               SHORT_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "short_range_alaska",
                               ALASKA,
                               Duration.ofHours( 3 ), // NWM SRF for Alaska is produced every 3 hours.
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getMediumRangeEnsembleChannelRtAlaska()
    {
        return new NwmProfile( 68 * 3,
                               6,
                               Duration.ofHours( 1 ),
                               true,
                               MEDIUM_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "medium_range_alaska",
                               ALASKA,
                               Duration.ofHours( 6 ),
                               true,
                               Duration.ZERO );
    }
    
    private static NwmProfile getMediumRangeDeterministicChannelRtAlaska()
    {
        return new NwmProfile( 80 * 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               MEDIUM_RANGE,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "medium_range_alaska",
                               ALASKA,
                               Duration.ofHours( 6 ),
                               true, // For 2.0 and higher, the path has an ensemble-like structure: #110992
                               Duration.ZERO );
    }
    
    private static NwmProfile getMediumRangeNoDaDeterministicChannelRtAlaska()
    {
        return new NwmProfile( 80,
                               1,
                               Duration.ofHours( 3 ),
                               true,
                               MEDIUM_RANGE_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.F,
                               "medium_range_alaska_no_da",
                               ALASKA,
                               Duration.ofHours( 6 ),
                               false, // Available for 2.1 and above, no ensemble-like structure: #110992
                               Duration.ZERO );
    }
    
    private static NwmProfile getAnalysisAssimChannelRtAlaska()
    {
        return new NwmProfile( 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_alaska",
                               ALASKA,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }
    
    private static NwmProfile getAnalysisAssimNoDaChannelRtAlaska()
    {
        return new NwmProfile( 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_alaska_no_da",
                               ALASKA,
                               Duration.ofHours( 1 ),
                               false,
                               Duration.ZERO );
    }

    private static NwmProfile getAnalysisAssimExtendChannelRtAlaska()
    {
        return new NwmProfile( 28,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM_EXTEND,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_extend_alaska",
                               ALASKA,
                               Duration.ofDays( 1 ),
                               false,
                               Duration.ofHours( 20 ) );
    }

    private static NwmProfile getAnalysisAssimExtendNoDaChannelRtAlaska()
    {
        return new NwmProfile( 28,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               ANALYSIS_ASSIM_EXTEND_NO_DA,
                               CHANNEL_RT,
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_extend_alaska_no_da",
                               ALASKA,
                               Duration.ofDays( 1 ),
                               false,
                               Duration.ofHours( 20 ) );
    }
    
    private NwmProfiles()
    {
        // Static utility class, no construction allowed.
    }
}
