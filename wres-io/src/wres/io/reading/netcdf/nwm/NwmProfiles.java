package wres.io.reading.netcdf.nwm;

import java.time.Duration;

import wres.config.generated.InterfaceShortHand;

/**
 * Static helper class to generate NWM profiles for NWM versions.
 *
 * Maps short-hand declarations to the ten parameters required to describe an
 * NWM dataset sufficient for successful retrieval.
 *
 * Survey of the directories on 2019-10-23:
 *
 * Index of /pub/data/nccf/com/nwm/prod/nwm.20191023
 *
 * Name                           Last modified      Size  Parent Directory                                    -
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
 * Survey of directories on para 2020-08-03
 * Index of /pub/data/nccf/com/nwm/para/nwm.20200802
 *
 * Name                               Last modified      Size  Parent Directory                                        -
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
 */

class NwmProfiles
{
    private NwmProfiles()
    {
        // Static utility class, no construction allowed.
    }

    static NwmProfile getProfileFromShortHand( InterfaceShortHand shortHand )
    {
        if ( shortHand.equals( InterfaceShortHand.NWM_SHORT_RANGE_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getShortRangeChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getMediumRangeEnsembleChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getMediumRangeDeterministicChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS_HOURLY ) )
        {
            return NwmProfiles.getMediumRangeEnsembleChannelRtConusHourly();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS_HOURLY ) )
        {
            return NwmProfiles.getMediumRangeDeterministicChannelRtConusHourly();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_MEDIUM_RANGE_NO_DA_DETERMINISTIC_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getMediumRangeNoDaDeterministicChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getAnalysisAssimChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getAnalysisAssimNoDaChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_EXTEND_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getAnalysisAssimExtendChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getAnalysisAssimExtendNoDaChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_CHANNEL_RT_HAWAII ) )
        {
            return NwmProfiles.getAnalysisAssimChannelRtHawaii();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_HAWAII ) )
        {
            return NwmProfiles.getAnalysisAssimNoDaChannelRtHawaii();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_CHANNEL_RT_PUERTORICO ) )
        {
            return NwmProfiles.getAnalysisAssimChannelRtPuertoRico();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_PUERTORICO ) )
        {
            return NwmProfiles.getAnalysisAssimNoDaChannelRtPuertoRico();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_LONG_RANGE_CHANNEL_RT_CONUS ) )
        {
            return NwmProfiles.getLongRangeChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_SHORT_RANGE_CHANNEL_RT_HAWAII ) )
        {
            return NwmProfiles.getShortRangeChannelRtHawaii();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_HAWAII ) )
        {
            return NwmProfiles.getShortRangeNoDaChannelRtHawaii();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_SHORT_RANGE_CHANNEL_RT_PUERTORICO ) )
        {
            return NwmProfiles.getShortRangeChannelRtPuertoRico();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_PUERTORICO ) )
        {
            return NwmProfiles.getShortRangeNoDaChannelRtPuertoRico();
        }
        else
        {
            throw new UnsupportedOperationException( "No NwmProfile known to WRES for "
                                                     + shortHand.toString() );
        }
    }

    private static NwmProfile getShortRangeChannelRtConus()
    {
        return new NwmProfile( 18,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               "short_range",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "short_range",
                               "conus",
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
                               "medium_range",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "medium_range",
                               "conus",
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
                               "medium_range",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "medium_range",
                               "conus",
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
                               "medium_range",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "medium_range",
                               "conus",
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
                               "medium_range",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "medium_range",
                               "conus",
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
                               "medium_range_no_da",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "medium_range_no_da",
                               "conus",
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
                               "analysis_assim",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim",
                               "conus",
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
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "long_range",
                               "conus",
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
                               "short_range",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "short_range_puertorico",
                               "puertorico",
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
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "short_range_puertorico_no_da",
                               "puertorico",
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
                               "short_range",
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "short_range_hawaii",
                               "hawaii",
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
                               "channel_rt",
                               NwmProfile.TimeLabel.F,
                               "short_range_hawaii_no_da",
                               "hawaii",
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
                               "analysis_assim",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_hawaii",
                               "hawaii",
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
                               "analysis_assim_no_da",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_hawaii_no_da",
                               "hawaii",
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
                               "analysis_assim",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_puertorico",
                               "puertorico",
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
                               "analysis_assim_no_da",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_puertorico_no_da",
                               "puertorico",
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
                               "analysis_assim_no_da",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_no_da",
                               "conus",
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
                               "analysis_assim_extend",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_extend",
                               "conus",
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
                               "analysis_assim_extend_no_da",
                               "channel_rt",
                               NwmProfile.TimeLabel.TM,
                               "analysis_assim_extend_no_da",
                               "conus",
                               Duration.ofDays( 1 ),
                               false,
                               Duration.ofHours( 16 ) );
    }
}
