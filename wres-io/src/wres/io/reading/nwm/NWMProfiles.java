package wres.io.reading.nwm;

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
 */

class NWMProfiles
{
    private NWMProfiles()
    {
        // Static utility class, no construction allowed.
    }

    static NWMProfile getProfileFromShortHand( InterfaceShortHand shortHand )
    {
        if ( shortHand.equals( InterfaceShortHand.NWM_SHORT_RANGE_CHANNEL_RT_CONUS ) )
        {
            return NWMProfiles.getShortRangeChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS ) )
        {
            return NWMProfiles.getMediumRangeEnsembleChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS ) )
        {
            return NWMProfiles.getMediumRangeDeterministicChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_ANALYSIS_ASSIM_CHANNEL_RT_CONUS ) )
        {
            return NWMProfiles.getAnalysisAssimChannelRtConus();
        }
        else if ( shortHand.equals( InterfaceShortHand.NWM_LONG_RANGE_CHANNEL_RT_CONUS ) )
        {
            return NWMProfiles.getLongRangeChannelRtConus();
        }
        else
        {
            throw new UnsupportedOperationException( "No NWMProfile known to WRES for "
                                                     + shortHand.toString() );
        }
    }

    private static NWMProfile getShortRangeChannelRtConus()
    {
        return new NWMProfile( 18,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               "short_range",
                               "channel_rt",
                               NWMProfile.TimeLabel.f,
                               "short_range",
                               "conus",
                               Duration.ofHours( 1 ),
                               false );
    }


    private static NWMProfile getMediumRangeEnsembleChannelRtConus()
    {
        return new NWMProfile( 68,
                               7,
                               Duration.ofHours( 3 ),
                               true,
                               "medium_range",
                               "channel_rt",
                               NWMProfile.TimeLabel.f,
                               "medium_range",
                               "conus",
                               Duration.ofHours( 6 ),
                               true );
    }

    private static NWMProfile getMediumRangeDeterministicChannelRtConus()
    {
        return new NWMProfile( 80,
                               1,
                               Duration.ofHours( 3 ),
                               true,
                               "medium_range",
                               "channel_rt",
                               NWMProfile.TimeLabel.f,
                               "medium_range",
                               "conus",
                               Duration.ofHours( 6 ),
                               true );
    }

    private static NWMProfile getAnalysisAssimChannelRtConus()
    {
        return new NWMProfile( 3,
                               1,
                               Duration.ofHours( 1 ),
                               true,
                               "analysis_assim",
                               "channel_rt",
                               NWMProfile.TimeLabel.tm,
                               "analysis_assim",
                               "conus",
                               Duration.ofHours( 1 ),
                               false );
    }

    private static NWMProfile getLongRangeChannelRtConus()
    {
        return new NWMProfile( 120,
                               4,
                               Duration.ofHours( 6 ),
                               true,
                               "long_range",
                               "channel_rt",
                               NWMProfile.TimeLabel.f,
                               "long_range",
                               "conus",
                               Duration.ofHours( 6 ),
                               true );
    }

}
