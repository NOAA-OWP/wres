package wres.datamodel.bootstrap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Estimates the block size for the stationary bootstrap. The citation is: Patton, A. Politis, D.N. and White, H.
 * (2008) CORRECTION TO 'Automatic Block-Length Selection for the Dependent Bootstrap' by D.N. Politis and H. White.
 * Econometric Reviews, 28(4):372-375. The original article is: Politis, D.N., and White, H. (2004) Automatic block-
 * length selection for the dependent bootstrap. Econometric Reviews, 23(1):53-70.
 *
 * <p>This code is ported from an R implementation by C. Parmeter and J. Racine, available here (last checked,
 * 2023-08-22T12:00:00Z):
 *
 * <p><a href="http://public.econ.duke.edu/~ap172/ppw.R.txt">...</a>
 *
 * @author James Brown
 */
class BlockSizeEstimator
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( BlockSizeEstimator.class );

    /** Fast Fourier Transform. */
    private static final FastFourierTransformer FAST_FOURIER_TRANSFORMER =
            new FastFourierTransformer( DftNormalization.STANDARD );

    /** Mean function. */
    private static final Mean MEAN = new Mean();

    /** Variance function. */
    private static final Variance VARIANCE = new Variance( false );

    /**
     * Estimate the block size for the stationary bootstrap from the autocorrelated sample data.
     * @param data the sample data, required
     * @return the optimal block size estimate
     * @throws NullPointerException if the data is null
     */

    static int getOptimalBlockSize( double[] data )
    {
        Objects.requireNonNull( data );

        int n = data.length;

        if ( n < 2 )
        {
            LOGGER.warn( "Cannot estimate the optimal block length from a dataset that contains fewer than 2 samples: "
                         + "{}. Proceeding with a block size of 1, but the stationary block bootstrap may not be "
                         + "appropriate for estimating the sampling uncertainties in this context.", n );
            return 1;
        }

        // Set some default values for required parameters based on Politis and White (2004, 2008)

        // The autocorrelation run length
        int kN = Math.max( 5, ( int ) Math.ceil( Math.log10( n ) ) );
        // The maximum value of the lag to use when calculating the autocorrelation function
        int mMax = ( int ) ( Math.ceil( Math.sqrt( n ) ) + kN );
        // The maximum value of the block size
        int bMax = ( int ) Math.ceil( Math.min( Math.sqrt( n ) * 3.0, n / 3.0 ) );
        // A constant used to calculate the critical/significant value of the autocorrelation
        double c = 1.959964; // qnorm(0.975)

        return BlockSizeEstimator.getBlockSizeInner( data, kN, mMax, bMax, c );
    }

    /**
     * Estimate the block size for the stationary bootstrap from the autocorrelated sample data.
     * @param data the sample data, required
     * @param kN the autocorrelation run length
     * @param mMax the maximum value of the lag to use when calculating the autocorrelation function
     * @param bMax the maximum value of the block size
     * @param c a constant used to calculate the critical/significant value of the autocorrelation
     * @return the optimal block size estimate
     * @throws NullPointerException if the data is null
     * @throws IllegalArgumentException if any supplied input is invalid
     */

    private static int getBlockSizeInner( double[] data, int kN, int mMax, int bMax, double c )
    {
        // Compute the acf
        double[] acf = BlockSizeEstimator.getAutocorrelationFunction( data, mMax );

        // Compute mhat
        int n = data.length;
        double rhoCritical = c * Math.sqrt( Math.log10( n ) / n );

        // Compute the number of insignificant runs for each rho(k), k=1,...,mMax
        List<Integer> insignificant = BlockSizeEstimator.getInsignificantRuns( acf, rhoCritical, mMax, kN );

        // If there are any values of rho(k) for which the kN proceeding values of rho(k+j), j=1,...,kN are all
        // insignificant, take the smallest rho(k) such that this holds
        int mhat;
        if ( insignificant.contains( kN ) )
        {
            mhat = insignificant.indexOf( kN ) + 1;
        }
        else
        {
            // If no runs of length Kn are insignificant, take the smallest value of rho(k) that is significant
            int[] significantLags = BlockSizeEstimator.getIndicesOfRhoLargerThanCritical( acf, rhoCritical );

            if ( significantLags.length > 0 )
            {
                if ( significantLags.length == 1 )
                {
                    mhat = significantLags[0];
                }
                else
                {
                    mhat = Arrays.stream( significantLags )
                                 .max()
                                 .getAsInt();
                }
            }
            else
            {
                mhat = 1;
            }
        }

        int m = Math.min( ( 2 * mhat ), mMax );

        // Create the autocovariance function, limited to a lag of m, but including a lag of 0 and symmetric for
        // positive and negative lags
        double variance = VARIANCE.evaluate( data );
        double[] acovf = BlockSizeEstimator.getSymmetricAutocovarianceFunction( acf, m, variance );
        int[] kk = IntStream.range( -m, m + 1 )
                            .toArray();

        double gHat = 0;
        double dsBHat = 0;

        for ( int i = 0; i < acovf.length; i++ )
        {
            double first = BlockSizeEstimator.getFlatTopLagWindow( kk[i] / ( double ) m );
            gHat += ( first * Math.abs( kk[i] ) * acovf[i] );
            dsBHat += ( first * acovf[i] );
        }

        dsBHat = 2.0 * Math.pow( dsBHat, 2 );

        double bStar = Math.pow( ( 2.0 * Math.pow( gHat, 2 ) ) / dsBHat, 1.0 / 3.0 ) * Math.pow( n, 1.0 / 3.0 );

        if ( bStar > bMax )
        {
            bStar = bMax;
        }

        if ( bStar < 1.0 || Double.isNaN( bStar ) )
        {
            LOGGER.debug( "Could not determine a valid block size estimate from the sample autocorrelations. Assuming "
                          + "a block size of 1 instead." );

            bStar = 1.0;
        }

        return ( int ) Math.rint( bStar );
    }

    /**
     * Computes the number of insignificant runs following each rho(k), k=1,...,mMax.
     * @param acf the autocorrelation function
     * @param rhoCritical the critical autocorrelation
     * @param mMax the maximum lag
     * @param kN the autocorrelation run length
     * @return the insignificant runs
     */

    private static List<Integer> getInsignificantRuns( double[] acf, double rhoCritical, int mMax, int kN )
    {
        int capacity = mMax - kN + 1;

        List<Integer> insignificant = new ArrayList<>( capacity );
        for ( int i = 0; i < capacity; i++ )
        {
            int next = BlockSizeEstimator.sumInsignificant( i, acf, rhoCritical, kN );
            insignificant.add( next );
        }

        return Collections.unmodifiableList( insignificant );
    }

    /**
     * Returns the sum of insignificant autocorrelation values for the specified inputs.
     * @param index the start index
     * @param acf the autocorrelation function
     * @param rhoCritical the critical autocorrelation
     * @param stop the stop index
     * @return the number of insignificant autocorrelation values
     */

    private static int sumInsignificant( int index, double[] acf, double rhoCritical, int stop )
    {
        int sum = 0;
        for ( int j = index; j < index + stop; j++ )
        {
            if ( Math.abs( acf[j] ) < rhoCritical )
            {
                sum += 1;
            }
        }

        return sum;
    }

    /**
     * Returns the indices within the acf that have an autocorrelation greater than the critical value.
     * @param acf the autocorrelation function
     * @param rhoCritical the critical autocorrelation
     * @return the indices whose autocorrelation is greater than critical
     */

    private static int[] getIndicesOfRhoLargerThanCritical( double[] acf, double rhoCritical )
    {
        List<Integer> indices = new ArrayList<>();

        for ( int i = 0; i < acf.length; i++ )
        {
            if ( acf[i] > rhoCritical )
            {
                indices.add( i + 1 );
            }
        }

        return indices.stream()
                      .mapToInt( Integer::intValue )
                      .toArray();
    }

    /**
     * Returns the autocorrelation function, one autocorrelation for each lag up to the maximum lag, not including
     * lag=0, which is removed.
     *
     * @param samples the samples
     * @param maxLag the maximum lag
     * @return the autocorrelation function
     */

    private static double[] getAutocorrelationFunction( double[] samples, int maxLag )
    {
        double[] padded = BlockSizeEstimator.padAndTransform( samples );

        // FFT
        Complex[] fft = FAST_FOURIER_TRANSFORMER.transform( padded, TransformType.FORWARD );

        // Multiply by complex conjugate
        for ( int i = 0; i < fft.length; i++ )
        {
            fft[i] = fft[i].multiply( fft[i].conjugate() );
        }

        // Inverse transform
        fft = FAST_FOURIER_TRANSFORMER.transform( fft, TransformType.INVERSE );

        double[] correlations = new double[maxLag];
        int stop = Math.min( maxLag, fft.length - 1 );
        for ( int i = 0; i < stop; i++ )
        {
            correlations[i] = fft[i + 1].getReal() / fft[0].getReal();
        }

        return correlations;
    }

    /**
     * Calculates the symmetric autocovariance function from the supplied inputs. There is an autocovariance for each
     * lag and corresponding negative lag up to the maximum lag, including the autocovariance at a lag of zero, which
     * is the variance.
     * @param acf the autocorrelation function
     * @param maxLag the maximum lag
     * @param variance the data variance (not sample variance, i.e. no sample correction)
     * @return the autocovariance function
     */

    private static double[] getSymmetricAutocovarianceFunction( double[] acf, int maxLag, double variance )
    {
        double[] acvf = new double[maxLag * 2 + 1];
        acvf[maxLag] = variance;
        for ( int i = 1; i < maxLag + 1; i++ )
        {
            acvf[maxLag + i] = acf[i - 1] * variance;
            acvf[maxLag - i] = acf[i - 1] * variance;
        }
        return acvf;
    }

    /**
     * Pads the data and subtracts the mean.
     *
     * @param samples the samples
     * @return the padded, transformed samples
     */
    private static double[] padAndTransform( double[] samples )
    {
        double m = MEAN.evaluate( samples );

        // Pad with 0
        int n = samples.length;
        double padding = Math.pow( 2, 32.0 - Integer.numberOfLeadingZeros( 2 * n - 1 ) );
        double[] values = new double[( int ) padding];

        // Zero mean
        for ( int i = 0; i < n; i++ )
        {
            values[i] = samples[i] - m;
        }

        return values;
    }

    /**
     * Used to construct a "flat-top" lag window for spectral estimation based on Politis, D.N. and J.P. Romano, 1995:
     * Bias-Corrected Nonparametric Spectral Estimation", Journal of Time Series Analysis, vol. 16, No. 1.
     *
     * @param s the input
     * @return the lag
     */

    private static double getFlatTopLagWindow( double s )
    {
        return ( Math.abs( s ) >= 0.0 ? 1.0 : 0.0 )
               * ( Math.abs( s ) < 0.5 ? 1.0 : 0.0 ) + 2.0
                                                       * ( 1.0 - Math.abs( s ) )
                                                       * ( Math.abs( s ) >= 0.5 ? 1.0 : 0.0 )
                                                       * ( Math.abs( s ) <= 1.0 ? 1.0 : 0.0 );
    }

    /**
     * Do not construct.
     */
    private BlockSizeEstimator()
    {
    }
}
