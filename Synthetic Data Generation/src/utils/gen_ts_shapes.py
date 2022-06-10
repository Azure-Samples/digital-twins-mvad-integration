"""
Functions to calculate the shapes of time-series using beta pdf distribution, piece-wise linear functions, and cosine functions
"""
import random
import numpy as np
import pandas as pd
from scipy.stats import beta


def gen_beta_anom(series, a=2, b=5, scale_fac=0.5, surge_or_dip="surge"):
    """Generates a curvy line, exponentially decaying, to present anomaly surge or dip,
     that of shape defined by the pdf of a beta-distribution parameterized by a & b
    Args:
        series, Pandas.series: series for part of original time-series that is an anomaly occurrence,
                                to convert to anomalous behavior
        a, b (float): parameters for beta-distribution, note b>a gives a right-skewed pdf
                      and vice-versa
        scale_fac, float: factor by which to rescale the generated beta pdf
        surge_or_dip, str: "surge" or "dip" to denote if curve is concave or convex
    Returns:
        series, Pandas.series: series with ts values with anomalous behavior
    """

    x = np.linspace(0, 1, len(series))
    pdf = scale_fac * (beta.pdf(x, a, b)) + 1
    if surge_or_dip == "surge":
        series *= pdf
    elif surge_or_dip == "dip":
        series /= pdf
    else:
        raise Exception('Use "surge" or "dip"')
    return series


def gen_cosine_trend(
    n_datapts,
    sine_mean=20,
    amplitude=5,
    sine_period=60 / 10 * 24,
    theta=0,
    noise_dist="uniform",
    noise_min=-1,
    noise_max=2,
    sigma=2,
    sinebkpt_factor=10,
    trendbkpt_factor=100,
    start_coeff=1,
    coeff_dist="uniform",
    coeff_max=0.1,
    coeff_min=-0.1,
    sigma_coeff=2,
    n_pts_bufferstart=0,
    n_pts_bufferend=0,
):
    """Generate a sinusoidal TS shape that consists of an imperfect cosine wave superposed with a piece-wise linear trend line.
        Sum of `gen_cosine_imperfect` and `gen_pw_lineaer_trend` methods
    Args:
        n_datapts, int: number of datapts in TS to generate
        sine_mean, amplitude, sine_period, theta (float): params for creating cosine wave
        sinebkpt_factor, int: factor representing the number of data-points within a small segment, for which noise is sampled and added
        noise_dist, str:"uniform" or "normal" distribution
        noise_min, noise_max, sigma (int): params to determine amplitude of added noise, and stdev if normal distribution used
        trendbkpt_factor,int: factor representing the number of data-points within each piece-wise line segment, for which the gradient coefficient is sampled,
        start_coeff, float: y-intersect to start off the trend line
        coeff_dist, str: "uniform" or "normal" distribution
        coeff_max, coeff_min (float): range of coeff values from which to sample the coeffs
        sigma, float: parameter to determine the stdev if normal distribution used for trend line coefficient sampling
        n_pts_bufferstart, n_pts_bufferend (int): number of data-points at the start and end to set to zero
    Returns:
        ans, np.array: 1D array consisting value of generated sinusoidal wave
    """

    sine_wave = gen_cosine_imperfect(
        n_datapts,
        sinebkpt_factor,
        sine_mean,
        amplitude,
        sine_period,
        theta,
        noise_dist=noise_dist,
        noise_min=noise_min,
        noise_max=noise_max,
        sigma=sigma,
    )
    trend = _gen_pw_linear_trend(
        n_datapts,
        trendbkpt_factor,
        start_coeff=start_coeff,
        coeff_dist=coeff_dist,
        coeff_max=coeff_max,
        coeff_min=coeff_min,
        sigma=sigma_coeff,
        n_pts_bufferstart=n_pts_bufferstart,
        n_pts_bufferend=n_pts_bufferend,
    )

    ans = sine_wave + trend
    return ans


def get_wave_period(freq_str, period_str="1D", theta_start_str="6H"):
    """Returns parameters for sinusoidal simulation for given ts sampling rate, freq_str
    Args:
        freq_str, str: Sampling frequency/rate of ts sensor,
        period_str, str: period of cosine wave, e.g. 1D if daily
        theta_start, str: time at which to put the the start of the -ve cosine function, i.e. the trough
    Returns:
        period_num/freq_num, float: number of datapoints for a period
        theta, float: number of datapoints to offset the start of the -ve cosine function, i.e. the trough
    """
    period_num, freq_num = (
        pd.to_timedelta(period_str).total_seconds(),
        pd.to_timedelta(freq_str).total_seconds(),
    )

    theta_val = pd.to_timedelta(theta_start_str).total_seconds()
    theta = theta_val / freq_num

    return (period_num / freq_num, theta)


def gen_cosine_imperfect(
    n_datapts,
    sinebkpt_factor=10,
    sine_mean=20,
    amplitude=5,
    sine_period=60 / 10 * 24,
    theta=0,
    noise_dist="uniform",
    noise_min=-1,
    noise_max=2,
    sigma=2,
):
    """Generate a sinusoidal TS shape that consists of a cosine wave, made imperfect with some randomly-sampled noise added per small segments across the wave
    Args:
        n_datapts, int: number of datapts in TS to generate
        sine_mean, amplitude, sine_period, theta (float): params for creating cosine wave
        sinebkpt_factor, int: factor representing the number of data-points within a small segment, for which noise is sampled and added
        noise_dist, str:"uniform" or "normal" distribution
        noise_min, noise_max, sigma (int): params to determine amplitude of added noise, and stdev if normal distribution used
    Returns:
        sinewave, np.array: 1D array consisting value of generated sinusoidal wave
    """
    time = np.arange(n_datapts)
    frequency = 1 / sine_period
    sinewave = sine_mean + amplitude * -1 * np.cos(
        2 * np.pi * frequency * (time + theta)
    )

    n_bpkts = int(n_datapts / sinebkpt_factor)
    bkps = _draw_bkps(len(sinewave), n_bpkts)
    noise1 = _gen_samples(noise_dist, len(bkps), noise_min, noise_max, sigma)

    for i, sub in enumerate(np.split(sinewave, bkps)):
        if sub.size > 0:
            sub += noise1[i]

    return sinewave


def gen_pw_concave_trend(
    n_datapts,
    trendbkpt_factor=100,
    concavity="concave",
    buffer_itval=9,
    start_coeff=1,
    coeff_dist="uniform",
    coeff_val=0.1,
    coeff_mid_factor=4,
    trend_shift_max=None,
    sigma=2,
    scale_2ndhalf_zero=False,
    date=None,
    trend_val_max=0.5,
):
    """Generate a piece-wise linear trend line, that is concave or convex, i.e. over time ramps up from zero, then has little fluctuation, then ramps down to zero.
        This is done by sampling the coefficients of each of the piece-wise line segments, which can be grouped into these 3 major sections
    Args:
        n_datapts,int: number of datapts in TS to generate,
        trendbkpt_factor,int: factor representing the number of data-points within each piece-wise line segment, for which the gradient coefficient is sampled,
        buffer_itval, int: number of such line segments to use to define the ramp-up and the ramp-down of the convex/concave curve,
        concavity, str: "concave" for bumpy line or "convex" for trough line,
        coeff_dist, str: "uniform" or "normal" distribution
        sigma, float: parameter to determine the stdev if normal distribution used
        start_coeff, float: y-intersect to start off the concave/convex trend line
        coeff_val, float: parameter representing maximum possible abs value of gradient coefficient,
        coeff_mid_factor, float: factor to temper down the gradient in the middle section
        trend_shift_max, float: value to which to scale the whole curve line so that maximum value in the trough/bump is that value
        scale_2ndhalf_zero, boolean: whether to scale 2nd half of trend line, so that last valueit ends up at zero
        date, datetime.date: date passed on, to print out in case of error msg
        trend_val_max, float: maximum trend value to which to reduce any values exceeding that
    Returns:
        y_all, np.array: array for trend line values
    """
    n_bkps = int(n_datapts / trendbkpt_factor)
    bkps = _draw_bkps(n_datapts, n_bkps)
    x_arr = np.arange(n_datapts)

    # print(f'n_datapts":{n_datapts}, n_bkps:{n_bkps}, buffer_itval:{buffer_itval}')
    # assert n_bkps-2*buffer_itval>buffer_itval # make sure there are enough in the middle section
    if n_bkps - 2 * buffer_itval < buffer_itval:
        print(
            f"For date:{date}, not enough datapoints to do pw concave trend, n_datapoints:{n_datapts}"
        )
        return np.zeros(x_arr.shape)

    if concavity == "concave":
        coeff_max, coeff_min = [coeff_val, coeff_val / coeff_mid_factor, 0], [
            0,
            -coeff_val / coeff_mid_factor,
            -coeff_val,
        ]
    elif concavity == "convex":
        coeff_max, coeff_min = [0, coeff_val / coeff_mid_factor, coeff_val], [
            -coeff_val,
            -coeff_val / coeff_mid_factor,
            0,
        ]
    else:
        raise Exception("Try concave or convex")

    coeffs = []
    seg_1 = _gen_samples(coeff_dist, buffer_itval, coeff_min[0], coeff_max[0], sigma)
    coeffs.extend(seg_1)
    coeffs.extend(
        _gen_samples(
            coeff_dist, len(bkps) - 2 * buffer_itval, coeff_min[1], coeff_max[1], sigma
        )
    )
    seg_3 = seg_1.copy()
    random.shuffle(seg_3)
    seg_3 = -1 * seg_3
    coeffs.extend(seg_3)

    n_indices = [buffer_itval, len(bkps) - 2 * (buffer_itval), buffer_itval]
    for i, (coeff_i_min, coeff_i_max) in enumerate(zip(coeff_min, coeff_max)):
        ans1 = _gen_samples(coeff_dist, n_indices[i], coeff_i_min, coeff_i_max, sigma)
        coeffs.extend(ans1)

    yintercepts = [start_coeff]
    y_all = np.zeros(x_arr.shape)
    for i, val in enumerate(bkps):
        if i == 0:
            y_all[range(0, val, 1)] = coeffs[i] * x_arr[range(0, val, 1)] + start_coeff
        else:
            yintercepts.append(
                (coeffs[i - 1] - coeffs[i]) * x_arr[bkps[i - 1]] + yintercepts[i - 1]
            )
            y_all[range(bkps[i - 1], val, 1)] = (
                coeffs[i] * x_arr[range(bkps[i - 1], val, 1)] + yintercepts[i]
            )

    if scale_2ndhalf_zero:  # Scale so that 2nd half goes to zero
        y_all[-int(len(y_all) / 2) :] = y_all[-int(len(y_all) / 2) :] - y_all[-1]

    if trend_shift_max is not None:
        if concavity == "concave":
            y_all = y_all / (max(y_all) / trend_shift_max)
        elif concavity == "convex":
            y_all = y_all / (min(y_all) / trend_shift_max)
        else:
            raise Exception("Try concave or convex")
    # Correction for too positive trend value:
    y_all[y_all > trend_val_max] = trend_val_max

    return y_all


def _gen_pw_linear_trend(
    n_datapts,
    trendbkpt_factor=100,
    start_coeff=1,
    coeff_dist="uniform",
    coeff_max=0.1,
    coeff_min=-0.1,
    sigma=2,
    n_pts_bufferstart=0,
    n_pts_bufferend=0,
):
    #     coeff_max=1.0, coeff_min=-0.1,
    """Generate a piece-wise linear trend line. This is done by sampling the coefficients of each of the piece-wise line segments.
    Args:
        n_datapts,int: number of datapts in TS to generate,
        trendbkpt_factor,int: factor representing the number of data-points within each piece-wise line segment, for which the gradient coefficient is sampled,
        start_coeff, float: y-intersect to start off the trend line
        coeff_dist, str: "uniform" or "normal" distribution
        coeff_max, coeff_min (float): range of coeff values from which to sample the coeffs
        sigma, float: parameter to determine the stdev if normal distribution used
        n_pts_bufferstart, n_pts_bufferend (int): number of data-points at the start and end to set to zero

    """
    n_bkps = int(n_datapts / trendbkpt_factor)
    bkps = _draw_bkps(n_datapts, n_bkps)
    x_arr = np.arange(n_datapts)

    coeffs = _gen_samples(coeff_dist, len(bkps), coeff_min, coeff_max, sigma)

    yintercepts = [start_coeff]
    y_all = np.zeros(x_arr.shape)
    for i, val in enumerate(bkps):
        if i == 0:
            y_all[range(0, val, 1)] = coeffs[i] * x_arr[range(0, val, 1)] + start_coeff
        else:
            yintercepts.append(
                (coeffs[i - 1] - coeffs[i]) * x_arr[bkps[i - 1]] + yintercepts[i - 1]
            )
            y_all[range(bkps[i - 1], val, 1)] = (
                coeffs[i] * x_arr[range(bkps[i - 1], val, 1)] + yintercepts[i]
            )

    # do buffer at start and end, so that no strong jumps
    y_all[:n_pts_bufferstart], y_all[-n_pts_bufferend:] = 0, 0
    return y_all


def _gen_samples(coeff_dist, n_pts, coeff_min, coeff_max, sigma):
    """Randomly sample `n_pts` from `coeff_dist` distribution, sampling from range between coeff_min and coeff_max.
    Note that for normal dist, the stdev is calculated such that 95% of the samples come from within that range"""
    if coeff_dist == "uniform":
        coeffs = np.random.uniform(coeff_min, coeff_max, n_pts)
    elif coeff_dist == "normal":
        coeffs = np.random.normal(
            0.5 * (coeff_max + coeff_min),
            abs(coeff_max - coeff_min) / (2 * sigma),
            n_pts,
        )
    else:
        raise "Distribution for coeffs not recognized, please use uniform or normal"

    return coeffs


def _draw_bkps(n_samples=100, n_bkps=3, seed=None):
    """Draw a random partition with specified number of samples and specified
    number of changes, adapted from https://github.com/deepcharles/ruptures/blob/master/src/ruptures/utils/drawbkps.py"""
    rng = np.random.default_rng(seed=seed)
    alpha = np.ones(n_bkps + 1) / (n_bkps + 1) * 2000
    bkps = np.cumsum(rng.dirichlet(alpha) * n_samples).astype(int).tolist()
    bkps[-1] = n_samples  # -1
    return bkps
