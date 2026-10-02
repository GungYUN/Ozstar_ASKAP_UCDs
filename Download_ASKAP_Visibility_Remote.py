import numpy as np
import math
import time
import os
import glob

import pandas as pd

from astropy.io.votable import parse
from astroquery.casda import Casda
from astroquery.utils.tap.core import TapPlus
from astroquery.utils.tap.core import Tap
from astropy.io.votable import parse, parse_single_table

import getpass

import astropy.coordinates as coord
from astropy.coordinates import SkyCoord
import astropy.units as un

# import keyring
# keyring.core.set_keyring(keyring.core.load_keyring('keyrings.cryptfile.cryptfile.CryptFileKeyring'))
# print("Keyring method: " + str(keyring.get_keyring()))

OPAL_USER = "qhua0119@uni.sydney.edu.au"        # set to opal login username
casda = Casda()
casda.login(username=OPAL_USER, store_password=True)

def convert_xml_to_pandas(xml_file_name):
    votable = parse(xml_file_name)
    table = votable.get_first_table()
    bill = table.to_table(use_names_over_ids=True)
    return bill.to_pandas()

# Set up the TAP url
tap = TapPlus(url="https://casda.csiro.au/casda_vo_tools/tap")



# Manually edit these two coordinates for a simple one-source download.
RA = 179.4962
Dec = 9.3668

# ASKAP primary-beam settings used for the frequency-dependent cross-match.
# FWHM is the full width, so the usable radial half-power cutoff is FWHM / 2.
# ObsCore em_min/em_max are vacuum wavelength bounds in metres.  The highest
# frequency in each observation is therefore c / em_min.
QUERY_LIMIT = 500
INITIAL_SEARCH_RADIUS_DEG = 1.0
REFERENCE_FWHM_DEG = 1.2
REFERENCE_FREQUENCY_GHZ = 1.4
SPEED_OF_LIGHT_METRES_PER_SECOND = 299792458.0

query = (
    F"SELECT TOP {QUERY_LIMIT} * FROM ivoa.obscore "
    F"where(dataproduct_type = 'visibility' and t_exptime > 100) "
    F"AND 1 = CONTAINS(POINT('ICRS',{RA},{Dec}), "
    F"circle('ICRS', s_ra, s_dec, {INITIAL_SEARCH_RADIUS_DEG}))"
)

job = tap.launch_job_async(query)

# job = tap.launch_job_async(F"SELECT TOP 500 * FROM ivoa.obscore where(dataproduct_type = 'visibility' and "
#                         F"t_exptime > 1000 )"
#                         F"AND 1 = CONTAINS(POINT('ICRS',{RA},{Dec}),s_region)")
r = job.get_results()

# Reaching TOP may mean that the one-degree query was silently truncated.
# Increase QUERY_LIMIT and run again rather than downloading an incomplete set.
if len(r) >= QUERY_LIMIT:
    raise RuntimeError(
        F"CASDA returned the query limit ({QUERY_LIMIT} rows); "
        "increase QUERY_LIMIT before continuing"
    )

# Here I keep only good or uncertain data
data = r[(r['quality_level'] == 'GOOD') | (r['quality_level'] == 'UNCERTAIN')]

# Invalid pointing centres cannot be used for a spherical separation.
s_ra = np.asarray(np.ma.filled(data['s_ra'], np.nan), dtype=float)
s_dec = np.asarray(np.ma.filled(data['s_dec'], np.nan), dtype=float)
valid_coordinates = np.isfinite(s_ra) & np.isfinite(s_dec)
print('Rows removed for invalid s_ra/s_dec: ',
      int(np.count_nonzero(~valid_coordinates)))
data = data[valid_coordinates]


def wavelength_column_in_metres(table, column_name):
    """Read an ObsCore wavelength column and return plain metre values."""

    if column_name not in table.colnames:
        raise RuntimeError(F"CASDA result does not contain {column_name}")

    column = table[column_name]
    column_unit = column.unit if column.unit is not None else un.m
    try:
        values = np.asarray(np.ma.filled(column, np.nan), dtype=float)
        return (values * un.Unit(column_unit)).to_value(un.m)
    except (TypeError, ValueError, un.UnitConversionError) as error:
        raise RuntimeError(
            F"CASDA column {column_name} is not a wavelength: {column_unit}"
        ) from error


# Calculate the target-to-pointing-centre distance for every returned row.
target_coord = SkyCoord(RA * un.deg, Dec * un.deg, frame='icrs')
observation_coords = SkyCoord(np.asarray(data['s_ra'], dtype=float) * un.deg,
                              np.asarray(data['s_dec'], dtype=float) * un.deg,
                              frame='icrs')
separation_deg = target_coord.separation(observation_coords).deg

# Use the highest frequency (c / em_min), because it has the narrowest beam.
# Requiring the target to fall inside that beam keeps the full frequency band
# within the half-power radius.
em_min_metres = wavelength_column_in_metres(data, 'em_min')
em_max_metres = wavelength_column_in_metres(data, 'em_max')
valid_spectral_range = (np.isfinite(em_min_metres)
                        & np.isfinite(em_max_metres)
                        & (em_min_metres > 0)
                        & (em_max_metres >= em_min_metres))

max_frequency_ghz = np.full(len(data), np.nan, dtype=float)
max_frequency_ghz[valid_spectral_range] = (
    SPEED_OF_LIGHT_METRES_PER_SECOND
    / em_min_metres[valid_spectral_range]
    / 1.0e9
)
half_power_radius_deg = np.full(len(data), np.nan, dtype=float)
half_power_radius_deg[valid_spectral_range] = (
    REFERENCE_FWHM_DEG / 2.0
    * REFERENCE_FREQUENCY_GHZ
    / max_frequency_ghz[valid_spectral_range]
)

inside_frequency_dependent_radius = (valid_spectral_range
                                     & np.isfinite(separation_deg)
                                     & (separation_deg
                                        <= half_power_radius_deg))

print('Rows after quality and coordinate filtering: ', len(data))
print('Rows removed for invalid em_min/em_max: ',
      int(np.count_nonzero(~valid_spectral_range)))
print('Rows removed outside the frequency-dependent half-power radius: ',
      int(np.count_nonzero(valid_spectral_range
                           & ~inside_frequency_dependent_radius)))

data = data[inside_frequency_dependent_radius]
print('Rows after frequency-dependent radius filtering: ', len(data))

# You have to do this step unless you have permission
# for embargoed data associated with you OPAL
# account login
public_data = Casda.filter_out_unreleased(data)

# Get the centre coords of all of the observations in the table
# Actually this might not be useful when you download Visibiltiy data, you can delete this
public_coords = SkyCoord(np.array(public_data['s_ra'])*un.deg,
                        np.array(public_data['s_dec'])*un.deg)

# Get the file names
public_files = np.array(public_data['filename'])

# Path to your local directory where you want to save the files
casda_filepath = '~/Downloads/'

# If the directory does not exist, create it
if not os.path.exists(casda_filepath):
    os.makedirs(casda_filepath)

# I like pandas better
pubdat = public_data.to_pandas()

print('Number of rows: ', len(pubdat.index))

url_list = casda.stage_data(public_data)

# Now download your files :D
filelist = casda.download_files(url_list,
                                savedir=casda_filepath)
