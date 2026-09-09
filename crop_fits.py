"""Crop WSClean FITS images while preserving the useful celestial WCS."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.nddata import Cutout2D
from astropy.wcs import WCS


_COPY_HEADER_KEYS = (
    "BUNIT",
    "BMAJ",
    "BMIN",
    "BPA",
    "DATE-OBS",
    "MJD-OBS",
    "OBJECT",
    "TELESCOP",
    "RESTFRQ",
    "SPECSYS",
)


def _as_2d(data: np.ndarray) -> np.ndarray:
    data = np.asarray(data)
    while data.ndim > 2:
        data = data[0]
    if data.ndim != 2:
        raise ValueError(f"Expected a 2-D image after squeezing, got {data.shape}")
    return data


def crop_fits(path: Path, ra_deg: float, dec_deg: float, size_arcmin: float) -> None:
    """Crop one image in place around the proper-motion-corrected position."""

    path = Path(path)
    with fits.open(path, memmap=False) as hdul:
        original_header = hdul[0].header.copy()
        data = _as_2d(hdul[0].data)
        celestial_wcs = WCS(original_header).celestial

    position = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame="icrs")
    cutout = Cutout2D(
        data,
        position=position,
        size=(size_arcmin * u.arcmin, size_arcmin * u.arcmin),
        wcs=celestial_wcs,
        mode="partial",
        fill_value=np.nan,
        copy=True,
    )

    header = cutout.wcs.to_header()
    for key in _COPY_HEADER_KEYS:
        if key in original_header:
            header[key] = original_header[key]
    header["HISTORY"] = f"Cropped to {size_arcmin:g} arcmin by Ozstar ASKAP pipeline"

    temporary = path.parent / f".{path.name}.crop.tmp"
    fits.PrimaryHDU(data=np.asarray(cutout.data), header=header).writeto(
        temporary, overwrite=True
    )
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fits", type=Path)
    parser.add_argument("ra", type=float)
    parser.add_argument("dec", type=float)
    parser.add_argument("--size-arcmin", type=float, default=10.0)
    args = parser.parse_args()
    crop_fits(args.fits, args.ra, args.dec, args.size_arcmin)


if __name__ == "__main__":
    main()
