# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#   "weather-skills-core>=0.4.0",
#   "pandas",
#   "gcsfs",
#   "tahmo",
# ]
#
# [tool.uv.sources]
# tahmo = { git = "https://github.com/rhiza-research/tahmo-api", rev = "8ed3adc22b5b7c53d08753e45676e9d4a0a52ab8" }
# ///
"""Fetch TAHMO weather-station observations as a weather-skills station Zarr."""
# Uses the TAHMO Python SDK. Credentials: TAHMO_API_USERNAME and TAHMO_API_PASSWORD.
#
# List stations in a region from the deployment API (no ``-o``)::
#
#     tahmo-fetch --list-stations --bbox N/W/S/E
#
# Then fetch chosen station IDs (or every TA station in a bbox): 5-minute records of
# the ``-v`` variables, or complete-day values with ``--daily``.
#
# With ``--gridded``, reads Sheerwater's historical all-Africa 0.25° TAHMO precip grid
# from private GCS instead (Google credentials, no TAHMO login). With ``--gold-stations``,
# reads the per-station daily precip those cells are built from, from the same bucket.

import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from weather_skills_core import DataError, UsageError, weather_skill
from weather_skills_core.cf import udunits_error
from weather_skills_core.standard_utils import bbox_subset, is_transient, require_env
from weather_skills_core.units import stamp_data_interval

# Auto-populated by the version-bump CI workflow. Do not edit manually.
_SKILL_VERSION = "0.1.4"

DEFAULT_WORKERS = 8
_TAHMO_PREFIX = "TA"

VAR_MAP = {
    "pr": "precip",
    "te": "temperature",
    "rh": "humidity",
    "ap": "pressure",
}
DAILY_AGG = {
    "precip": "sum",
    "temperature": "mean",
    "humidity": "mean",
    "pressure": "mean",
    "tmax": "max",
    "tmin": "min",
}
# Daily extremes taken from the 5-minute temperature records, under the same
# complete-day rule as the other variables.
DERIVED = {"tmax": "temperature", "tmin": "temperature"}
EXTREMES = {
    "tmax": ("time: maximum", "daily maximum air temperature"),
    "tmin": ("time: minimum", "daily minimum air temperature"),
}
# TAHMO records every variable at a 5-minute interval, stamped at the interval
# end. A daily value is kept only when all 288 stamps of that day passed the
# quality filter; any gap makes the day NaN. This applies to the means as well
# as the precip sum: a gap removes part of the diurnal cycle (e.g. the warm
# afternoon), so a partial-day mean is a biased estimate, not an observation.
SAMPLES_PER_DAY = 288
# A gauge's day rain total (mm) below this counts as dry when looking for a blocked gauge.
DRY_MM = 0.01
# (standard_name, units). Always CF/pint strings — TAHMO getVariables() uses
# "-" for dimensionless (relative humidity) and that fails pint.quantify().
CF_META = {
    "precip": ("lwe_precipitation_rate", "mm day-1"),
    "temperature": ("air_temperature", "degree_Celsius"),
    "humidity": ("relative_humidity", "1"),
    "pressure": ("air_pressure", "kPa"),
    "tmax": ("air_temperature", "degree_Celsius"),
    "tmin": ("air_temperature", "degree_Celsius"),
}
# TAHMO API leftovers if CF_META has no override.
_TAHMO_UNITS = {
    "-": "1",
    "degrees Celsius": "degree_Celsius",
    "degree Celsius": "degree_Celsius",
}
# Metadata for --variable codes that getVariables() lacks or labels "-".
_RAW_META = {
    "lb": {"units": "percent"},
    "lv": {"description": "Logger battery voltage", "units": "mV"},
}


# Historical daily precip for all of Africa: complete QC'd TAHMO station days
# averaged within each 0.25° cell, with precip_count stations per cell-day and
# precip_flag, a bit mask of the quality flags of those stations.
# Dims (time, lat, lon); lat/lon list only the rows and columns that have a
# station, so the skill puts them back on the regular 0.25° grid.
_GRIDDED_STORE = "sheerwater-datalake/hindcasts/tahmo_africa_precip.zarr"
_GRID_STEP = 0.25
# The station-days the gridded store averages: dims (time, station_id), with
# precip_flag per station-day and station latitude, longitude, elevation, country, name.
_GOLD_STORE = "sheerwater-datalake/hindcasts/tahmo_africa_stations.zarr"
_GRIDDED_AUTH_MSG = (
    "cannot read gs://sheerwater-datalake/hindcasts (private GCS). Set "
    "GCLOUD_SERVICE_CREDENTIALS to the full service-account key JSON "
    "(no file needed), GOOGLE_APPLICATION_CREDENTIALS to a service-account "
    "JSON file path, or run `gcloud auth application-default login`."
)


def _gcs():
    """Authenticated GCS filesystem: GCLOUD_SERVICE_CREDENTIALS, then
    GOOGLE_APPLICATION_CREDENTIALS / Application Default Credentials.
    """
    import gcsfs

    creds_json = os.environ.get("GCLOUD_SERVICE_CREDENTIALS")
    if creds_json:
        try:
            info = json.loads(creds_json)
        except json.JSONDecodeError as exc:
            raise DataError(f"GCLOUD_SERVICE_CREDENTIALS is not valid JSON ({exc}).") from None
        try:
            return gcsfs.GCSFileSystem(token=info)
        except Exception as exc:  # noqa: BLE001 — surface auth failures cleanly
            raise DataError(f"{_GRIDDED_AUTH_MSG} ({exc})") from None
    try:
        return gcsfs.GCSFileSystem()
    except Exception as exc:  # noqa: BLE001 — surface auth failures cleanly
        raise DataError(f"{_GRIDDED_AUTH_MSG} ({exc})") from None


def _open_gridded():
    """Open the gridded store lazily (no dask), lat/lon renamed to the standard names."""
    import xarray as xr

    try:
        ds = xr.open_zarr(_gcs().get_mapper(_GRIDDED_STORE), chunks=None)
    except Exception as exc:  # noqa: BLE001
        raise DataError(f"{_GRIDDED_AUTH_MSG} Opening it failed: {exc}") from None
    ds = ds[["precip", "precip_count", "precip_flag"]].set_coords(["precip_count", "precip_flag"])
    return ds.rename({"lat": "latitude", "lon": "longitude"})


def _open_gold():
    """Open the gold station store lazily (no dask), precip_flag as a coordinate."""
    import xarray as xr

    try:
        ds = xr.open_zarr(_gcs().get_mapper(_GOLD_STORE), chunks=None)
    except Exception as exc:  # noqa: BLE001
        raise DataError(f"{_GRIDDED_AUTH_MSG} Opening it failed: {exc}") from None
    return ds[["precip", "precip_flag"]].set_coords("precip_flag")


def _fetch_gold(start_time, end_time, bbox, station_ids):
    """Historical gold TAHMO station precip for ``start_time``..``end_time``."""
    import numpy as np
    import pandas as pd

    ds = _open_gold()

    if station_ids:
        wanted = list(dict.fromkeys(s.strip().upper() for s in station_ids))
        missing = sorted(set(wanted) - set(ds["station_id"].values))
        if missing:
            raise UsageError("no gold TAHMO record for station id(s): " + ", ".join(missing))
        ds = ds.sel(station_id=wanted)
    elif bbox is not None:
        north, west, south, east = bbox
        lat, lon = ds["latitude"].values, ds["longitude"].values
        lon_in = (lon >= west) & (lon <= east) if west <= east else (lon >= west) | (lon <= east)
        keep = (lat >= south) & (lat <= north) & lon_in
        if not keep.any():
            raise DataError(
                f"no gold TAHMO station in --bbox {north:g}/{west:g}/{south:g}/{east:g}."
            )
        ds = ds.isel(station_id=np.nonzero(keep)[0])

    times = pd.DatetimeIndex(ds["time"].values)
    keep = (times >= pd.Timestamp(start_time)) & (times <= pd.Timestamp(end_time))
    if not keep.any():
        raise DataError(
            f"no gold TAHMO day in {start_time}..{end_time} (store runs "
            f"{times.min().date()} to {times.max().date()})."
        )
    ds = ds.isel(time=np.nonzero(keep)[0]).load()
    # Never fill: a station-day is NaN unless all 288 records passed QC.
    reported = ds["precip"].notnull()
    if not bool(reported.any()):
        raise DataError(
            "no gold TAHMO station has a complete day in this selection and date range."
        )
    ds = ds.isel(station_id=np.nonzero(reported.any("time").values)[0])
    n_days = ds.sizes["time"]
    complete = int(ds["precip"].notnull().all("time").sum())
    print(
        f"precip: {complete} of {ds.sizes['station_id']} stations complete "
        f"(all {n_days} days {start_time} → {end_time}); "
        f"{int((ds['precip_flag'] > 0).sum())} station-days flagged (see precip_flag)",
        file=sys.stderr,
    )

    ds["precip"].attrs = {
        "standard_name": "lwe_precipitation_rate",
        "units": "mm day-1",
        "long_name": "daily precipitation, TAHMO station complete days",
        "coordinates": "latitude longitude",
    }
    ds["latitude"].attrs.update(standard_name="latitude", units="degrees_north", axis="Y")
    ds["longitude"].attrs.update(standard_name="longitude", units="degrees_east", axis="X")
    ds["time"].attrs.update(
        standard_name="time", axis="T", comment="day D covers D 00:00 to D+1 00:00 UTC"
    )
    ds["station_id"].attrs.update(cf_role="timeseries_id", long_name="TAHMO station identifier")
    ds["country"].attrs.update(long_name="ISO 3166-1 alpha-2 country code")
    ds["name"].attrs.update(long_name="station name")
    ds.attrs = {
        "featureType": "timeSeries",
        "Conventions": "CF-1.13",
        "weather_skills_source": f"sheerwater:{_GOLD_STORE.split('/', 1)[1]}",
    }
    return stamp_data_interval(ds, period="1 day")


def _fetch_gridded(start_time, end_time, bbox):
    """Historical gridded TAHMO precip for ``start_time``..``end_time`` on a regular 0.25° grid."""
    import numpy as np
    import pandas as pd

    ds = _open_gridded()
    times = pd.DatetimeIndex(ds["time"].values)
    keep = (times >= pd.Timestamp(start_time)) & (times <= pd.Timestamp(end_time))
    if not keep.any():
        raise DataError(
            f"no gridded TAHMO day in {start_time}..{end_time} (store runs "
            f"{times.min().date()} to {times.max().date()})."
        )
    ds = ds.isel(time=np.nonzero(keep)[0])

    # The store keeps only the rows and columns with a station; rebuild the regular
    # 0.25° grid over its extent. Cells with no station stay NaN (nothing filled).
    lat, lon = ds["latitude"].values, ds["longitude"].values
    ds = ds.reindex(
        latitude=np.arange(lat.min(), lat.max() + _GRID_STEP / 2, _GRID_STEP),
        longitude=np.arange(lon.min(), lon.max() + _GRID_STEP / 2, _GRID_STEP),
    )
    if bbox is not None:
        ds = bbox_subset(ds, bbox, lat_dim="latitude", lon_dim="longitude")
    ds = ds.load()
    # reindex made the new cells NaN and the ints float; a cell with no station has
    # 0 stations and no flags, and the flags must be ints again for bit tests.
    ds["precip_count"] = ds["precip_count"].fillna(0).astype("int16")
    ds["precip_flag"] = ds["precip_flag"].fillna(0).astype("int8")
    # Never fill: a day is a cell's mean of the stations that reported it.
    reported = ds["precip"].notnull()
    if not bool(reported.any()):
        raise DataError(
            "no gridded TAHMO cell has data in this --bbox and date range (cells with data "
            f"span lat {lat.min():g}..{lat.max():g}, lon {lon.min():g}..{lon.max():g})."
        )
    n_days = ds.sizes["time"]
    cells = reported.any("time")
    complete = reported.all("time")
    print(
        f"precip: {int(cells.sum())} cells with data, {int(complete.sum())} complete "
        f"(all {n_days} days {start_time} → {end_time}); "
        f"{int((ds['precip_flag'] > 0).sum())} cell-days flagged (see precip_flag)",
        file=sys.stderr,
    )

    ds["precip"].attrs = {
        "standard_name": "lwe_precipitation_rate",
        "units": "mm day-1",
        "long_name": "daily precipitation, mean of the TAHMO stations with a complete day in each 0.25° cell",
        "cell_methods": "area: mean",
    }
    ds["precip_count"].attrs = {
        "units": "1",
        "long_name": "number of TAHMO stations averaged into the cell-day",
    }
    ds["latitude"].attrs.update(standard_name="latitude", units="degrees_north", axis="Y")
    ds["longitude"].attrs.update(standard_name="longitude", units="degrees_east", axis="X")
    ds["time"].attrs.update(
        standard_name="time", axis="T", comment="day D is the UTC calendar date D"
    )
    ds.attrs = {
        "Conventions": "CF-1.13",
        "weather_skills_source": f"sheerwater:{_GRIDDED_STORE.split('/', 1)[1]}",
    }
    return stamp_data_interval(ds.transpose("time", "latitude", "longitude"), period="1 day")


def _pint_units(raw) -> str | None:
    """Map a TAHMO/API units string to a pint-parseable CF spelling, or None."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    return _TAHMO_UNITS.get(text, text)


def _coord_str(row, *keys) -> str:
    for key in keys:
        if key not in row.index:
            continue
        val = row[key]
        if val is None or val != val:
            continue
        text = str(val).strip()
        if text:
            return text
    return ""


def _station_name(row) -> str:
    return _coord_str(row, "location_name", "name")


def _ta_stations(stations):
    out = stations.dropna(subset=["location_latitude", "location_longitude"])
    return out[out["code"].astype(str).str.startswith(_TAHMO_PREFIX)].copy()


def _filter_bbox(stations, bbox):
    north, west, south, east = bbox
    lat = stations["location_latitude"]
    lon = stations["location_longitude"]
    lat_in = (lat >= south) & (lat <= north)
    if west <= east:
        lon_in = (lon >= west) & (lon <= east)
    else:
        lon_in = (lon >= west) | (lon <= east)
    return stations.loc[lat_in & lon_in].copy()


def _print_station_list(stations) -> None:
    ordered = stations.sort_values("code")
    writer = csv.writer(sys.stdout, dialect="excel-tab", lineterminator="\n")
    writer.writerow(["station_id", "name", "latitude", "longitude", "country"])
    for _, row in ordered.iterrows():
        writer.writerow(
            [
                str(row["code"]),
                _station_name(row),
                f"{float(row['location_latitude']):.5f}",
                f"{float(row['location_longitude']):.5f}",
                _coord_str(row, "location_countrycode"),
            ]
        )
    print(f"{len(ordered)} stations", file=sys.stderr)


def _select_stations(stations, station_ids, bbox):
    """Return deployment rows for explicit ``--station`` IDs, else TA stations in ``--bbox``."""
    if station_ids:
        wanted = list(dict.fromkeys(s.strip().upper() for s in station_ids))
        by_code = stations.set_index(stations["code"].astype(str).str.upper())
        missing = [s for s in wanted if s not in by_code.index]
        if missing:
            raise UsageError("unknown TAHMO station id(s): " + ", ".join(missing))
        return by_code.loc[wanted].reset_index(drop=True)

    if bbox is None:
        raise UsageError("pass --station ID (repeatable) and/or --bbox N/W/S/E.")

    selected = _filter_bbox(_ta_stations(stations), bbox)
    if selected.empty:
        north, west, south, east = bbox
        raise DataError(f"no TAHMO stations in --bbox {north:g}/{west:g}/{south:g}/{east:g}.")
    return selected


def _fetch_raw(api, station_id: str, start: str, end: str, variables=None):
    """Call getRawData once, retrying a single transient error after a short backoff.

    The API filters by variable only when ``variables`` holds a single code.
    """
    try:
        return api.getRawData(
            station=station_id,
            startDate=start,
            endDate=end,
            variables=variables,
            dataset="controlled",
        )
    except Exception as exc:
        if not is_transient(exc):
            raise
        print(f"{station_id}: transient error ({exc}); retrying once", file=sys.stderr)
        time.sleep(2.0)
        return api.getRawData(
            station=station_id,
            startDate=start,
            endDate=end,
            variables=variables,
            dataset="controlled",
        )


def _merge_gauges(raw, keys, offset):
    """Return one value per ``keys`` from redundant sensors: the mean of their QC'd readings.

    A rain gauge whose day total is under DRY_MM while another rain gauge's day
    total is DRY_MM or more is taken as blocked and left out for that whole day.
    Days start ``offset`` after 00:00 UTC. Of the remaining readings, only those
    with the best QC flag at each stamp count.
    """
    import pandas as pd

    # Stamps mark interval ends, so the HH:00 stamp closes the previous day.
    raw = raw.assign(day=(raw["time"] - pd.Timedelta(minutes=5) - offset).dt.floor("D"))
    raw = raw.reset_index(drop=True)
    gauge_total = raw.groupby(["day", "variable", "sensor"])["value"].transform("sum")
    wettest = gauge_total.groupby([raw["day"], raw["variable"]]).transform("max")
    blocked = (raw["variable"] == "pr") & (gauge_total < DRY_MM) & (wettest >= DRY_MM)
    raw = raw[~blocked]
    raw = raw[raw["quality"] == raw.groupby(keys)["quality"].transform("min")]
    return raw.groupby(keys)["value"].mean()


def _station_raw_frame(api, station_id: str, start: str, end: str, codes):
    """Return one station's 5-minute records for TAHMO variable ``codes``, or None.

    Indexed by every 5-minute interval start from ``start`` 00:00 to ``end``
    23:55 UTC; missing records and records failing QC are NaN.
    """
    import pandas as pd

    first = pd.Timestamp(start)
    last = pd.Timestamp(end) + pd.Timedelta(days=1)
    columns = {}
    for code in codes:
        try:
            raw = _fetch_raw(api, station_id, start, last.date().isoformat(), [code])
        except Exception as exc:  # noqa: BLE001
            print(f"{station_id}: DROPPED, fetch failed ({exc})", file=sys.stderr)
            return None
        if raw is None or len(raw) == 0:
            continue
        raw["time"] = pd.to_datetime(raw["time"], format="mixed", utc=True).dt.tz_convert(None)
        raw = raw[(raw["variable"] == code) & (raw["time"] > first) & (raw["time"] <= last)]
        raw = raw[raw["quality"] <= 2]
        # Several sensors of a standard variable are redundant gauges, merged as
        # --daily does. For other codes they usually measure different things
        # (soil depths), so refuse.
        sensors = raw.groupby("time")["sensor"].nunique()
        if code not in VAR_MAP and (sensors > 1).any():
            raise DataError(
                f"{station_id}: {code!r} has several sensors at once "
                f"({', '.join(sorted(raw['sensor'].unique()))}); --variable takes "
                "codes with one sensor per station."
            )
        values = _merge_gauges(raw, "time", pd.Timedelta(0))
        # A 5-minute rain amount (mm) as a rate, so precip is mm day-1 in both modes.
        if code == "pr":
            values = values * SAMPLES_PER_DAY
        columns[VAR_MAP.get(code, code)] = values
    if not columns:
        return None
    frame = pd.DataFrame(columns)
    # Re-stamp each record at its interval start, as the daily values are.
    frame.index = frame.index - pd.Timedelta(minutes=5)
    frame = frame.reindex(pd.date_range(first, last, freq="5min", inclusive="left", name="time"))
    frame["station_id"] = station_id
    return frame


def _station_frame(api, station_id: str, start: str, end: str, day_start_hour: int = 0):
    """Return a daily DataFrame for one station (NaN for incomplete days), or None.

    Days ``start`` .. ``end`` inclusive, each starting at ``day_start_hour`` UTC.
    TAHMO stamps mark the end of each 5-minute interval, so day D holds the
    stamps in (D HH:00, D+1 HH:00]: the HH:00 stamp closes the previous day.
    """
    import pandas as pd

    offset = pd.Timedelta(hours=day_start_hour)
    first = pd.Timestamp(start) + offset
    last = pd.Timestamp(end) + pd.Timedelta(days=1) + offset
    try:
        # The API end bound is an inclusive date (00:00), so round up to the
        # date that includes the HH:00 stamp closing ``end``.
        raw = _fetch_raw(api, station_id, start, last.ceil("D").date().isoformat())
    except Exception as exc:  # noqa: BLE001
        print(f"{station_id}: DROPPED, fetch failed ({exc})", file=sys.stderr)
        return None
    if raw is None or len(raw) == 0:
        return None

    raw["time"] = pd.to_datetime(raw["time"], format="mixed", utc=True).dt.tz_convert(None)
    keep_vars = set(VAR_MAP.keys())
    raw = raw[raw["variable"].isin(keep_vars) & (raw["time"] > first) & (raw["time"] <= last)]
    raw = raw[raw["quality"] <= 2]
    if raw.empty:
        return None

    wide = _merge_gauges(raw, ["time", "variable"], offset).unstack("variable")
    wide = wide.rename(columns=VAR_MAP)
    for name, source in DERIVED.items():
        if source in wide.columns:
            wide[name] = wide[source]

    agg_spec = {c: DAILY_AGG[c] for c in wide.columns if c in DAILY_AGG}
    if not agg_spec:
        return None
    # Re-stamp each record at its interval start, so the HH:00 stamp falls in
    # the day it closes, and shift by the day-start hour so each day bins as
    # one calendar day labelled by the date it starts on.
    wide.index = wide.index - pd.Timedelta(minutes=5) - offset
    daily = wide[list(agg_spec)].resample("D").agg(agg_spec)
    # Plain sum gives 0 mm for an empty day and an undercount for a partial
    # one; a mean over a partial day is biased. Keep only complete days,
    # counting records on the 5-minute grid only.
    on_grid = wide.index == wide.index.floor("5min")
    counts = wide.loc[on_grid, list(agg_spec)].resample("D").count()
    daily = daily.where(counts >= SAMPLES_PER_DAY)
    daily = daily.reindex(pd.date_range(start, end, freq="D", name="time"))
    daily["station_id"] = station_id
    return daily


def _ensure_setup(state):
    """Authenticate and load the deployment catalogue + variable metadata, at most once."""
    import pandas as pd

    if "api" not in state:
        username, password = require_env(
            "TAHMO_API_USERNAME",
            "TAHMO_API_PASSWORD",
            message="TAHMO_API_USERNAME and TAHMO_API_PASSWORD must be set.",
        )
        try:
            from TAHMO import apiWrapper
        except ImportError as exc:
            raise UsageError(
                f"could not import TAHMO ({exc}). Install via "
                f"'pip install git+https://github.com/rhiza-research/tahmo-api'."
            ) from None
        api_local = apiWrapper()
        api_local.setCredentials(username, password)
        stations_raw = api_local.getStations()
        stations_local = pd.json_normalize(list(stations_raw.values()), sep="_")
        state["api"] = api_local
        state["stations"] = stations_local
        state["var_meta"] = api_local.getVariables()
    return state["api"], state["stations"], state["var_meta"]


@weather_skill(
    name="tahmo-fetch",
    version=_SKILL_VERSION,
)
@weather_skill.argument("--start-time", required=True)
@weather_skill.argument("--end-time", required=True)
@weather_skill.argument(
    "--bbox",
    help=(
        "Select every TA station in this box. With --list-stations, the region "
        "to search on the TAHMO deployment API. Ignored for selection when "
        "--station IDs are given."
    ),
)
@weather_skill.argument(
    "--station",
    action="append",
    help="TAHMO station id (repeatable), e.g. TA00025. Discover ids with --list-stations.",
)
@weather_skill.argument(
    "--workers",
    type=int,
    default=DEFAULT_WORKERS,
    help=(
        f"Max concurrent per-station fetch threads (default {DEFAULT_WORKERS}). "
        "Lower this if TAHMO returns 429/throttling errors."
    ),
)
@weather_skill.argument(
    "--day-start-hour",
    type=int,
    default=0,
    help=(
        "UTC hour each daily value starts at (0-23, default 0). Day D covers "
        "D HH:00 to D+1 HH:00 UTC and is labelled D; e.g. 6 for KMSA's 06 UTC rainfall day."
    ),
)
@weather_skill.argument(
    "-v",
    "--variable",
    action="append",
    metavar="CODE",
    help=(
        "Variable to fetch (repeatable): precip, temperature, humidity, pressure, "
        "tmax, tmin (--daily only), or any TAHMO code such as ra, lb, lv."
    ),
)
@weather_skill.argument(
    "--daily",
    action="store_true",
    default=False,
    help=(
        "Write complete-day values (only days where all 288 records passed QC) "
        "instead of 5-minute records. Only precip, temperature, tmax, tmin, "
        "humidity and pressure have them; without -v, all six."
    ),
)
@weather_skill.argument(
    "--gridded",
    action="store_true",
    default=False,
    help=(
        "Read Sheerwater's historical daily TAHMO precip grid (0.25° cell means of "
        "QC'd stations, private GCS, Google credentials) instead of the TAHMO API."
    ),
)
@weather_skill.argument(
    "--gold-stations",
    action="store_true",
    default=False,
    help=(
        "Read Sheerwater's historical daily TAHMO station precip (QC'd complete days "
        "with quality flags, 2015-2025, private GCS, Google credentials) instead of "
        "the TAHMO API. Selects by --station and/or --bbox, or every station."
    ),
)
@weather_skill.argument(
    "--list-stations",
    action="store_true",
    default=False,
    probe=True,
    help=(
        "Print TA stations in --bbox from the TAHMO deployment API as TSV on "
        "stdout (station_id, name, latitude, longitude, country) and exit. No -o."
    ),
)
@weather_skill.argument(
    "--probe-latest",
    nargs="?",
    const="",
    metavar="IDENT",
    probe=True,
    help=(
        "Print the newest complete day in the last 14 days (YYYY-MM-DD) on stdout "
        "and exit; with --gridded, the store's last day with data. Does not download "
        "fields. IDENT is ignored."
    ),
)
def fetch(
    start_time,
    end_time,
    workers,
    bbox,
    station,
    day_start_hour,
    variable,
    daily,
    gridded,
    gold_stations,
    list_stations,
    probe_latest,
    **kwargs,
):
    """Fetch TAHMO weather-station observations as a weather-skills station Zarr."""
    import pandas as pd
    import xarray as xr

    if not 0 <= day_start_hour <= 23:
        raise UsageError(f"--day-start-hour must be 0-23, got {day_start_hour}.")

    if variable and (gridded or gold_stations or list_stations or probe_latest is not None):
        raise UsageError(
            "--variable takes only --station, --bbox and the times; not --gridded, "
            "--gold-stations, --list-stations or --probe-latest."
        )
    fetching = not (gridded or gold_stations or list_stations or probe_latest is not None)
    if fetching and not (variable or daily):
        raise UsageError(
            "pass -v VAR (repeatable) for 5-minute records, or --daily for complete-day "
            "values (precip, temperature, tmax, tmin, humidity, pressure)."
        )
    if fetching and day_start_hour and not daily:
        raise UsageError("--day-start-hour sets when each day starts; it needs --daily.")

    if gold_stations:
        if gridded or list_stations or probe_latest is not None:
            raise UsageError(
                "--gold-stations takes only --station, --bbox and the times; not "
                "--gridded, --list-stations or --probe-latest."
            )
        if day_start_hour != 0:
            raise UsageError(
                "--gold-stations days are UTC calendar dates; --day-start-hour must be 0."
            )
        return _fetch_gold(start_time, end_time, bbox, station)

    if gridded:
        if station or list_stations:
            raise UsageError(
                "--gridded reads whole cells: use --bbox, not --station or --list-stations."
            )
        if day_start_hour != 0:
            raise UsageError("--gridded days are UTC calendar dates; --day-start-hour must be 0.")
        if probe_latest is not None:
            import numpy as np

            ds = _open_gridded()
            has_data = ds["precip"].notnull().any(["latitude", "longitude"]).values
            if not has_data.any():
                raise DataError("gridded TAHMO store has no data")
            print(str(ds["time"].values[np.nonzero(has_data)[0][-1]])[:10])
            return
        return _fetch_gridded(start_time, end_time, bbox)

    if list_stations and bbox is None:
        raise UsageError("--list-stations requires --bbox N/W/S/E.")
    if not list_stations and probe_latest is None and not station and bbox is None:
        raise UsageError("pass --station ID (repeatable) and/or --bbox N/W/S/E.")

    api, stations, var_meta = _ensure_setup({})
    # pr, te, rh, ap are also accepted under their standard names.
    names = [VAR_MAP.get(v, v) for v in variable or []]
    code_for = {name: code for code, name in VAR_MAP.items()}
    codes = [code_for.get(n, n) for n in names]
    if daily:
        no_daily = [n for n in names if n not in DAILY_AGG]
        if no_daily:
            raise UsageError(
                f"--daily has complete-day values only for {', '.join(DAILY_AGG)}; not for "
                f"{', '.join(no_daily)}. Drop --daily for 5-minute records."
            )
    else:
        extremes = [n for n in names if n in DERIVED]
        if extremes:
            raise UsageError(f"{', '.join(extremes)} are daily extremes; add --daily.")
        # lv is recorded but missing from getVariables().
        unknown = sorted(set(codes) - set(var_meta) - set(_RAW_META))
        if unknown:
            raise UsageError(
                f"unknown --variable {', '.join(unknown)}. Choose from: "
                + ", ".join(sorted(set(VAR_MAP.values()) | set(var_meta) | set(_RAW_META)))
            )

    if list_stations:
        _print_station_list(_filter_bbox(_ta_stations(stations), bbox))
        return

    if probe_latest is not None:
        from datetime import UTC, datetime, timedelta

        now = datetime.now(UTC)
        end = now.date()
        start = end - timedelta(days=14)
        # Day D ends at D+1 HH:00 UTC, so the newest day that can be complete
        # is the one that ended last; before HH:00 that is two days back.
        newest = (now - timedelta(hours=day_start_hour)).date() - timedelta(days=1)
        if station or bbox is not None:
            selected = _select_stations(stations, station, bbox)
        else:
            selected = _ta_stations(stations).head(3)
        # Stop at the first station with that day complete instead of
        # downloading every station in the box.
        latest = None
        pool = ThreadPoolExecutor(max_workers=workers)
        futures = [
            pool.submit(
                _station_frame,
                api,
                row["code"],
                start.isoformat(),
                end.isoformat(),
                day_start_hour,
            )
            for _, row in selected.iterrows()
        ]
        for fut in as_completed(futures):
            daily = fut.result()
            if daily is None:
                continue
            observed = daily.drop(columns="station_id").dropna(how="all")
            if observed.empty:
                continue
            day = observed.index.max().date()
            latest = day if latest is None else max(latest, day)
            if latest >= newest:
                break
        pool.shutdown(wait=False, cancel_futures=True)
        if latest is None:
            raise DataError("TAHMO probe found no recent complete days")
        print(latest.isoformat())
        return

    selected = _select_stations(stations, station, bbox)
    start = start_time.isoformat()
    end = end_time.isoformat()
    step = "days" if daily else "5-minute steps"
    print(
        f"Fetching {len(selected)} TAHMO station(s) {start} → {end}, "
        + (
            f"days starting {day_start_hour:02d}:00 UTC"
            if daily
            else f"5-minute records of {', '.join(names)}"
        ),
        file=sys.stderr,
    )

    def _fetch_one(row):
        sid = row["code"]
        if daily:
            frame = _station_frame(api, sid, start, end, day_start_hour)
            if frame is not None and names:
                keep = [n for n in names if n in frame.columns]
                frame = frame[keep + ["station_id"]] if keep else None
        else:
            frame = _station_raw_frame(api, sid, start, end, codes)
        if frame is None:
            return None
        return frame, {
            "station_id": sid,
            "latitude": float(row["location_latitude"]),
            "longitude": float(row["location_longitude"]),
            "country": _coord_str(row, "location_countrycode"),
            "name": _station_name(row),
        }

    frames = []
    meta_rows = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_one, row): row["code"] for _, row in selected.iterrows()}
        for fut in as_completed(futures):
            sid = futures[fut]
            try:
                result = fut.result()
            except DataError:
                raise
            except Exception as exc:  # noqa: BLE001
                print(f"{sid}: DROPPED, worker error ({exc})", file=sys.stderr)
                continue
            if result is None:
                print(f"{sid}: no data in the requested range", file=sys.stderr)
                continue
            frame, meta_row = result
            frames.append(frame)
            meta_rows.append(meta_row)
            complete = int(frame.drop(columns="station_id").notna().any(axis=1).sum())
            print(f"{sid}: {complete}/{len(frame)} {step} with a complete record", file=sys.stderr)

    if not frames:
        raise DataError("no data returned for any station.")

    df = pd.concat(frames).reset_index()
    meta = pd.DataFrame(meta_rows).drop_duplicates("station_id").set_index("station_id")
    df = df.set_index(["time", "station_id"])

    ds = xr.Dataset.from_dataframe(df)
    ids = ds["station_id"].values
    ds = ds.assign_coords(
        latitude=("station_id", meta.loc[ids, "latitude"].values),
        longitude=("station_id", meta.loc[ids, "longitude"].values),
        country=("station_id", meta.loc[ids, "country"].values),
        name=("station_id", meta.loc[ids, "name"].values),
    )
    ds["latitude"].attrs.update(standard_name="latitude", units="degrees_north", axis="Y")
    ds["longitude"].attrs.update(standard_name="longitude", units="degrees_east", axis="X")
    ds["time"].attrs.update(
        standard_name="time",
        axis="T",
        comment=(
            f"day D covers D {day_start_hour:02d}:00 to D+1 {day_start_hour:02d}:00 UTC"
            if daily
            else "each label is the start of a 5-minute interval, UTC"
        ),
    )
    ds["station_id"].attrs.update(cf_role="timeseries_id", long_name="TAHMO station identifier")
    ds["country"].attrs.update(long_name="ISO 3166-1 alpha-2 country code")
    ds["name"].attrs.update(long_name="station name")
    short_code_for = {v: k for k, v in VAR_MAP.items()}
    for canonical in ds.data_vars:
        short = short_code_for.get(canonical, canonical)
        api_meta = {**var_meta.get(short, {}), **_RAW_META.get(short, {})}
        std_name, units_override = CF_META.get(canonical, (None, None))
        attrs = {"coordinates": "latitude longitude"}
        if std_name:
            attrs["standard_name"] = std_name
        units = _pint_units(units_override) or _pint_units(api_meta.get("units"))
        if not units:
            raise DataError(
                f"variable {canonical!r} has no pint-parseable units from TAHMO metadata."
            )
        exc = udunits_error(units)
        if exc is not None:
            raise DataError(
                f"units {units!r} for variable {canonical!r} are not udunits-valid "
                f"({exc}); refusing to write a non-CF store."
            )
        attrs["units"] = units
        description = api_meta.get("description")
        if description:
            attrs["long_name"] = description
        if canonical in EXTREMES:
            attrs["cell_methods"], attrs["long_name"] = EXTREMES[canonical]
        ds[canonical].attrs.update(attrs)
    ds.attrs.update(
        featureType="timeSeries",
        Conventions="CF-1.13",
        weather_skills_source="tahmo",
    )
    for name in names:
        if name not in ds.data_vars:
            print(f"WARNING: no requested station has {name!r} data.", file=sys.stderr)
    # A station is complete when it reported every requested step; multi-day
    # totals are only meaningful for those (missing steps are never filled).
    # Requested stations with no data count as incomplete.
    n_steps = ((end_time - start_time).days + 1) * (1 if daily else SAMPLES_PER_DAY)
    for name in sorted(ds.data_vars):
        complete = int((ds[name].notnull().sum("time") == n_steps).sum())
        print(
            f"{name}: {complete} of {len(selected)} requested stations complete "
            f"(all {n_steps} {step} {start} → {end})",
            file=sys.stderr,
        )
    return stamp_data_interval(ds, period="1 day" if daily else "5 minutes")


if __name__ == "__main__":
    fetch()
