# Weather Skills Fetchers

> ⚠️ **Under active development — not production ready.**
>
> These skills are an early experiment in tool composition for weather/climate
> data pipelines. Interfaces, dataset schema, and skill boundaries may change
> without notice. Fetchers hit real APIs and require credentials; middle-
> pipeline skills have only been smoke-tested on small synthetic data. Do not
> use in any automated workflow you rely on, and do not assume outputs are
> scientifically validated. Expect breakage.

Fetcher skills: each one pulls a forecast, reanalysis, satellite or station
dataset from its source and writes it as a weather-skills Zarr — see
[`STANDARD_DATASET.md`](https://github.com/weather-skills/weather-skills-core/blob/main/docs/weather-skill-authoring/references/STANDARD_DATASET.md). Transform the result with the skills in
[weather-skills/weather-skills](https://github.com/weather-skills/weather-skills).

Built as [Agent Skills](https://agentskills.io) by Rhiza Research.

## Skills

Prefer `dynamical-fetch` when the dynamical.org catalog has the dataset
(GFS, GEFS, ECMWF IFS-ENS, AIFS, Google WeatherNext 2 and 3, ICON-EU, analyses,
IMERG, CHIRPS). Use a
credentialed or source-specific fetcher only when it does not.

| Skill | What it does |
|---|---|
| `dynamical-fetch` | **Preferred when the catalog has it.** dynamical.org open catalog (GFS, GEFS, ECMWF IFS-ENS 15-day and 46-day, AIFS, Google WeatherNext 2, WeatherNext 3 mean and percentiles, ICON-EU, MRMS, analyses, **IMERG**, **CHIRPS**) via `--dataset`, credential-free → Zarr. Forecasts: one init (`--date`) or a range of inits (repeatable `--start-time`/`--end-time`). Default IMERG source: `nasa-imerg-analysis-late` / `nasa-imerg-analysis-early`. CHIRPS products: `ucsb-chc-chirps-analysis-final` / `ucsb-chc-chirps-analysis-preliminary`. |
| `ecmwf-fetch` | ECDS **fallback** for ECMWF S2S / ER (pre-2026, ocean, unmapped fields). Prefer `dynamical-fetch --dataset ecmwf-ifs-ens-forecast-46-day-daily-1-5-degree` for 2026+ catalog fields. |
| `chirps-fetch` | Default CHIRPS source: dynamical.org final + prelim fallback → `precip` Zarr |
| `imerg-fetch` | Earthdata **daily** IMERG Final (`GPM_3IMERGDF`) → Zarr. All IMERG Late/Early comes from `dynamical-fetch`. |
| `clim-fetch` | Climatology (mean + std) for a `--dataset` (`imerg_final`, `era5`, `chirps`, `ecmwf_ifs`, `oisst`, …), at a selected `--prediction-timedelta` lead and `--window` (days; correctly rolled up, centered, with circular padding if not pre-mirrored), expanded to a `--start-time`/`--end-time` window → Zarr. OISST SST is `--dataset oisst --variable sst`. |
| `tahmo-fetch` | TAHMO station observations (daily-aggregated) → Zarr |
| `3dpaws-fetch` | 3D-PAWS station observations from a CHORDS portal (default: FEWS NET Kenya; daily precip, temperature, humidity, pressure, wind) → station Zarr. Requires credentials. |
| `cumulus-fetch` | Cumulus AI operational ensemble precip (Aurora S2S run) — 30 members, 46 daily leads, 0.25° or 1° global (`--dataset precip`, `precip_1deg`, `precip_era5`), one init or a range of inits → Zarr. Requires credentials. |
| `cumulus-downscaled-fetch` | Cumulus AI downscaled Aurora precip — 0.1° over East and West Africa, 46 daily leads, diffusion / flow-matching / UNet, ensemble mean or 10 members → Zarr. Requires credentials. |
| `cumulus-fetch` | Cumulus AI operational ensemble precip (Aurora S2S run) — 30 members, 46 daily leads, 0.25° or 1° global (`--dataset precip`, `precip_1deg`, `precip_era5`), one init or a range of inits → Zarr. Requires credentials. |
| `cumulus-downscaled-fetch` | Cumulus AI downscaled Aurora precip — 0.1° over East and West Africa, 46 daily leads, diffusion / flow-matching / UNet, ensemble mean or 10 members → Zarr. Requires credentials. |
| `hindcast-fetch` | ECMWF (inits 1996–2025), GEFS (1998–2019) or Cumulus (1996 to 2026-08-31) subseasonal hindcasts for one or more ranges of init dates → multi-init forecast Zarr of daily precip, 2 m temperature and (ECMWF) SST. ECMWF and GEFS need no credentials; Cumulus does. |
| `neuralgcm-fetch` | NeuralGCM S2S ensemble (IMERG precip `precip`, ERA5 surface `t2m`/`d2m`) — up to 50 members, 6-hour leads to 60 days, ~2.8°, one init or a range of inits → Zarr. Requires credentials. |
| `weathernext-fetch` | Google WeatherNext 2 ensemble (2025 on; 64 members, 6-hour leads to 15 days, 0.25°, 13 pressure levels) or WeatherNext 3 (2026 on; 64-member ensemble, requester pays, or free ensemble mean/percentiles; hourly leads to 15 days, 0.1°) → Zarr. One init or a range of inits. Pass `-v` and `--member` or `--statistic` (full cubes are ~1 TB). Requires credentials. |
| `windborne-fetch` | WindBorne data, one `--product` per call: WeatherMesh-6 ensemble grids (live or archived runs from mid-2026; 128 members as mean, std or all; 3-hour leads to 15 days, 0.25°), point forecasts with spread, percentiles and rain-exceedance probabilities, vertical soundings, plain-language site summaries, ERA5 1991–2020 hourly normals at points, the recent ECMWF IFS analysis, or US state heating/cooling degree days → Zarr. Requires credentials. |
| `pbc-fetch` | PBC / StillLearning AI Weather Quest precip **quintile probabilities** (`era5-p_pr_19` week 3, `era5-p_pr_26` week 4), 1.5° global, one init or a range of inits → Zarr. Requires credentials. |
| `ecmwf-hres-fetch` | Deterministic ECMWF HRES forecast — 0.25°, out to 15 days, surface and pressure-level fields → Zarr. No credentials; needs the eccodes system library. |
| `arco-era5-fetch` | ERA5 hourly reanalysis (ARCO-ERA5) → Zarr. No credentials. |
| `cmip6-fetch` | One CMIP6 model run (historical 1850–2014 or `ssp*` to 2100) → Zarr. No credentials. |
| `rfe-fetch` | NOAA CPC African Rainfall Estimate (RFE2) — daily, 0.1° over Africa, from 2001 → Zarr. No credentials. |
| `gsmap-fetch` | JAXA GSMaP v6 gauge-corrected hourly 0.1° precip (from April 2025, 60°N–60°S) → Zarr. No credentials. |
| `pardig-fetch` | Google PARDIG AI precip estimate, 15-minute 0.1° (NRT 2016 to a few hours ago, LR 1999–2023) → Zarr. No credentials. |
| `oisst-fetch` | NOAA OISST v2.1 daily 0.25° sea-surface temperature (1981-09 on) → Zarr. No credentials. |
| `soil-moisture-fetch` | Daily satellite surface soil moisture → Zarr, one `--dataset`: `ascat` (C3S ACTIVE, % saturation, 0.25°, 1991 on), `combined` (C3S COMBINED, m³/m³, 0.25°, 1978 on), `smap` (`SPL3SMP_E` 9 km, 2015 on), `smos` (CATDS 25 km, 2010 on), or `smap-smos` (NSIDC-0800 intercalibrated, 9 km). SMAP/SMOS take the 6am, 6pm or both overpasses. Requires credentials (CDS or Earthdata; none for `smos`). |
| `ghcn-daily-fetch` | NOAA GHCN-Daily station observations (daily precip and max/min/mean temperature, ~130,000 stations) → station Zarr. No credentials. |
| `openaq-fetch` | OpenAQ air-quality station observations (daily mean PM2.5, PM10, NO2, O3, SO2, CO) → station Zarr. Requires credentials. |
| `cams-fetch` | CAMS global air-quality fields (PM2.5, PM10, near-surface NO2/O3/SO2/CO, AOD, total columns) — 5-day forecast at 0.4° (2015 on) or EAC4 reanalysis at 0.75° (2003 on) → Zarr. Requires credentials. |
| `meteosat-fetch` | Meteosat FCI or SEVIRI satellite imagery (0° or Indian Ocean) at chosen times — visible reflectance and infrared brightness temperature on a regular lat/lon grid at native sampling → Zarr. Up to 12 scans per call by default, since each is a ~200 MB whole-disk download. Requires credentials. |

## Install

### As a CLI tool

For ad-hoc command-line use (no agent involved), install the skills as a
single `weather-skills-fetchers` binary:

```bash
# One-shot, no install — list available skills
uvx --from git+https://github.com/rhiza-research/weather-skills-fetchers weather-skills-fetchers

# Run one
uvx --from git+https://github.com/rhiza-research/weather-skills-fetchers weather-skills-fetchers <skill> [args]
```

Or install once and invoke directly:

```bash
uv tool install git+https://github.com/rhiza-research/weather-skills-fetchers
weather-skills-fetchers                          # list
weather-skills-fetchers <skill> [args]           # run one
```

Each skill's PEP 723 inline dependency block is resolved by `uv run`
on each invocation, so the runner itself contributes no Python deps to the
script's runtime environment.

### As agent skills

For use by an LLM agent (Claude Code, etc.), install the `SKILL.md` files
into your project with [skillkit](https://github.com/rohitg00/skillkit) — no
local install needed, `npx` runs the latest skillkit on demand (add `@latest`
to always pull the newest):

```bash
# List what skillkit discovers in the repo
npx skillkit install rhiza-research/weather-skills-fetchers --list

# Install all skills to the current project
npx skillkit install rhiza-research/weather-skills-fetchers --all --yes

# Install globally so any project can use them
npx skillkit install rhiza-research/weather-skills-fetchers --all --yes --global

# Target a specific agent (otherwise skillkit installs for every agent it detects)
npx skillkit install rhiza-research/weather-skills-fetchers --all --yes --agent claude-code

# Install just a subset
npx skillkit install rhiza-research/weather-skills-fetchers --skill=ecmwf-fetch
npx skillkit install rhiza-research/weather-skills-fetchers --skills=dynamical-fetch,chirps-fetch

# Overwrite an existing install
npx skillkit install rhiza-research/weather-skills-fetchers --all --yes --force
```

Pin in a manifest for team / reproducible use:

```bash
npx skillkit manifest init
npx skillkit manifest add rhiza-research/weather-skills-fetchers
npx skillkit manifest install
```

See `npx skillkit install --help` for more flags.

## Standard dataset contract

Skills share a simple Zarr contract: fixed dimension names (`latitude`,
`longitude`, `time`, `step`, `number`, `station_id`, …) and short types (`spatial`, `observations`, `forecast`,
`vertical_forecast`, `point_obs`, …).
See
[`STANDARD_DATASET.md`](https://github.com/weather-skills/weather-skills-core/blob/main/docs/weather-skill-authoring/references/STANDARD_DATASET.md).
Fetchers write that shape; other skills only depend on dims, coords, data
variables, and `weather_skills_*` attrs — not on per-variable encoding.

## CLI flag conventions

Each skill declares its CLI through the `@weather_skill` decorator from
`weather_skills_core`, so common parameters (`--input` / `-o`, `--bbox`,
`--start-time` / `--end-time`, etc.) mean the same thing wherever they appear.
See
[`CONVENTIONS.md`](https://github.com/weather-skills/weather-skills-core/blob/main/docs/weather-skill-authoring/references/CONVENTIONS.md)
for the full mapping of concept → canonical flag.

## Credentials

Fetchers read credentials from environment variables (or `.netrc` where
supported by the underlying client). Nothing is hardcoded. See each skill's
`compatibility:` frontmatter for the specific vars required.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the publishing model (`main` is the
consumer-facing branch — every merge is a release), the PR workflow, and the
version-bump conventions (per-skill `metadata.version` in SKILL.md, driven by `release: major`
/ `release: minor` PR labels, with patch as the default).

## License

MIT. See [`LICENSE`](LICENSE).
