# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrclmb.ac.uk) [1]
# *
# * [1] MRC Laboratory of Molecular Biology (MRC-LMB)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
# * 02111-1307  USA
# *
# *  All comments concerning this program package may be sent to the
# *  e-mail address 'gsharov@mrclmb.ac.uk'
# *
# **************************************************************************

""" Statistics views used by the dashboards: continuous aggregates, materialized views and plain views.
Each view is one file: sql/<schema>/caggs|mviews|views/<name>.sql. STATS lists them with their dependencies,
the commands work out the order:
    create:    creates the views that are missing, with the missing views they read from.
               force: drops and recreates the views and the views that read from them.
    drop:      drops the views and the views that read from them.
    refresh:   refreshes the views, then the views that read from them.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from em_health.db_manager import DatabaseManager
from em_health.utils.tools import logger

Kind = Literal["cagg", "mview", "view"]


@dataclass(frozen=True)
class CaggPolicy:
    """ Cagg refresh policy, see DatabaseManager.schedule_cagg_refresh. """
    start_offset: str
    end_offset: str
    interval: str


HOURLY = CaggPolicy(start_offset="6 hours", end_offset="1 hour", interval="1 hour")
DAILY = CaggPolicy(start_offset="4 days", end_offset="1 day", interval="12 hours")
# a month bucket is refreshed only once it is complete: cover the last 2 full months
MONTHLY = CaggPolicy(start_offset="3 months", end_offset="1 day", interval="1 day")

MVIEW_REFRESH_INTERVAL = "12 hours"


@dataclass(frozen=True)
class Stat:
    name: str  # schema.name, also the file name
    kind: Kind
    dbs: tuple[str, ...]
    deps: tuple[str, ...] = ()  # views of STATS this view reads from
    policy: CaggPolicy | None = None  # caggs only

    @property
    def path(self) -> str:
        schema, name = self.name.split(".")
        return f"{schema}/{self.kind}s/{name}.sql"


TEM = ("tem",)
SEM = ("sem",)
BOTH = ("tem", "sem")

STATS: tuple[Stat, ...] = (
    Stat("events.em_off", "mview", BOTH),
    Stat("events.vacuum_state_daily", "mview", TEM, deps=("events.em_off",)),

    Stat("events.load_counter_values_daily", "cagg", BOTH, policy=DAILY),
    Stat("events.load_counters_daily", "view", BOTH, deps=("events.load_counter_values_daily",)),

    # TEM
    Stat("events.data_counter_values_daily", "cagg", TEM, policy=DAILY),
    Stat("events.data_counters_daily", "view", TEM, deps=("events.data_counter_values_daily",)),
    Stat("events.image_counter_values_daily", "cagg", TEM, policy=DAILY),
    Stat("events.image_counters_daily", "view", TEM, deps=("events.image_counter_values_daily",)),
    Stat("events.tem_cryocycles", "mview", TEM),
    Stat("events.tem_pressure_hourly", "cagg", TEM, policy=HOURLY),
    Stat("events.tem_pressure_daily", "cagg", TEM, deps=("events.tem_pressure_hourly",), policy=DAILY),
    Stat("events.tem_pressure_monthly", "cagg", TEM, deps=("events.tem_pressure_daily",), policy=MONTHLY),

    Stat("events.epu_sessions", "mview", TEM),
    Stat("events.epu_runs", "mview", TEM, deps=("events.epu_sessions",)),
    Stat("events.epu_counters", "mview", TEM, deps=("events.epu_sessions",)),
    Stat("events.epu_running_daily", "mview", TEM, deps=("events.epu_sessions",)),
    Stat("events.tomo_sessions", "mview", TEM),
    Stat("events.tomo_runs", "mview", TEM, deps=("events.tomo_sessions",)),
    Stat("events.tomo_counters", "mview", TEM, deps=("events.tomo_sessions",)),
    Stat("events.tomo_running_daily", "mview", TEM, deps=("events.tomo_sessions",)),

    # SEM
    Stat("events.fib_baa_counter_values_daily", "cagg", SEM, policy=DAILY),
    Stat("events.fib_baa_counters_daily", "view", SEM, deps=("events.fib_baa_counter_values_daily",)),
    Stat("events.fib_bda_counter_values_daily", "cagg", SEM, policy=DAILY),
    Stat("events.fib_bda_counters_daily", "view", SEM, deps=("events.fib_bda_counter_values_daily",)),
    Stat("events.gis_counter_values_daily", "cagg", SEM, policy=DAILY),
    Stat("events.gis_counters_daily", "view", SEM, deps=("events.gis_counter_values_daily",)),
    Stat("events.aperture_strip_replacements", "mview", SEM),
    Stat("events.fibsem_source_replacements", "mview", SEM),
    Stat("events.sem_cryocycles", "mview", SEM),
    Stat("events.sem_temperature_states", "mview", SEM),
    Stat("events.sem_pressure_hourly", "cagg", SEM, policy=HOURLY),
    Stat("events.sem_pressure_daily", "cagg", SEM, deps=("events.sem_pressure_hourly",), policy=DAILY),
    Stat("events.sem_pressure_monthly", "cagg", SEM, deps=("events.sem_pressure_daily",), policy=MONTHLY),
    Stat("events.fib_beam_daily", "cagg", SEM, policy=DAILY),
    Stat("events.sem_beam_daily", "cagg", SEM, policy=DAILY),
    Stat("events.flm_beam_daily", "cagg", SEM, policy=DAILY),
    Stat("events.sem_beamtime_daily", "mview", SEM,
         deps=("events.em_off", "events.sem_cryocycles", "events.fib_beam_daily",
               "events.sem_beam_daily", "events.flm_beam_daily")),
)


def registry(dbname: str) -> dict[str, Stat]:
    """ Views of one database by name, in STATS order. """
    return {s.name: s for s in STATS if dbname in s.dbs}


def resolve(dbname: str, names: Iterable[str]) -> list[str]:
    """ Full view names from the command line: 'all', schema.name or name. """
    reg = registry(dbname)
    names = list(names)
    if "all" in names:
        return list(reg)

    result = []
    for name in names:
        full = name if "." in name else f"events.{name}"
        if full not in reg:
            raise ValueError(f"Unknown view '{name}' for database {dbname}, see 'emhealth stats -d {dbname} list'")
        result.append(full)
    return result


def ordered(reg: dict[str, Stat], names: Iterable[str]) -> list[str]:
    """ Sort names so that each view comes after the views it reads from. """
    names = set(names)
    result: list[str] = []

    def visit(name: str) -> None:
        if name in result:
            return
        for dep in reg[name].deps:
            if dep in names:
                visit(dep)
        result.append(name)

    for name in reg:
        if name in names:
            visit(name)
    return result


def upstream(reg: dict[str, Stat], names: Iterable[str]) -> set[str]:
    """ The views and all views they read from. """
    result: set[str] = set()
    todo = list(names)
    while todo:
        name = todo.pop()
        if name not in result:
            result.add(name)
            todo.extend(reg[name].deps)
    return result


def downstream(reg: dict[str, Stat], names: Iterable[str]) -> set[str]:
    """ The views and all views that read from them. """
    result = set(names)
    changed = True
    while changed:
        changed = False
        for s in reg.values():
            if s.name not in result and result.intersection(s.deps):
                result.add(s.name)
                changed = True
    return result


class StatsManager(DatabaseManager):
    """ Create, drop and refresh the views of STATS. """
    def __init__(self, dbname: str, **kwargs):
        super().__init__(dbname, **kwargs)
        self.reg = registry(dbname)

    def existing(self) -> set[str]:
        """ Views of the registry that exist (a cagg is a view in pg_class). """
        rows = self.run_query("""
            SELECT n.nspname || '.' || c.relname
            FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('v', 'm')
              AND n.nspname || '.' || c.relname = ANY(%s)
        """, values=(list(self.reg),), fetch="all")
        return {r[0] for r in rows}

    def create_one(self, s: Stat) -> None:
        self.create_mview(s.name, s.path)
        if s.kind == "cagg":
            assert s.policy is not None
            self.force_refresh_cagg(s.name)
            self.schedule_cagg_refresh(s.name, start_offset=s.policy.start_offset,
                                       end_offset=s.policy.end_offset, interval=s.policy.interval)
            self.enable_rt_cagg(s.name)
        elif s.kind == "mview":
            self.schedule_mview_refresh(s.name, interval=MVIEW_REFRESH_INTERVAL)

    def refresh_one(self, s: Stat) -> None:
        if s.kind == "cagg":
            self.force_refresh_cagg(s.name)
        elif s.kind == "mview":
            self.run_query("REFRESH MATERIALIZED VIEW {view}", {"view": s.name})
            logger.info("Refreshed MVIEW %s", s.name)

    def create(self, names: list[str], force: bool = False) -> None:
        """ Create the views that are missing and the missing views they read from.
        force: drop and recreate the views and the views that read from them. """
        todo = upstream(self.reg, names)
        if force:
            rebuild = downstream(self.reg, names)
            self.drop(list(rebuild))
            todo |= upstream(self.reg, rebuild)

        exists = self.existing()
        skipped = [n for n in names if n in exists]
        if skipped:
            logger.info("Already exist (use -f to recreate): %s", ", ".join(skipped))
        for name in ordered(self.reg, todo):
            if name not in exists:
                self.create_one(self.reg[name])

    def drop(self, names: list[str]) -> None:
        """ Drop the views and the views that read from them, in reverse order. """
        for name in reversed(ordered(self.reg, downstream(self.reg, names))):
            self.drop_mview(name, kind=self.reg[name].kind)

    def refresh(self, names: list[str]) -> None:
        """ Refresh the views, then the views that read from them. """
        exists = self.existing()
        missing = [n for n in names if n not in exists]
        if missing:
            raise ValueError(f"Missing views: {', '.join(missing)}. Run 'emhealth stats create' first")

        for name in ordered(self.reg, downstream(self.reg, names)):
            if name in exists:
                self.refresh_one(self.reg[name])
            else:
                logger.warning("%s does not exist, not refreshed", name)

    def list_views(self) -> None:
        """ Print the views with their state and refresh schedule. """
        exists = self.existing()
        rows = self.run_query("""
            SELECT config ->> 'view', schedule_interval::text
            FROM timescaledb_information.jobs
            WHERE proc_schema = 'public' AND proc_name = 'refresh_mview'
            UNION ALL
            -- a cagg refresh policy job has the cagg as its hypertable
            SELECT hypertable_schema || '.' || hypertable_name, schedule_interval::text
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_refresh_continuous_aggregate'
        """, fetch="all")
        schedules = dict(rows)

        width = max(len(n) for n in self.reg)
        print(f"{'VIEW':<{width}}  {'KIND':<5}  {'EXISTS':<6}  {'REFRESH':<8}  DEPENDS ON")
        for name, s in self.reg.items():
            refresh = schedules.get(name, "-")
            print(f"{name:<{width}}  {s.kind:<5}  {'yes' if name in exists else 'no':<6}  {refresh:<8}  "
                  f"{', '.join(s.deps) or '-'}")


def main(dbname: str, action: str, views: list[str] | None = None, force: bool = False) -> None:
    with StatsManager(dbname) as db:
        if action == "list":
            db.list_views()
            return

        names = resolve(dbname, views or [])
        # creating and refreshing reads all raw data: give sorts/hashes in this session more memory
        db.run_query("SET work_mem = '256MB'")
        if action == "create":
            db.create(names, force=force)
        elif action == "drop":
            db.drop(names)
        elif action == "refresh":
            db.refresh(names)
