import json
import logging
import pickle
from hashlib import sha1
from operator import itemgetter
from typing import Any, Dict, Generator, Tuple

import pandas as pd
import sqlite3

from ixmp.backend import ItemType
from ixmp.backend.base import Backend
from ixmp.backend.io import s_write_gdx
from ixmp.core import TimeSeries

log = logging.getLogger(__name__)


class DatabaseBackend(Backend):
    """Backend based on Python DB API 2.0."""

    # TODO use CachingBackend as the base class

    # Database connection object.
    conn = None

    _index = {}

    def __init__(self, **kwargs):
        self._db_path = kwargs.pop("path")
        assert 0 == len(kwargs)

        # Open and maybe initialize the database
        self.open_db()

    # Optional methods

    def open_db(self):
        self.conn = sqlite3.connect(self._db_path)
        init_schema(self.conn)

    def close_db(self):
        try:
            self.conn.close()
        except AttributeError:
            pass  # Already closed
        else:
            self.conn = None

    # Methods for ixmp.Platform

    def set_node(self, name, parent=None, hierarchy=None, synonym=None):
        if synonym:
            raise NotImplementedError

        hierarchy = hierarchy or "default"
        self._insert_code(f"node_{hierarchy}", name, parent)

    def get_nodes(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM code WHERE codelist LIKE 'node_%'")
        while True:
            row = cur.fetchone()
            if row is None:
                return
            yield (row[1], None, row[2], row[0].split("node_")[-1])

    def get_timeslices(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT c.id, c.codelist, a.value FROM code AS c INNER JOIN annotation AS a
            ON a.obj_class == 'code' AND printf('%s:%s', c.codelist, c.id) == a.obj_id
            WHERE codelist LIKE 'timeslice_%'
            """
        )
        while True:
            row = cur.fetchone()
            if row is None:
                return
            yield (row[0], row[1].split("timeslice_")[1], float(row[2]))

    def set_timeslice(self, name, category, duration):
        codelist = f"timeslice_{category}"
        self._insert_code(codelist, name)
        self._annotate_code(codelist, name, "duration", repr(duration))

    def add_model_name(self, name):
        self._insert_code("model_name", name)

    def add_scenario_name(self, name):
        self._insert_code("scenario_name", name)

    def get_model_names(self) -> Generator[str, None, None]:
        yield from map(itemgetter(0), self._select_codes("model_name"))

    def get_scenario_names(self) -> Generator[str, None, None]:
        yield from map(itemgetter(0), self._select_codes("scenario_name"))

    def set_unit(self, name, comment):
        self._insert_code("unit", name)
        if comment:
            log.info(comment)

    def get_units(self):
        return list(map(itemgetter(0), self._select_codes("unit")))

    def write_file(self, path, item_type, **kwargs):
        # TODO move this code to the parent class. s_write_gdx() works with any
        #      backend, so it should be for any class that *doesn't* overload it
        s, kwargs["filters"] = self._handle_rw_filters(kwargs.pop("filters", {}))

        if path.suffix == ".gdx" and item_type is ItemType.MODEL and s:
            s_write_gdx(self, s, path, **kwargs)
        else:
            super().write_file(path, item_type, **kwargs)

    # Methods for ixmp.TimeSeries

    def init(self, ts, annotation):
        # Identifiers for `ts`
        info = [ts.__class__.__name__, ts.model, ts.scenario]
        cur = self.conn.cursor()

        # Identify the maximum previous version matching these identifiers
        cur.execute(
            """
            SELECT max(version) FROM timeseries WHERE class = ? AND model_name = ?
            AND scenario_name = ?
            """,
            info,
        )
        previous_version = cur.fetchone()[0] or 0

        # Use the next available version
        ts.version = previous_version + 1

        # Insert
        cur.execute(
            """
            INSERT OR ABORT INTO timeseries (class, model_name, scenario_name, version)
            VALUES (?, ?, ?, ?)
            """,
            info + [ts.version],
        )
        cur.execute("SELECT last_insert_rowid()")

        # Store the ID
        self._index[ts] = cur.fetchone()[0]

        self.conn.commit()

    def get(self, ts):
        args = [ts.model, ts.scenario]
        if ts.version:
            query = """
                SELECT ts.id, ts.version FROM timeseries AS ts WHERE model_name = ?
                AND scenario_name = ? AND version = ?
            """
            args.append(ts.version)
        else:
            query = """
                SELECT ts.id, ts.version FROM timeseries AS ts JOIN annotation AS a
                ON a.obj_id == ts.id WHERE ts.model_name = ? AND ts.scenario_name = ?
                AND a.obj_class == 'timeseries' AND a.id == '__ixmp_default_version'
            """

        cur = self.conn.cursor()
        cur.execute(query, (ts.model, ts.scenario))

        id, version = cur.fetchone()

        ts.version = ts.version or version
        assert ts.version == version  # Sanity check

        self._index[ts] = id

    def check_out(self, ts, timeseries_only):
        if timeseries_only:
            log.info("timeseries_only=True ignored")
        log.warning("check_out() has no effect")

    def commit(self, ts, comment):
        log.warning("commit() has no effect")
        log.info(comment)

    def set_as_default(self, ts):
        cur = self.conn.cursor()

        cond = "a.obj_class == 'timeseries' AND a.id == '__ixmp_default_version'"

        cur.execute(
            f"""
            SELECT ts.id FROM timeseries AS ts JOIN annotation AS a ON a.obj_id == ts.id
            WHERE ts.model_name = ? AND ts.scenario_name = ? AND {cond}
            """,
            (ts.model, ts.scenario),
        )
        existing_default = cur.fetchone()

        if existing_default:
            # Delete the existing default
            cur.execute(
                f"DELETE FROM annotation AS a WHERE a.obj_id = ? AND {cond}",
                existing_default,
            )

        self._annotate(ts, "__ixmp_default_version", None)

    def is_default(self, ts):
        return self._select_anno(ts, "__ixmp_default_version") is not None

    def run_id(self, ts):
        return self._jindex[ts]

    def _hash(self, identifiers):
        return sha1(json.dumps(identifiers).encode()).hexdigest()

    def set_data(self, ts, region, variable, data, unit, subannual, meta):
        cur = self.conn.cursor()

        identifiers = dict(
            region=region,
            variable=variable,
            subannual=subannual,
            unit=unit,
            meta=meta,
        )

        # Compute a unique name for this combination of identifiers
        name = self._hash(identifiers)
        log.debug(f"hash {name} for {identifiers}")

        # Create the entry in the database
        self.init_item(ts, "tsdata", name, ["year"], ["year"])

        # Retrieve any existing data
        id, existing = self._item_data(ts, name)

        if existing:
            raise NotImplementedError("set_data() with existing data")

        all_data = identifiers.copy()
        all_data["data"] = data

        # Dump the data
        cur.execute(
            "INSERT OR REPLACE INTO item_data (item, value) VALUES (?, ?)",
            (id, pickle.dumps(all_data)),
        )
        self.conn.commit()

        # Store an annotation with the identifiers
        self._annotate(("item", name), "__ixmp_ts_info", repr(identifiers))

    # Methods for ixmp.Scenario

    def list_items(self, s, type):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT i.name FROM timeseries AS ts
            JOIN item as i ON ts.id == i.timeseries
            WHERE ts.id = ? AND i.type = ?
            """,
            (self._index[s], type),
        )
        return list(map(itemgetter(0), cur.fetchall()))

    def init_item(self, s, type, name, idx_sets, idx_names):
        idx_names = idx_names or idx_sets
        dimensions = {n: s for n, s in zip(idx_names, idx_sets)}

        cur = self.conn.cursor()

        cur.execute(
            """
            INSERT OR ABORT INTO item (timeseries, type, name, dims)
            VALUES (?, ?, ?, ?)
            """,
            (self._index[s], type, name, repr(dimensions)),
        )

    def item_index(self, s, name, sets_or_names):
        cur = self.conn.cursor()

        cur.execute(
            "SELECT dims FROM item AS i WHERE i.timeseries = ? AND i.name = ?",
            (self._index[s], name),
        )
        dims = eval(cur.fetchone()[0])
        return list(dims.keys() if sets_or_names == "names" else dims.values())

    def _item_data(self, s, name):
        cur = self.conn.cursor()

        cur.execute(
            """
            SELECT i.id, value FROM item AS i LEFT JOIN item_data
            ON i.id == item_data.item WHERE i.timeseries = ? AND i.name = ?
            """,
            (self._index[s], name),
        )
        result = cur.fetchone() or (None, None)

        return result

    def item_get_elements(self, s, type, name, filters):
        id, data = self._item_data(s, name)

        cur = self.conn.cursor()
        if data is None:
            cur.execute("SELECT dims from item WHERE id = ?", (id,))
            dims = eval(cur.fetchone()[0])
            idx_names, idx_sets = list(zip(*dims.items()))
            data = tuple()

        if len(idx_sets):
            # Mapping set or multi-dimensional equation, parameter, or variable
            columns = list(idx_names)

            # Prepare dtypes for index columns
            dtypes = {}
            for idx_name, idx_set in zip(columns, idx_sets):
                dtypes[idx_name] = str

            # Prepare dtypes for additional columns
            if type == "par":
                columns.extend(["value", "unit"])
                dtypes["value"] = float
                dtypes["unit"] = str
            elif type in ("equ", "var"):
                columns.extend(["lvl", "mrg"])
                dtypes.update({"lvl": float, "mrg": float})

            # Create data frame
            result = pd.DataFrame(data, columns=columns).astype(dtypes)
        else:
            raise NotImplementedError("non-indexed items")

        return result

    def item_set_elements(self, s, type, name, elements):
        cur = self.conn.cursor()

        id, data = self._item_data(s, name)

        if data is not None:
            raise NotImplementedError("update existing items")

        data = []

        for e in elements:
            data.append(e[0] if type == "set" else e)

        cur.execute(
            "INSERT OR REPLACE INTO item_data (item, value) VALUES (?, ?)",
            (id, pickle.dumps(data)),
        )
        self.conn.commit()

    # Internal

    def _select_anno(self, obj, anno_id):
        if isinstance(obj, TimeSeries):
            data = ["timeseries", str(self._index[obj])]
        elif isinstance(obj, tuple):
            data = list(obj)
        else:
            raise NotImplementedError

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT * FROM annotation WHERE annotation.obj_class = ?
            AND annotation.obj_id = ? AND annotation.id = ?
            """,
            data + [anno_id],
        )
        return cur.fetchone()

    def _select_codes(self, codelist):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM code WHERE codelist == ?", (codelist,))
        while True:
            results = cur.fetchmany()
            if not len(results):
                break
            yield from results

    def _insert_code(self, codelist, id, parent=None):
        self.conn.execute(
            "INSERT OR ABORT INTO code VALUES (?, ?, ?)", (codelist, id, parent)
        )
        self.conn.commit()

    def _annotate(self, obj, anno_id, value):
        if isinstance(obj, TimeSeries):
            data = ["timeseries", str(self._index[obj])]
        elif isinstance(obj, tuple):
            data = list(obj)
        else:
            raise NotImplementedError

        self.conn.execute(
            "INSERT OR ABORT INTO annotation VALUES (?, ?, ?, ?)",
            data + [anno_id, value],
        )
        self.conn.commit()

    def _annotate_code(self, codelist, code_id, anno_id, value):
        self.conn.execute(
            "INSERT OR ABORT INTO annotation VALUES (?, ?, ?, ?)",
            ("code", f"{codelist}:{code_id}", anno_id, value),
        )
        self.conn.commit()

    # Required methods that are not implemented
    #
    # Since base.Backend is an abstract base class with abstract methods, a subclass
    # (like this one) cannot be instantiated unless a concrete implementation is given
    # for each abstract method. Here we raise NotImplementedError for each.

    def nie(self, *args, **kwargs):
        raise NotImplementedError

    cat_get_elements = nie
    cat_list = nie
    cat_set_elements = nie
    clear_solution = nie
    clone = nie
    delete = nie
    delete_geo = nie
    delete_item = nie
    delete_meta = nie
    discard_changes = nie
    get_data = nie
    get_doc = nie
    get_geo = nie
    get_meta = nie
    get_scenarios = nie
    item_delete_elements = nie
    last_update = nie
    remove_meta = nie
    set_doc = nie
    set_geo = nie
    set_meta = nie

    # Class-specific methods

    @classmethod
    def handle_config(cls, args: Tuple[str, ...]) -> Dict[str, Any]:
        """Handle ``ixmp platform add`` CLI arguments."""
        info = {"class": "dbapi", "path": args.pop(0)}
        assert 0 == len(args)
        return info


SCHEMA = """
    CREATE TABLE schema (key, value);
    INSERT INTO schema VALUES ('version', '1.0');

    CREATE TABLE code (codelist, id, parent);

    CREATE TABLE annotation (
        obj_class VARCHAR NOT NULL, obj_id NOT NULL, id VARCHAR NOT NULL, value
    );

    CREATE TABLE timeseries (
        id INTEGER PRIMARY KEY,
        class,
        model_name,
        scenario_name,
        version INTEGER,
        UNIQUE (model_name, scenario_name, version)
    );

    CREATE TABLE item (
        id INTEGER PRIMARY KEY,
        timeseries INTEGER,
        name VARCHAR,
        type VARCHAR,
        dims TEXT,
        FOREIGN KEY (timeseries) REFERENCES timeseries(id),
        UNIQUE (timeseries, name)
    );

    CREATE TABLE item_data (
        item INTEGER, value BLOB, FOREIGN KEY (item) REFERENCES item(id)
    );
"""


def init_schema(conn):
    """Check or initialize the database schema in `conn`."""
    cur = conn.cursor()

    try:
        # Check that the schema table exists and there is one expected value in it
        cur.execute("SELECT value FROM schema WHERE key == 'version'")
    except sqlite3.OperationalError as e:
        if "no such table: schema" in e.args:
            pass  # Not initialized yet
        else:
            raise  # Something else
    else:
        if "1.0" == cur.fetchone()[0]:
            return  # Already initialized

    # Initialize the database
    cur.executescript(SCHEMA)
    conn.commit()
