import logging
from operator import itemgetter
from typing import Any, Dict, Generator, Tuple

import sqlite3

from ixmp.backend.base import CachingBackend

log = logging.getLogger(__name__)


class DatabaseBackend(CachingBackend):
    """Backend based on Python DB API 2.0."""

    #: Database connection object.
    conn = None

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

    # Internal
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
    check_out = nie
    clear_solution = nie
    clone = nie
    commit = nie
    delete = nie
    delete_geo = nie
    delete_item = nie
    delete_meta = nie
    discard_changes = nie
    get = nie
    get_data = nie
    get_doc = nie
    get_geo = nie
    get_meta = nie
    get_scenarios = nie
    has_solution = nie
    init = nie
    init_item = nie
    is_default = nie
    item_delete_elements = nie
    item_get_elements = nie
    item_index = nie
    item_set_elements = nie
    last_update = nie
    list_items = nie
    remove_meta = nie
    run_id = nie
    set_as_default = nie
    set_data = nie
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
    cur.execute("CREATE TABLE schema (key, value)")
    cur.execute("INSERT INTO schema VALUES ('version', '1.0')")
    cur.execute("CREATE TABLE code (codelist, id, parent)")
    cur.execute("CREATE TABLE annotation (obj_class, obj_id, id, value)")
    conn.commit()
