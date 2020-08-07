import json
import logging

import numpy as np
import xarray as xr

from .base import Backend


log = logging.getLogger(__name__)


class XarrayBackend(Backend):
    # TODO store each TimeSeries' data as a distinct xr.Dataset
    def __init__(self):
        self._init_data()

    def _init_data(self):
        self._data = xr.Dataset()
        self._ts = dict()

        # Counter
        self._data.attrs["next run_id"] = 1
        self._data.attrs["items"] = []

        # Dimension
        self._data["_codelist"] = (
            xr.DataArray(["name", "anno"], dims=["_codelist"]).astype(str)
        )

        # Default code lists
        for name in ["item", "model", "node", "scenario", "timeslice", "unit"]:
            self._data[name] = xr.DataArray([], dims=[name]).astype(str)
            self._data[f"{name}_info"] = xr.DataArray(
                coords=[[], ["name", "anno"]],
                dims=[name, "_codelist"],
            ).astype(object)

    def _ds_for_ts(self, ts):
        run_id = self.run_id(ts)
        return self._ts[run_id]

    def _add_code(self, cl, id, name, **anno):
        new = xr.Dataset(
            {f"{cl}_info": ([cl, "_codelist"], [[name, json.dumps(anno)]])},
            coords={cl: [id], "_codelist": ["name", "anno"]}
        )
        self._data = xr.combine_by_coords(
            [self._data, new], data_vars=[f"{cl}_info"], coords=[cl],
        )

    def set_node(self, name, parent=None, hierarchy=None, synonym=None):
        self._add_code(
            "node",
            name,
            name,
            parent=parent,
            hierarchy=hierarchy,
            synonym=synonym,
        )

    def set_timeslice(self, name, category, duration):
        self._add_code(
            "timeslice", name, name, category=category, duration=duration
        )

    def set_unit(self, name, comment):
        self._add_code("unit", name, comment)

    def get_nodes(self):
        for n in self._data["node_info"]:
            print(n)
            # n, p, h = r.getName(), r.getParent(), r.getHierarchy()
            # yield (n, None, p, h)
            # yield from [(s, n, p, h) for s in (r.getSynonyms() or [])]
            yield n

    def get_units(self):
        return list(self._data["unit"].values)

    def init(self, ts, annotation=None):
        annotation = annotation or ""
        self._add_code("model", ts.model, annotation)
        self._add_code("scenario", ts.scenario, annotation)

        # Store the run_id
        run_id = self._data.attrs["next run_id"]
        self._data.attrs["next run_id"] += 1
        new = xr.Dataset(
            {"run_id": (["model", "scenario"], [[run_id]])},
            coords=dict(model=[ts.model], scenario=[ts.scenario]),
        )
        self._data = xr.combine_by_coords([self._data, new])

        # Create a new Dataset for the TimeSeries/Scenario's data
        self._ts[run_id] = xr.Dataset(attrs=dict(
                model=ts.model,
                scenario=ts.scenario,
                run_id=run_id,
        ))

    def list_items(self, s, type):
        return [
            name for (name, da) in self._ds_for_ts(s).items()
            if da.attrs.get("_type", None) == type
        ]

    def init_item(self, s, type, name, idx_sets, idx_names):
        ds = self._ds_for_ts(s)
        assert isinstance(name, str)
        if name in ds:
            raise ValueError(f"item {name} already exists")

        attrs = dict(_type=type)

        if type == "set":
            if len(idx_sets):
                raise NotImplementedError("non-index sets")

            ds[name] = xr.DataArray([], dims=[name]).astype(str)
        elif type in ("par", "var", "equ"):
            attrs["_names"] = idx_names
            ds[name] = xr.DataArray(
                coords=[list() for _ in idx_sets], dims=idx_sets,
            )
        else:
            raise NotImplementedError(f"init_item(type={type})")

        ds[name].attrs.update(attrs)

    def item_index(self, s, name, sets_or_names):
        da = self._ds_for_ts(s)[name]
        coords = list(da.coords.keys())
        coords = [] if coords == [name] else coords
        if sets_or_names == "sets":
            return coords
        else:
            return da.attrs.get("idx_names", coords)

    def item_get_elements(self, s, type, name, filters=None):
        if type != "par":
            raise NotImplementedError(f"item_get_elements(type={type})")

        if filters is not None:
            raise NotImplementedError("item_get_elements() with filters")

        ds = self._ds_for_ts(s)
        return ds[name].to_series().rename("value").reset_index()

    def item_set_elements(self, s, type, name, elements):
        ds = self._ds_for_ts(s)

        # FIXME attribute set above in init_item is missing
        # assert ds[name].attrs["_type"] == type

        if type == "set":
            to_add = []
            for key, value, unit, comment in elements:
                assert value is unit is comment is None
                to_add.append(key)
            new = (
                xr.DataArray(to_add, dims=[name], name=name)
                .astype(str)
                .to_dataset()
                .set_coords(name)
            )
            ds = xr.combine_by_coords([ds, new], coords=[name])
        else:
            da = ds[name].copy()
            stored_unit = da.attrs.get("_unit", None)
            for key, value, unit, comment in elements:
                if value is None:
                    raise ValueError(f"set value None for key={key}")
                if comment is not None:
                    log.info(f"Discard comment: {comment}")
                if unit not in (None, "nan"):
                    if stored_unit and unit != stored_unit:
                        raise ValueError(f"units {unit} != {stored_unit}")
                    else:
                        stored_unit = unit

                da.loc[key] = value
                da.attrs["_unit"] = stored_unit
                ds[name] = da

        self._ts[ds.attrs["run_id"]] = ds

    def get(self, ts):
        # Raises KeyError or TypeError if nonexistent
        self.run_id(ts)

        # TODO load Dataset from file

        ts.version = -1

    def run_id(self, ts):
        """Get the run id of this TimeSeries."""
        return int(self._data["run_id"].loc[ts.model, ts.scenario])

    def _noop(self, *args, **kwargs):
        raise NotImplementedError

    cat_get_elements = _noop
    cat_list = _noop
    cat_set_elements = _noop
    check_out = _noop
    clear_solution = _noop
    clone = _noop
    commit = _noop
    delete = _noop
    delete_geo = _noop
    delete_item = _noop
    delete_meta = _noop
    discard_changes = _noop
    get_data = _noop
    get_doc = _noop
    get_geo = _noop
    get_meta = _noop
    get_scenarios = _noop
    get_timeslices = _noop
    has_solution = _noop
    is_default = _noop
    item_delete_elements = _noop
    last_update = _noop
    set_as_default = _noop
    set_data = _noop
    set_geo = _noop
    set_meta = _noop
    set_doc = _noop
