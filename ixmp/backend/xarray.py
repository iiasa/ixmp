import json

import xarray as xr

from .base import Backend


class XarrayBackend(Backend):
    # TODO store each TimeSeries' data as a distinct xr.Dataset
    def __init__(self):
        self._init_data()

    def _init_data(self):
        self._data = xr.Dataset()

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

        run_id = self._data.attrs["next run_id"]
        self._data.attrs["next run_id"] += 1
        new = xr.Dataset(
            {"run_id": (["model", "scenario"], [[run_id]])},
            coords=dict(model=[ts.model], scenario=[ts.scenario]),
        )
        self._data = xr.combine_by_coords([self._data, new])

    def list_items(self, s, type):
        return self._data.attrs["items"]

    def init_item(self, s, type, name, idx_sets, idx_names):
        assert isinstance(name, str)
        if name in self._data.attrs["items"]:
            raise ValueError(f"item {name} already exists")
        else:
            self._data.attrs["items"].append(name)

        attrs = dict(_type=type)

        if type == "set":
            if len(idx_sets):
                raise NotImplementedError("non-index sets")

            self._data[name] = xr.DataArray([], dims=[name]).astype(str)
        elif type in ("par", "var", "equ"):
            attrs["_names"] = idx_names
            self._data[name] = xr.DataArray(
                coords=[list() for _ in idx_sets], dims=idx_sets,
            )
        else:
            raise NotImplementedError(f"init_item(type={type})")

        self._data[name].attrs.update(attrs)

    def item_index(self, s, name, sets_or_names):
        da = self._data[name]
        coords = list(da.coords.keys())
        coords = [] if coords == [name] else coords
        if sets_or_names == "sets":
            return coords
        else:
            return da.attrs.get("idx_names", coords)

    def item_set_elements(self, s, type, name, elements):
        print(s, type, name, elements)

        # TODO attribute set above in init_item is missing
        print(self._data, self._data[name])
        assert self._data[name].attrs["_type"] == type

        raise NotImplementedError

    def get(self, ts):
        # Raises KeyError or TypeError if nonexistent
        self.run_id(ts)

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
    item_get_elements = _noop
    last_update = _noop
    set_as_default = _noop
    set_data = _noop
    set_geo = _noop
    set_meta = _noop
    set_doc = _noop
