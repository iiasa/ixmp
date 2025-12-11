import random
import time

import pandas as pd

import ixmp

mp = ixmp.Platform("test_MESSAGEix-GLOBIOM_1.1_R11_no-policy_baseline_snapshot-1")

scen = ixmp.Scenario(mp, "model", "scenario", version="new")

units = pd.read_csv("/home/fridolin/ixmp4/tests/fixtures/big/units.csv")
for _, name in units.itertuples():
    mp.add_unit(name)

insert_pardata = pd.read_csv(
    "/home/fridolin/ixmp4/tests/fixtures/big/insert_parameterdata_vk.csv"
)
upsert_pardata = pd.read_csv(
    "/home/fridolin/ixmp4/tests/fixtures/big/upsert_parameterdata_vk.csv"
)

# insert_pardata["values"] = [x for x in range(len(insert_pardata))]
# insert_pardata["units"] = random.choices(
#     [unit for unit in mp.units()],
#     k=len(insert_pardata),
# )
# # TODO Why do we still have range from 1 here?
# upsert_pardata["values"] = [x for x in range(1, len(upsert_pardata) + 1)]
# upsert_pardata["units"] = random.choices(
#     [unit for unit in mp.units()],
#     k=len(upsert_pardata),
# )

for i in range(12):
    scen.init_set(f"Indexset {i}")
    scen.add_set(f"Indexset {i}", [str(x) for x in range(50)])

parameter = scen.init_par("Test Par", idx_sets=[f"Indexset {i}" for i in range(12)])


def benchmark_upsert_pardata() -> None:
    start = time.process_time()

    scen.add_par(
        "Test Par",
        insert_pardata,
        value=[x for x in range(len(insert_pardata))],
        unit=random.choices(
            [unit for unit in mp.units()],
            k=len(insert_pardata),
        ),
    )
    scen.add_par(
        "Test Par",
        upsert_pardata,
        value=[x for x in range(1, len(upsert_pardata) + 1)],
        unit=random.choices(
            [unit for unit in mp.units()],
            k=len(upsert_pardata),
        ),
    )

    end = time.process_time()
    print(end - start)


benchmark_upsert_pardata()

mp.close_db()
