pytest_plugins = ["ixmp.testing"]


# Hooks


def pytest_addoption(parser):
    parser.addoption(
        "--jvm-mem-limit",
        action="store",
        default=-1,
        help=(
            "Memory limit, in MiB, for the Java Virtual Machine (JVM) "
            "started by the ixmp JDBCBackend"
        ),
    )
    parser.addoption(
        "--resource-limit",
        action="store",
        default="DATA:-1",
        help=(
            "Limit a Python resource via the ixmp.testing.resource_limit "
            "fixture. Use e.g. 'DATA:500' to limit RLIMIT_DATA to 500 MiB"
        ),
    )
