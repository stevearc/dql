""" Custom exceptions """


class ExplainSignal(Exception):
    """ Thrown to stop a query when we're doing an EXPLAIN """


class EngineRuntimeError(RuntimeError):
    """ Issue with the DQL engine at runtime """
