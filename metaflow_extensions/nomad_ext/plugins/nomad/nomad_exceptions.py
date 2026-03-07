from metaflow.exception import MetaflowException


class NomadException(MetaflowException):
    headline = "Nomad error"


class NomadKilledException(MetaflowException):
    headline = "Nomad job killed"
