class ChainDataError(Exception):
    pass


class ChainIdMismatchError(ChainDataError):
    pass


class ChainDataIsNoneError(ChainDataError):
    pass


class APIResponseError(ChainDataError):
    pass


class ProcessChainDataError(Exception):
    pass
