class ChainDataError(BaseException):
    pass


class ChainIdMismatchError(ChainDataError):
    pass


class ChainDataIsNoneError(ChainDataError):
    pass


class APIResponseError(ChainDataError):
    pass


class BlockPrimaryKeyNotDefinedError(ChainDataError):
    pass


class ProcessChainDataError(BaseException):
    pass


class EnvironmentError(BaseException):
    pass
