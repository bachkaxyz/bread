class ChainDataError(BaseException):
    pass


class ChainIdMismatchError(ChainDataError):
    pass


class ChainDataIsNoneError(ChainDataError):
    pass


class APIResponseError(ChainDataError):
    pass


class BlockNotParsedError(ChainDataError):
    pass


class ProcessChainDataError(BaseException):
    pass
