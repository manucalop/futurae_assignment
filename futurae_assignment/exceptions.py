class PipelineError(Exception):
    pass


class DatabaseError(PipelineError):
    pass


class ParsingError(PipelineError):
    pass


