from ..graph import graph_object


def assert_graph(args_pos=None, key=None):
    """Generates a decorator that validates that a graph given as an
    input parameter is valid.

    Exactly one of the optional parameters must be given.

    Parameters
    ----------
    args_pos : int, optional
        If graph is a positional parameter, provide the index of it.
    args_pos : str, optional
        If graph is an optional parameter, provide its key.

    Raises
    ------
    ValueError
        If the parameter that should be a Graph in fact is not, or if
        the graph has been dropped.
    """
    assert args_pos or key
    assert not (args_pos and key)

    def decorator(function):
        def wrapper(*args, **kwargs):

            if args_pos:
                G = args[args_pos]
            else:
                # Graph is optional as a keyword argument.
                if key not in kwargs.keys():
                    return function(*args, **kwargs)
                G = kwargs[key]

            if not isinstance(G, graph_object.Graph):
                raise ValueError("A valid Graph object must be provided")
            if G.dropped():
                raise ValueError("This Graph object has been dropped")

            return function(*args, **kwargs)

        return wrapper

    return decorator
