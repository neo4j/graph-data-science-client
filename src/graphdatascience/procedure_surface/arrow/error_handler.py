import re

from neo4j.exceptions import ClientError
from pyarrow import flight


def handle_flight_error(e: Exception) -> None:
    if isinstance(e, flight.FlightServerError | flight.FlightInternalError | ClientError):
        original_message = e.args[0] if len(e.args) > 0 else e.message
        improved_message = original_message.replace(
            "Flight RPC failed with message: org.apache.arrow.flight.FlightRuntimeException: ", ""
        )
        improved_message = improved_message.replace(
            "Flight returned internal error, with message: org.apache.arrow.flight.FlightRuntimeException: ", ""
        )
        improved_message = improved_message.replace(
            "Failed to invoke procedure `gds.arrow.project`: Caused by: org.apache.arrow.flight.FlightRuntimeException: ",
            "",
        )
        improved_message = re.sub(r"(\. )?gRPC client debug context: .+$", "", improved_message)

        raise flight.FlightServerError(improved_message)
    else:
        raise e
