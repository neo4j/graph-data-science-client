import base64
import json
import time
import warnings
from typing import Any, Dict, Optional, Tuple

from pandas import DataFrame
from pyarrow import ChunkedArray, Schema, Table, chunked_array, flight
from pyarrow._flight import FlightStreamReader, FlightStreamWriter
from pyarrow.flight import ClientMiddleware, ClientMiddlewareFactory
from pyarrow.types import is_dictionary

from ..server_version.server_version import ServerVersion
from .arrow_endpoint_version import ArrowEndpointVersion
from .query_runner import QueryRunner


class GdsArrowClient:
    @staticmethod
    def is_arrow_enabled(query_runner: QueryRunner) -> bool:
        arrow_info = query_runner.call_procedure(endpoint="gds.debug.arrow", custom_error=False).squeeze().to_dict()
        return not not arrow_info["running"]

    @staticmethod
    def create(
        query_runner: QueryRunner,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        connection_string_override: Optional[str] = None,
    ) -> "GdsArrowClient":
        arrow_info = query_runner.call_procedure(endpoint="gds.debug.arrow", custom_error=False).squeeze().to_dict()

        server_version = query_runner.server_version()
        connection_string: str
        if connection_string_override is not None:
            connection_string = connection_string_override
        else:
            connection_string = arrow_info.get("advertisedListenAddress", arrow_info["listenAddress"])

        host, port = connection_string.split(":")

        arrow_endpoint_version = ArrowEndpointVersion.from_arrow_info(arrow_info.get("versions", []))

        return GdsArrowClient(
            host,
            int(port),
            server_version,
            auth,
            encrypted,
            disable_server_verification,
            tls_root_certs,
            arrow_endpoint_version,
        )

    def __init__(
        self,
        host: str,
        port: int,
        server_version: ServerVersion,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        arrow_endpoint_version: ArrowEndpointVersion = ArrowEndpointVersion.ALPHA,
    ):
        self._server_version = server_version
        self._arrow_endpoint_version = arrow_endpoint_version
        self._host = host
        self._port = port
        self._auth = auth

        location = flight.Location.for_grpc_tls(host, port) if encrypted else flight.Location.for_grpc_tcp(host, port)

        client_options: Dict[str, Any] = {"disable_server_verification": disable_server_verification}
        if auth:
            self._auth_middleware = AuthMiddleware(auth)
            client_options["middleware"] = [AuthFactory(self._auth_middleware)]
        if tls_root_certs:
            client_options["tls_root_certs"] = tls_root_certs

        self._flight_client = flight.FlightClient(location, **client_options)

    def connection_info(self) -> Tuple[str, int]:
        return self._host, self._port

    def request_token(self) -> Optional[str]:
        if self._auth:
            self._flight_client.authenticate_basic_token(self._auth[0], self._auth[1])
            return self._auth_middleware.token()
        else:
            return "IGNORED"

    def get_property(
        self, database: Optional[str], graph_name: str, procedure_name: str, configuration: Dict[str, Any]
    ) -> DataFrame:
        if not database:
            raise ValueError(
                "For this call you must have explicitly specified a valid Neo4j database to execute on, "
                "using `GraphDataScience.set_database`."
            )

        payload = {
            "database_name": database,
            "graph_name": graph_name,
            "procedure_name": procedure_name,
            "configuration": configuration,
        }

        if self._arrow_endpoint_version == ArrowEndpointVersion.V1:
            payload = {
                "name": "GET_COMMAND",
                "version": ArrowEndpointVersion.V1.version(),
                "body": payload,
            }

        ticket = flight.Ticket(json.dumps(payload).encode("utf-8"))
        get = self._flight_client.do_get(ticket)
        arrow_table = get.read_all()

        if configuration.get("list_node_labels", False):
            # GDS 2.5 had an inconsistent naming of the node labels column
            new_colum_names = ["nodeLabels" if i == "labels" else i for i in arrow_table.column_names]
            arrow_table = arrow_table.rename_columns(new_colum_names)

        # Pandas 2.2.0 deprecated an API used by ArrowTable.to_pandas() (< pyarrow 15.0)
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message=r"Passing a BlockManager to DataFrame is deprecated",
        )

        return self._sanitize_arrow_table(arrow_table).to_pandas()  # type: ignore

    def send_action(self, action_type: str, meta_data: Dict[str, Any]) -> None:
        action_type = self._versioned_action_type(action_type)
        result = self._flight_client.do_action(flight.Action(action_type, json.dumps(meta_data).encode("utf-8")))

        # Consume result fully to sanity check and avoid cancelled streams
        collected_result = list(result)
        assert len(collected_result) == 1

        json.loads(collected_result[0].body.to_pybytes().decode())

    def start_put(self, payload: Dict[str, Any], schema: Schema) -> Tuple[FlightStreamWriter, FlightStreamReader]:
        flight_descriptor = self._versioned_flight_descriptor(payload)
        upload_descriptor = flight.FlightDescriptor.for_command(json.dumps(flight_descriptor).encode("utf-8"))
        return self._flight_client.do_put(upload_descriptor, schema)  # type: ignore

    def close(self) -> None:
        self._flight_client.close()

    def _versioned_action_type(self, action_type: str) -> str:
        return self._arrow_endpoint_version.prefix() + action_type

    def _versioned_flight_descriptor(self, flight_descriptor: Dict[str, Any]) -> Dict[str, Any]:
        return (
            flight_descriptor
            if self._arrow_endpoint_version == ArrowEndpointVersion.ALPHA
            else {
                "name": "PUT_MESSAGE",
                "version": ArrowEndpointVersion.V1.version(),
                "body": flight_descriptor,
            }
        )

    @staticmethod
    def _sanitize_arrow_table(arrow_table: Table) -> Table:
        # empty columns cannot be used to build a chunked_array in pyarrow
        if len(arrow_table) == 0:
            return arrow_table

        dict_encoded_fields = [
            (idx, field) for idx, field in enumerate(arrow_table.schema) if is_dictionary(field.type)
        ]

        for idx, field in dict_encoded_fields:
            try:
                field.type.to_pandas_dtype()
            except NotImplementedError:
                # we need to decode the dictionary column before transforming to pandas
                if isinstance(arrow_table[field.name], ChunkedArray):
                    decoded_col = chunked_array([chunk.dictionary_decode() for chunk in arrow_table[field.name].chunks])
                else:
                    decoded_col = arrow_table[field.name].dictionary_decode()
                arrow_table = arrow_table.set_column(idx, field.name, decoded_col)
        return arrow_table


class AuthFactory(ClientMiddlewareFactory):  # type: ignore
    def __init__(self, middleware: "AuthMiddleware", *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._middleware = middleware

    def start_call(self, info: Any) -> "AuthMiddleware":
        return self._middleware


class AuthMiddleware(ClientMiddleware):  # type: ignore
    def __init__(self, auth: Tuple[str, str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._auth = auth
        self._token: Optional[str] = None
        self._token_timestamp = 0

    def token(self) -> Optional[str]:
        # check whether the token is older than 10 minutes. If so, reset it.
        if self._token and int(time.time()) - self._token_timestamp > 600:
            self._token = None

        return self._token

    def _set_token(self, token: str) -> None:
        self._token = token
        self._token_timestamp = int(time.time())

    def received_headers(self, headers: Dict[str, Any]) -> None:
        auth_header = headers.get("authorization", None)
        if not auth_header:
            return

        # the result is always a list
        header_value = auth_header[0]

        if not isinstance(header_value, str):
            raise ValueError(f"Incompatible header value received from server: `{header_value}`")

        auth_type, token = header_value.split(" ", 1)
        if auth_type == "Bearer":
            self._set_token(token)

    def sending_headers(self) -> Dict[str, str]:
        token = self.token()
        if not token:
            username, password = self._auth
            auth_token = f"{username}:{password}"
            auth_token = "Basic " + base64.b64encode(auth_token.encode("utf-8")).decode("ASCII")
            # There seems to be a bug, `authorization` must be lower key
            return {"authorization": auth_token}
        else:
            return {"authorization": "Bearer " + token}
