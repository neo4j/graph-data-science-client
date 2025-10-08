# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Optional

import pandas as pd

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.local_clustering_coefficient_endpoints import (
    LocalClusteringCoefficientEndpoints,
    LocalClusteringCoefficientMutateResult,
    LocalClusteringCoefficientStatsResult,
    LocalClusteringCoefficientWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class LocalClusteringCoefficientArrowEndpoints(LocalClusteringCoefficientEndpoints):
    def __init__(
        self,
        client: AuthenticatedArrowClient,
        remote_write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpoints(
            client,
            remote_write_back_client,
            show_progress,
        )

    def mutate(
        self,
        G: GraphV2,
        *,
        mutate_property: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> LocalClusteringCoefficientMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/community.localClusteringCoefficient", G, config, mutate_property
        )

        return LocalClusteringCoefficientMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> LocalClusteringCoefficientStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/community.localClusteringCoefficient",
            G,
            config,
        )

        return LocalClusteringCoefficientStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> pd.DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream(
            "v2/community.localClusteringCoefficient",
            G,
            config,
        )

    def write(
        self,
        G: GraphV2,
        *,
        write_property: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = None,
    ) -> LocalClusteringCoefficientWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            write_property=write_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
            write_concurrency=write_concurrency,
            write_to_result_store=write_to_result_store,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.localClusteringCoefficient",
            G,
            config,
            write_concurrency,
            concurrency,
            write_property,
        )

        return LocalClusteringCoefficientWriteResult(**result)

    def estimate(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        result = self._node_property_endpoints.estimate(
            "v2/community.localClusteringCoefficient.estimate",
            G,
            config,
        )

        return result
