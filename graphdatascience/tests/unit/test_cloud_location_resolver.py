import re

import pytest

from graphdatascience.session.aura_api import InstanceSpecificDetails, TenantDetails
from graphdatascience.session.cloud_location_resolver import CloudLocationResolver


def test_validates_set_default() -> None:
    details = TenantDetails("42", "ee", {"aws": {"dresden-1", "leipzig-1"}})

    resolver = CloudLocationResolver(details)

    with pytest.raises(
        ValueError, match=re.escape("Cloud provider `azure` not available for tenant. Available providers: ['aws']")
    ):
        resolver.set_default("azure", "us-east-1")

    with pytest.raises(ValueError, match="Region `berlin-2` not available for cloud provider `aws`"):
        resolver.set_default("aws", "berlin-2")


def test_for_instance() -> None:
    details = TenantDetails("42", "ee", {"aws": {"dresden-1", "leipzig-1"}})

    resolver = CloudLocationResolver(details)

    db_instance = InstanceSpecificDetails("1337", "the one", "42", "aws", "on", "here.now", "3G", "db", "leipzig-2")

    assert resolver.for_instance(db_instance) == ("aws", "leipzig-1")


def test_picks_the_only_option() -> None:
    match = CloudLocationResolver._closest_region("db_region", ["ds_region1"])
    assert match == "ds_region1"


def test_picks_the_closest() -> None:
    match = CloudLocationResolver._closest_region("eu-1", ["us-1", "aa-1", "ea-1", "eu-4"])
    assert match == "eu-4"


def test_picks_exact_match() -> None:
    match = CloudLocationResolver._closest_region("eu-2", ["eu-1", "eu-2", "eu-3"])
    assert match == "eu-2"


def test_no_options() -> None:
    match = CloudLocationResolver._closest_region("eu-2", [])
    assert match is None
