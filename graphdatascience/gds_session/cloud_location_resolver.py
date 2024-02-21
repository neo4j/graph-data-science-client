from typing import Iterable, NamedTuple, Optional

import textdistance

from graphdatascience.gds_session.aura_api import InstanceSpecificDetails, TenantDetails


class CloudLocation(NamedTuple):
    cloud_provider: str
    region: str


class CloudLocationResolver:
    def __init__(self, tenant_details: TenantDetails):
        self._tenant_id = tenant_details.id
        self._location_options = tenant_details.regions_per_provider
        self._default: Optional[CloudLocation] = None

    def set_default(self, cloud_provider: str, region: str) -> None:
        if cloud_provider not in self._location_options.keys():
            raise ValueError(
                f"Cloud provider `{cloud_provider}` not available for tenant."
                f" Available providers: {list(self._location_options.keys())}"
            )
        region_options: Iterable[str] = self._location_options.get(cloud_provider, [])
        if region not in region_options:
            raise ValueError(
                f"Region `{region}` not available for cloud provider `{cloud_provider}`."
                f" Available regions: {list(region_options)}"
            )

        self._default = CloudLocation(region=region, cloud_provider=cloud_provider)

    def for_instance(self, instance: InstanceSpecificDetails) -> CloudLocation:
        cloud_provider, region = instance.cloud_provider, instance.region
        available_regions = self._location_options[cloud_provider]

        ds_region = CloudLocationResolver._clostest_region(region, available_regions)
        if not ds_region:
            raise ValueError(
                f"Tenant `{self._tenant_id}` cannot create GDS sessions at cloud provider `{cloud_provider}`."
            )

        return CloudLocation(instance.cloud_provider, ds_region)

    def default_location(self) -> Optional[CloudLocation]:
        return self._default

    # AuraDB and AuraDS regions are not the same, so we need to find the closest match.
    @staticmethod
    def _clostest_region(db_region: str, ds_regions: Iterable[str]) -> Optional[str]:
        curr_max_similarity = 0.0
        closest_option = None

        for region in ds_regions:
            similarity = textdistance.jaro_winkler(db_region, region)
            if similarity > curr_max_similarity:
                closest_option = region
                curr_max_similarity = similarity

        return closest_option
