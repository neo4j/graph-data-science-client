def gpu_config_payload(gpu: bool) -> dict[str, dict[str, bool]]:
    """Arrow job config carrying the gpuConfig payload; empty when no GPU was requested."""
    return {"gpuConfig": {"enabled": gpu}} if gpu else {}
