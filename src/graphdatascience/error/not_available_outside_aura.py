class NotAvailableOutsideAura(Exception):
    def __init__(self, subject: str) -> None:
        super().__init__(
            f"{subject} is not available outside Aura. The underlying Cypher functions are only provided by AuraDBs."
        )
