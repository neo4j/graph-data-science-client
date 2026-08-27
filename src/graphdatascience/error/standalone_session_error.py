class NotAvailableInStandaloneSessions(Exception):
    def __init__(self, subject: str) -> None:
        super().__init__(f"{subject} is not available in standalone sessions")
