class NotAvailableInSessions(Exception):
    def __init__(self, subject: str) -> None:
        super().__init__(self, f"{subject} is not available in standalone sessions")
