class Query:
    mask = "******"

    def __init__(self, value: str, *sensitive_data: str):
        self.value = value
        self.sensitive_data = list(sensitive_data)

    def for_execute(self) -> str:
        return self.value

    def for_logging(self) -> str:
        result = self.value
        for data in self.sensitive_data:
            result = result.replace(data, self.mask)
        return result
