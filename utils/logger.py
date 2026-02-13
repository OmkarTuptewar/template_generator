import datetime
import sys


class Logger:
    def _log(self, level: str, message: str) -> None:
        ts = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        sys.stdout.write(f"[{ts}] {level}: {message}\n")
        sys.stdout.flush()

    def info(self, message: str) -> None:
        self._log("INFO", message)

    def error(self, message: str) -> None:
        self._log("ERROR", message)
